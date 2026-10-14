"""병합된 워커를 실제 NATS와 Postgres 위에서 구동해 consumer identity 보존을 검증한다.

격리된 loopback 스택이 있을 때만 실행한다.

    CLUE_VERIFY_DATABASE_URL=postgresql://postgres@127.0.0.1:<port>/clue_wm_verify
    CLUE_VERIFY_NATS_URL=nats://127.0.0.1:<port>

이 검사는 대상 DB의 events·event_processing·outbox·event_dead_letters를 비우고 NATS
스트림을 삭제하므로, 로컬 loopback 주소와 `clue_wm_verify` 접두 DB 이름이 아니면 거부한다.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import signal
import time
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest
from conftest import load_service
from nats.js.errors import NotFoundError
from sqlalchemy import text

from domains.rca.events import RcaCandidatesPlannedBody
from packages.contracts.event_bus.interfaces import EventEnvelope
from packages.events.bus import STREAM_NAME, NatsEventBus
from packages.events.envelope import event
from packages.runtime.worker import EventRetryPolicy, WorkerRuntime
from packages.storage.database import Database, wait_for_database

DATABASE_URL = os.environ.get("CLUE_VERIFY_DATABASE_URL", "")
NATS_URL = os.environ.get("CLUE_VERIFY_NATS_URL", "")
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
VERIFY_DATABASE_PREFIX = "clue_wm_verify"
POLL_TIMEOUT_SECONDS = 30.0
SETTLE_SECONDS = 3.0

pytestmark = pytest.mark.skipif(
    not (DATABASE_URL and NATS_URL),
    reason="CLUE_VERIFY_DATABASE_URL and CLUE_VERIFY_NATS_URL select the isolated stack",
)

# 병합 전 워커 이름. durable 이름과 ledger consumer 가 이 값으로 유지되어야 한다.
LEGACY_CONSUMERS = {
    "ai/rca-worker": {
        "evidence.bundle.built": "plan-worker",
        "rca.candidates.planned": "analyze-worker",
        "rca.candidates.evaluated": "rca-worker",
    },
    "ai/recovery-worker": {
        "rca.completed": "recovery-worker",
        "recovery.planned": "select-worker",
        "recovery.action_selected": "dispatch-worker",
    },
}
CASES = [
    (service, subject, legacy)
    for service, mapping in sorted(LEGACY_CONSUMERS.items())
    for subject, legacy in mapping.items()
]
APP_NAME = {"ai/rca-worker": "rca-worker", "ai/recovery-worker": "recovery-worker"}


def planned_payload() -> dict[str, Any]:
    """디코드가 통과하는 최소 rca.candidates.planned 본문(증거 번들 없음)."""
    return RcaCandidatesPlannedBody(
        candidate_count=0, evidence_ref="synthetic-ref", candidates=[]
    ).to_body()


def assert_isolated_target() -> None:
    database = urlparse(DATABASE_URL)
    nats = urlparse(NATS_URL)
    assert database.hostname in LOOPBACK_HOSTS, "database must be loopback"
    assert nats.hostname in LOOPBACK_HOSTS, "nats must be loopback"
    assert database.path.lstrip("/").startswith(VERIFY_DATABASE_PREFIX), "unexpected database name"


async def eventually(
    check: Callable[[], Awaitable[Any]],
    what: str,
    *,
    timeout: float = POLL_TIMEOUT_SECONDS,
) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        value = await check()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for: {what}")
        await asyncio.sleep(0.1)


class Stack:
    """격리 스택에 붙은 DB·버스와 합성 검증용 헬퍼."""

    def __init__(self, heartbeat: Path) -> None:
        self.db = Database()
        self.verifier = NatsEventBus()
        self.heartbeat = heartbeat
        self.tasks: list[asyncio.Task[None]] = []
        self.buses: list[NatsEventBus] = []

    async def open(self) -> None:
        await wait_for_database(self.db)
        with self.db.engine.begin() as conn:
            for table in ("outbox", "event_dead_letters", "event_processing", "events"):
                conn.execute(text(f"DELETE FROM {table}"))
        await self.verifier.connect()
        assert self.verifier.js is not None
        await self.verifier.js.delete_stream(STREAM_NAME)
        await self.verifier.ensure_stream()

    async def close(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for bus in (*self.buses, self.verifier):
            # drain 은 취소된 pull fetch 때문에 타임아웃까지 기다리므로 연결만 닫는다.
            if bus.nc is not None:
                await bus.nc.close()
        self.db.dispose()
        await self.db.dispose_async()

    def start_worker(
        self,
        service: str,
        monkeypatch: pytest.MonkeyPatch,
        *,
        retry_policy: EventRetryPolicy | None = None,
        module: Any = None,
    ) -> None:
        monkeypatch.setattr("packages.runtime.worker.HEARTBEAT_PATH", str(self.heartbeat))
        # load_service 는 호출마다 새 모듈을 만든다. 고장 주입한 모듈을 그대로 구동하려면 넘겨야 한다.
        spec = (module or load_service(service)).app.handler_spec()
        if retry_policy is not None:
            spec = dataclasses.replace(spec, retry_policy=retry_policy)
        bus = NatsEventBus()
        self.buses.append(bus)
        self.tasks.append(asyncio.create_task(WorkerRuntime(spec, bus=bus, db=self.db).run()))

    def start_outbox_relay(self, monkeypatch: pytest.MonkeyPatch, *, module: Any = None) -> None:
        monkeypatch.setattr("packages.runtime.worker.HEARTBEAT_PATH", str(self.heartbeat))
        relay = module or load_service("gateway/outbox-relay")
        bus = NatsEventBus()
        self.buses.append(bus)
        self.tasks.append(asyncio.create_task(relay.run(bus)))

    async def publish(self, evt: EventEnvelope) -> None:
        await self.verifier.publish_envelope(evt)

    def seed_processed(self, evt: EventEnvelope, consumer: str) -> None:
        self.db.record_event(evt)
        self.db.begin_event_processing(evt, consumer)
        self.db.finish_event_processing(evt, consumer, 1)

    def ledger(self, event_id: str) -> list[tuple[str, str, int]]:
        with self.db.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT consumer, status, attempts FROM event_processing "
                    "WHERE event_id = :id ORDER BY consumer"
                ),
                {"id": event_id},
            )
            return [(r[0], r[1], r[2]) for r in rows]

    def outbox(self, **where: str) -> list[dict[str, Any]]:
        clause = " AND ".join(f"{column} = :{column}" for column in where) or "TRUE"
        with self.db.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT subject, source, causation_id, event_id, "
                    "sent_at IS NOT NULL AS sent FROM outbox "
                    f"WHERE {clause} ORDER BY id"
                ),
                where,
            )
            return [dict(r._mapping) for r in rows]

    def dead_letters(self, event_id: str) -> list[tuple[str, int]]:
        with self.db.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT consumer, attempts FROM event_dead_letters "
                    "WHERE original_event_id = :id"
                ),
                {"id": event_id},
            )
            return [(r[0], r[1]) for r in rows]

    def raise_if_worker_died(self) -> None:
        for task in self.tasks:
            if task.done() and not task.cancelled() and task.exception() is not None:
                raise AssertionError("worker task died") from task.exception()

    async def durables(self) -> set[str]:
        self.raise_if_worker_died()
        assert self.verifier.js is not None
        infos = await self.verifier.js.consumers_info(STREAM_NAME)
        return {info.name for info in infos}

    async def drained(self, durable: str) -> bool:
        self.raise_if_worker_died()
        assert self.verifier.js is not None
        try:
            info = await self.verifier.js.consumer_info(STREAM_NAME, durable)
        except NotFoundError:
            return False
        return (
            info.delivered.consumer_seq >= 1 and info.num_pending == 0 and info.num_ack_pending == 0
        )


@pytest.fixture
def stack_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Path]:
    assert_isolated_target()
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("NATS_URL", NATS_URL)
    monkeypatch.setenv("DATABASE_STARTUP_MODE", "initialize")
    monkeypatch.delenv("OUTBOX_RELAY_SOURCE", raising=False)
    saved = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield tmp_path / "heartbeat"
    finally:
        for sig, handler in saved.items():
            signal.signal(sig, handler)


def run_scenario(
    stack_env: Path,
    scenario: Callable[[Stack], Awaitable[None]],
) -> None:
    async def main() -> None:
        stack = Stack(stack_env)
        await stack.open()
        try:
            await scenario(stack)
        finally:
            await stack.close()

    asyncio.run(main())


@pytest.mark.parametrize(("service", "subject", "legacy"), CASES)
def test_event_processed_before_merge_is_not_reprocessed(
    service: str,
    subject: str,
    legacy: str,
    stack_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evt = event(subject, "pre-merge-worker", {}, "corr-processed")

    async def scenario(stack: Stack) -> None:
        stack.seed_processed(evt, legacy)
        await stack.publish(evt)
        stack.start_worker(service, monkeypatch)

        await eventually(lambda: stack.drained(legacy), f"durable {legacy} drained")
        await asyncio.sleep(SETTLE_SECONDS)

        assert stack.ledger(evt.event_id) == [(legacy, "processed", 1)]
        assert stack.outbox(causation_id=evt.event_id) == []
        assert stack.dead_letters(evt.event_id) == []
        durables = await stack.durables()
        assert legacy in durables
        assert durables <= set(LEGACY_CONSUMERS[service].values())

    run_scenario(stack_env, scenario)


@pytest.mark.parametrize(("service", "subject", "legacy"), CASES)
def test_each_subject_gets_its_legacy_durable_on_nats(
    service: str,
    subject: str,
    legacy: str,
    stack_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario(stack: Stack) -> None:
        stack.start_worker(service, monkeypatch)
        expected = set(LEGACY_CONSUMERS[service].values())

        await eventually(lambda: _durables_equal(stack, expected), f"durables == {expected}")

    run_scenario(stack_env, scenario)


async def _durables_equal(stack: Stack, expected: set[str]) -> bool:
    return await stack.durables() == expected


def test_unprocessed_event_is_processed_under_legacy_consumer_and_relayed(
    stack_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evt = event("rca.candidates.planned", "pre-merge-worker", planned_payload(), "corr-fresh")

    async def scenario(stack: Stack) -> None:
        await stack.publish(evt)
        stack.start_worker("ai/rca-worker", monkeypatch)

        await eventually(
            lambda: _ledger_status(stack, evt.event_id, "analyze-worker", "processed"),
            "analyze-worker ledger processed",
        )
        rows = await eventually(
            lambda: _outbox_rows(stack, evt.event_id), "child event staged in outbox"
        )
        assert {row["source"] for row in rows} == {"rca-worker"}
        assert stack.ledger(evt.event_id) == [("analyze-worker", "processed", 1)]

        await eventually(
            lambda: _all_sent(stack, evt.event_id), "own relay publishes the staged child"
        )

    run_scenario(stack_env, scenario)


async def _ledger_status(stack: Stack, event_id: str, consumer: str, status: str) -> bool:
    return any(row[0] == consumer and row[1] == status for row in stack.ledger(event_id))


async def _outbox_rows(stack: Stack, event_id: str) -> list[dict[str, Any]]:
    return stack.outbox(causation_id=event_id)


async def _all_sent(stack: Stack, event_id: str) -> bool:
    rows = stack.outbox(causation_id=event_id)
    return bool(rows) and all(row["sent"] for row in rows)


def test_handler_failure_retries_then_dead_letters_under_legacy_consumer(
    stack_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evt = event("rca.candidates.planned", "pre-merge-worker", planned_payload(), "corr-failing")
    attempts: list[str] = []

    class ExplodingPipeline:
        def evaluate_body(self, _evt: object) -> object:
            attempts.append(evt.event_id)
            raise RuntimeError("synthetic handler failure")

    async def scenario(stack: Stack) -> None:
        module = load_service("ai/rca-worker")
        monkeypatch.setattr(module, "evaluation_pipeline", ExplodingPipeline())
        await stack.publish(evt)
        stack.start_worker(
            "ai/rca-worker",
            monkeypatch,
            retry_policy=EventRetryPolicy(max_attempts=2, retry_delay_seconds=1),
            module=module,
        )

        await eventually(
            lambda: _ledger_status(stack, evt.event_id, "analyze-worker", "dead_lettered"),
            "analyze-worker ledger dead_lettered",
        )
        assert stack.ledger(evt.event_id) == [("analyze-worker", "dead_lettered", 2)]
        assert stack.dead_letters(evt.event_id) == [("analyze-worker", 2)]
        assert attempts == [evt.event_id, evt.event_id]
        # 실패한 핸들러의 결과는 outbox 에 적재되지 않는다(원자성). DLQ 알림은 버스로 직접 나간다.
        assert stack.outbox(causation_id=evt.event_id) == []
        notified = await _observed(stack, "dead_letter.created", "observer-dlq")
        assert [(e.source, e.causation_id) for e in notified] == [("rca-worker", evt.event_id)]

        await eventually(lambda: stack.drained("analyze-worker"), "analyze-worker drained")

    run_scenario(stack_env, scenario)


def _legacy_row(stack: Stack) -> EventEnvelope:
    evt = event("rca.completed", "plan-worker", {}, "corr-legacy-source")
    with stack.db.unit_of_work() as conn:
        stack.db.stage_events(conn, [evt])
    return evt


async def _observed(stack: Stack, subject: str, durable: str) -> list[EventEnvelope]:
    sub = await stack.verifier.subscribe(subject, durable)
    try:
        messages = await sub.fetch(10, timeout=SETTLE_SECONDS)
    except TimeoutError:
        return []
    out = [EventEnvelope.from_mapping(json.loads(m.data)) for m in messages]
    for message in messages:
        await message.ack()
    return out


def test_standalone_worker_relay_does_not_publish_old_source_rows(
    stack_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario(stack: Stack) -> None:
        legacy = _legacy_row(stack)
        current = event("rca.completed", "rca-worker", {}, "corr-current-source")
        with stack.db.unit_of_work() as conn:
            stack.db.stage_events(conn, [current])
        stack.start_worker("ai/rca-worker", monkeypatch)

        # 양성 대조: 워커 자체 relay 는 동작하며 병합 앱 이름의 source 행만 발행한다.
        await eventually(lambda: _event_sent(stack, current.event_id), "own relay publishes own")
        await asyncio.sleep(SETTLE_SECONDS)

        assert [row["sent"] for row in stack.outbox(event_id=legacy.event_id)] == [False]
        observed = await _observed(stack, "rca.completed", "observer-standalone")
        assert [e.event_id for e in observed] == [current.event_id]

    run_scenario(stack_env, scenario)


def test_controller_form_all_source_relay_publishes_old_source_rows(
    stack_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario(stack: Stack) -> None:
        evt = _legacy_row(stack)
        # controller 와 같은 구성: 병합 워커의 자체 relay(source 한정)와 별도 outbox-relay(전체) 공존
        stack.start_worker("ai/rca-worker", monkeypatch)
        stack.start_outbox_relay(monkeypatch)

        await eventually(
            lambda: _event_sent(stack, evt.event_id), "all-source relay publishes legacy row"
        )
        seen = await _observed(stack, "rca.completed", "observer-controller")
        assert [(e.event_id, e.source) for e in seen] == [(evt.event_id, "plan-worker")]

    run_scenario(stack_env, scenario)


async def _event_sent(stack: Stack, event_id: str) -> bool:
    rows = stack.outbox(event_id=event_id)
    return bool(rows) and rows[0]["sent"]


def test_source_restricted_outbox_relay_leaves_old_source_rows(
    stack_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay_module = load_service("gateway/outbox-relay")
    monkeypatch.setattr(relay_module, "OUTBOX_RELAY_SOURCE", "rca-worker")

    async def scenario(stack: Stack) -> None:
        legacy = _legacy_row(stack)
        current = event("rca.completed", "rca-worker", {}, "corr-current-source")
        with stack.db.unit_of_work() as conn:
            stack.db.stage_events(conn, [current])
        stack.start_outbox_relay(monkeypatch, module=relay_module)

        # 양성 대조: 제한된 relay 가 실제로 동작하며 자기 source 행은 발행한다.
        await eventually(lambda: _event_sent(stack, current.event_id), "relay publishes own source")
        await asyncio.sleep(SETTLE_SECONDS)

        assert [row["sent"] for row in stack.outbox(event_id=legacy.event_id)] == [False]

    run_scenario(stack_env, scenario)
