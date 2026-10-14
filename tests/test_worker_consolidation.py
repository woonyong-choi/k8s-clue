from __future__ import annotations

import asyncio
import dataclasses
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from conftest import load_service

from packages.contracts.event_bus.interfaces import EventEnvelope
from packages.contracts.event_bus.processing import EventProcessingStatus
from packages.contracts.interfaces import EventProcessingRecord
from packages.events.envelope import event
from packages.runtime import controller
from packages.runtime.discovery import discover_services
from packages.runtime.worker import (
    EventHandlerSpec,
    EventProcessor,
    EventRetryPolicy,
    WorkerRuntime,
)

ROOT = Path(__file__).resolve().parents[1]
MERGED_AWAY = ("analyze-worker", "plan-worker", "select-worker", "dispatch-worker")
EXPECTED_SUBJECTS = {
    "ai/rca-worker": {
        "evidence.bundle.built",
        "rca.candidates.planned",
        "rca.candidates.evaluated",
    },
    "ai/recovery-worker": {
        "rca.completed",
        "recovery.planned",
        "recovery.action_selected",
    },
}


def test_merged_workers_are_removed_from_discovery_and_controller() -> None:
    names = {service.name for service in discover_services(ROOT)}

    assert names.isdisjoint(MERGED_AWAY)
    assert names.isdisjoint(set(MERGED_AWAY) & controller.CORE_CONTROLLER_SERVICE_NAMES)
    plan = controller.build_composition_plan(ROOT, event_bus_mode="inprocess")
    assert {service.name for service in plan.controller_services} == (
        controller.CORE_CONTROLLER_SERVICE_NAMES
    )


def test_independent_responsibilities_stay_separate() -> None:
    assert {"evidence-worker", "incident-worker", "rca-feedback-worker", "ai-diff-worker"} <= (
        controller.CORE_CONTROLLER_SERVICE_NAMES
    )


@pytest.mark.parametrize("service", sorted(EXPECTED_SUBJECTS))
def test_merged_worker_keeps_every_original_subscription(service: str) -> None:
    app = load_service(service).app
    spec = app.handler_spec()

    assert set(spec.subjects) == EXPECTED_SUBJECTS[service]
    assert len(spec.subjects) == len(set(spec.subjects))


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


@pytest.mark.parametrize("service", sorted(LEGACY_CONSUMERS))
def test_each_subject_keeps_its_legacy_durable_and_consumer(service: str) -> None:
    spec = load_service(service).app.handler_spec()

    for subject, legacy in LEGACY_CONSUMERS[service].items():
        assert spec.durable_for(subject) == legacy
        assert spec.consumer_for(subject) == legacy


def test_unpinned_multi_subject_worker_still_namespaces_durables() -> None:
    spec = EventHandlerSpec(
        service_name="svc",
        subjects=("a.b", "c.d"),
        handler_factory=lambda _client, _db: None,  # type: ignore[arg-type,return-value]
    )

    assert spec.durable_for("a.b") == "svc-a-b"
    assert spec.consumer_for("a.b") == "svc"


class FakeStore:
    def __init__(self) -> None:
        self.ledger: dict[tuple[str, str], tuple[str, int]] = {}
        self.staged: list[EventEnvelope] = []
        self.dead_letters: list[tuple[str, str]] = []

    @contextmanager
    def unit_of_work(self) -> Iterator[object]:
        yield object()

    def record_event(self, evt: EventEnvelope) -> None:
        return None

    def begin_event_processing(self, evt: EventEnvelope, consumer: str) -> EventProcessingRecord:
        key = (evt.event_id, consumer)
        status, attempts = self.ledger.get(key, ("", 0))
        if status in (EventProcessingStatus.PROCESSED, EventProcessingStatus.DEAD_LETTERED):
            return EventProcessingRecord(status=status, attempts=attempts)
        self.ledger[key] = (EventProcessingStatus.PROCESSING, attempts + 1)
        return EventProcessingRecord(status=EventProcessingStatus.PROCESSING, attempts=attempts + 1)

    def finish_event_processing(
        self, evt: EventEnvelope, consumer: str, duration_ms: int | None
    ) -> None:
        key = (evt.event_id, consumer)
        self.ledger[key] = (EventProcessingStatus.PROCESSED, self.ledger[key][1])

    def fail_event_processing(
        self,
        evt: EventEnvelope,
        consumer: str,
        error: str,
        status: EventProcessingStatus,
        duration_ms: int | None,
    ) -> None:
        key = (evt.event_id, consumer)
        self.ledger[key] = (status, self.ledger[key][1])

    def stage_events(self, conn: object, events: list[EventEnvelope]) -> None:
        self.staged.extend(events)


class FakeDeadLetters:
    def __init__(self, store: FakeStore) -> None:
        self.store = store

    async def capture(
        self, evt: EventEnvelope, consumer: str, error: Exception, attempts: int
    ) -> EventEnvelope:
        self.store.dead_letters.append((evt.event_id, consumer))
        return evt

    async def capture_raw(self, raw: bytes, consumer: str, error: Exception) -> EventEnvelope:
        raise AssertionError("raw dead letter is not expected")


class FakeMessage:
    def __init__(self, evt: EventEnvelope) -> None:
        self.data = json.dumps(evt.to_dict()).encode()
        self.acks = 0
        self.naks = 0

    async def ack(self) -> None:
        self.acks += 1

    async def nak(self, delay: int = 0) -> None:
        self.naks += 1


def make_processor(
    store: FakeStore,
    handler: object,
    *,
    service: str = "ai/rca-worker",
    max_attempts: int = 3,
) -> EventProcessor:
    spec = dataclasses.replace(
        load_service(service).app.handler_spec(),
        retry_policy=EventRetryPolicy(max_attempts=max_attempts, retry_delay_seconds=0),
    )
    return EventProcessor(
        spec,
        handler,  # type: ignore[arg-type]
        store,  # type: ignore[arg-type]
        FakeDeadLetters(store),
    )


def evt_for(subject: str) -> EventEnvelope:
    return event(subject, "test", {}, "corr-1")


def ok_handler(out: list[EventEnvelope]) -> object:
    async def handle(evt: EventEnvelope) -> list[EventEnvelope]:
        return out

    return handle


def test_processed_event_is_not_reprocessed_under_legacy_consumer() -> None:
    store = FakeStore()
    evt = evt_for("evidence.bundle.built")
    # 병합 전 plan-worker 가 이미 처리한 이벤트
    store.ledger[(evt.event_id, "plan-worker")] = (EventProcessingStatus.PROCESSED, 1)
    calls: list[str] = []

    async def handle(received: EventEnvelope) -> list[EventEnvelope]:
        calls.append(received.event_id)
        return []

    message = FakeMessage(evt)
    asyncio.run(make_processor(store, handle).process(message))  # type: ignore[arg-type]

    assert calls == []
    assert message.acks == 1
    assert store.staged == []


def test_unprocessed_event_is_processed_and_recorded_under_legacy_consumer() -> None:
    store = FakeStore()
    evt = evt_for("rca.candidates.planned")
    child = event("rca.candidates.evaluated", "rca-worker", {}, "corr-1", evt.event_id)
    message = FakeMessage(evt)

    asyncio.run(make_processor(store, ok_handler([child])).process(message))  # type: ignore[arg-type]

    assert store.ledger == {(evt.event_id, "analyze-worker"): (EventProcessingStatus.PROCESSED, 1)}
    assert store.staged == [child]
    assert message.acks == 1


def test_same_event_is_independent_per_legacy_consumer() -> None:
    store = FakeStore()
    evt = evt_for("recovery.planned")
    store.ledger[(evt.event_id, "recovery-worker")] = (EventProcessingStatus.PROCESSED, 1)
    processor = make_processor(store, ok_handler([]), service="ai/recovery-worker")

    asyncio.run(processor.process(FakeMessage(evt)))  # type: ignore[arg-type]

    assert store.ledger[(evt.event_id, "select-worker")][0] == EventProcessingStatus.PROCESSED


def test_failed_handler_retries_without_outbox_then_dead_letters_under_legacy_consumer() -> None:
    store = FakeStore()
    evt = evt_for("evidence.bundle.built")

    async def boom(received: EventEnvelope) -> list[EventEnvelope]:
        raise RuntimeError("boom")

    processor = make_processor(store, boom, max_attempts=2)
    first = FakeMessage(evt)
    asyncio.run(processor.process(first))  # type: ignore[arg-type]

    assert first.naks == 1
    assert store.ledger[(evt.event_id, "plan-worker")] == (EventProcessingStatus.RETRYING, 1)
    assert store.staged == []

    second = FakeMessage(evt)
    asyncio.run(processor.process(second))  # type: ignore[arg-type]

    assert second.acks == 1
    assert store.ledger[(evt.event_id, "plan-worker")] == (EventProcessingStatus.DEAD_LETTERED, 2)
    assert store.dead_letters == [(evt.event_id, "plan-worker")]
    assert store.staged == []


def test_merged_subjects_are_not_consumed_by_other_workers() -> None:
    merged = {name.split("/")[1] for name in EXPECTED_SUBJECTS}
    owned = set().union(*EXPECTED_SUBJECTS.values())
    for service in discover_services(ROOT):
        if service.kind != "worker" or service.name in merged:
            continue
        module = load_service(service.path.parent.relative_to("src/services").as_posix())
        assert owned.isdisjoint(module.app.handler_spec().subjects), service.name


class FakeRuntimeStore(FakeStore):
    """WorkerRuntime.run 이 요구하는 저장소 표면까지 갖춘 가짜."""

    async def unsent_events(self, batch: int, source: str | None) -> list[EventEnvelope]:
        return []


class FakeSubscription:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self.messages = messages

    async def fetch(self, batch: int, timeout: float | None = None) -> list[FakeMessage]:
        if self.messages:
            return [self.messages.pop(0)]
        await asyncio.sleep(0.01)
        raise TimeoutError


class FakeBus:
    def __init__(self, inbox: dict[str, list[FakeMessage]]) -> None:
        self.inbox = inbox
        self.durables: dict[str, str] = {}

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def subscribe(self, subject: str, durable: str) -> FakeSubscription:
        self.durables[subject] = durable
        return FakeSubscription(self.inbox.get(subject, []))

    async def publish_envelope(self, evt: EventEnvelope) -> EventEnvelope:
        return evt


async def run_worker_runtime(
    runtime: WorkerRuntime,
    messages: list[FakeMessage],
) -> None:
    task = asyncio.create_task(runtime.run())
    for _ in range(300):
        if all(m.acks + m.naks for m in messages) or task.done():
            break
        await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize(
    ("service", "subject", "legacy"),
    [
        (service, subject, legacy)
        for service, mapping in sorted(LEGACY_CONSUMERS.items())
        for subject, legacy in mapping.items()
    ],
)
def test_worker_runtime_skips_event_processed_before_merge(
    service: str,
    subject: str,
    legacy: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def no_wait(db: object) -> None:
        return None

    monkeypatch.setattr("packages.storage.database.wait_for_database", no_wait)
    monkeypatch.setattr("packages.runtime.worker.HEARTBEAT_PATH", str(tmp_path / "heartbeat"))
    store = FakeRuntimeStore()
    evt = evt_for(subject)
    store.ledger[(evt.event_id, legacy)] = (EventProcessingStatus.PROCESSED, 1)
    message = FakeMessage(evt)
    bus = FakeBus({subject: [message]})
    spec = load_service(service).app.handler_spec()

    asyncio.run(run_worker_runtime(WorkerRuntime(spec, bus=bus, db=store), [message]))  # type: ignore[arg-type]

    assert bus.durables[subject] == legacy
    assert message.acks == 1
    assert message.naks == 0
    assert store.staged == []
    assert set(store.ledger) == {(evt.event_id, legacy)}
