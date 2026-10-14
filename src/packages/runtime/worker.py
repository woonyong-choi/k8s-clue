from __future__ import annotations

import asyncio
import json
import math
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from packages.config.logs import get_logger
from packages.config.settings import env
from packages.contracts.event_bus.interfaces import (
    EventClient,
    EventConsumerBus,
    EventConsumerMetrics,
    EventEnvelope,
    EventHandler,
    EventMessage,
    EventSubscription,
)
from packages.contracts.event_bus.processing import TERMINAL_STATUSES, EventProcessingStatus
from packages.contracts.event_bus.subscriptions import durable_name
from packages.contracts.interfaces import EventProcessingStore
from packages.events.bus import (
    DeadLetterSink,
    NatsEventBus,
    RecordedEventClient,
    ack_wait_seconds,
    event_causation,
    event_context,
)
from packages.runtime.ledger import Ledger
from packages.runtime.relay import OutboxRelay

if TYPE_CHECKING:
    from packages.storage.database import Database

LOGGER = get_logger(__name__)

# 워커 처리량·재시도 정책 — 서비스별 deploy env 로 오버라이드 가능(기본값 불변).
# 주의: WORKER_HANDLER_TIMEOUT_SECONDS 를 올리면 NATS_ACK_WAIT_SECONDS(events/bus.py,
# 기본 60s)도 그보다 크게 함께 올려야 처리 중 재배달 중복이 방지됨.
WORKER_MAX_ATTEMPTS_ENV = "WORKER_MAX_ATTEMPTS"  # 핸들러 재시도 상한(소진 시 DLQ 종결)
WORKER_FETCH_BATCH_SIZE_ENV = "WORKER_FETCH_BATCH_SIZE"  # 루프당 subject 별 fetch 개수
WORKER_HANDLER_TIMEOUT_ENV = "WORKER_HANDLER_TIMEOUT_SECONDS"  # 핸들러 hang 상한 초
WORKER_RETRY_DELAY_ENV = "WORKER_RETRY_DELAY_SECONDS"  # nak 재확인 지연 초(기본 2)
WORKER_FETCH_TIMEOUT_ENV = "WORKER_FETCH_TIMEOUT_SECONDS"  # fetch 대기 한도 초(기본 1)
WORKER_IDLE_SLEEP_ENV = "WORKER_IDLE_SLEEP_SECONDS"  # 메시지/outbox 모두 없을 때 유휴 sleep 초
WORKER_DEAD_LETTER_TIMEOUT_ENV = (
    "WORKER_DEAD_LETTER_TIMEOUT_SECONDS"  # DLQ 기록 대기 한도 초(기본 10)
)
WORKER_HEARTBEAT_PATH_ENV = "WORKER_HEARTBEAT_PATH"  # liveness 하트비트 파일 경로
WORKER_CONSUMER_METRICS_INTERVAL_ENV = "WORKER_CONSUMER_METRICS_INTERVAL_SECONDS"
WORKER_MAX_CONCURRENCY_ENV = "WORKER_MAX_CONCURRENCY"  # 서로 다른 subject 동시 처리 상한


def _positive_float_env(name: str, fallback: float) -> float:
    try:
        value = float(env(name, str(fallback)))
    except (TypeError, ValueError):
        return fallback
    return value if math.isfinite(value) and value > 0 else fallback


def _positive_int_env(name: str, fallback: int) -> int:
    try:
        value = int(env(name, str(fallback)))
    except (TypeError, ValueError):
        return fallback
    return value if value > 0 else fallback


DEFAULT_MAX_ATTEMPTS = int(env(WORKER_MAX_ATTEMPTS_ENV, "3"))
DEFAULT_RETRY_DELAY_SECONDS = int(env(WORKER_RETRY_DELAY_ENV, "2"))
DEFAULT_FETCH_BATCH_SIZE = int(env(WORKER_FETCH_BATCH_SIZE_ENV, "1"))
DEFAULT_FETCH_TIMEOUT_SECONDS = _positive_float_env(WORKER_FETCH_TIMEOUT_ENV, 1.0)
DEFAULT_IDLE_SLEEP_SECONDS = float(env(WORKER_IDLE_SLEEP_ENV, "0.25"))
# 핸들러 hang 상한(안전망). 정상 최악 처리시간보다 넉넉히
DEFAULT_HANDLER_TIMEOUT_SECONDS = int(env(WORKER_HANDLER_TIMEOUT_ENV, "30"))
DEFAULT_DEAD_LETTER_TIMEOUT_SECONDS = int(env(WORKER_DEAD_LETTER_TIMEOUT_ENV, "10"))
DEFAULT_CONSUMER_METRICS_INTERVAL_SECONDS = float(env(WORKER_CONSUMER_METRICS_INTERVAL_ENV, "15"))
DEFAULT_MAX_CONCURRENCY = _positive_int_env(WORKER_MAX_CONCURRENCY_ENV, 8)
# liveness exec probe 가 mtime 신선도 검사 — 컨테이너 파일시스템 정책에 따라 경로 오버라이드 가능
HEARTBEAT_PATH = env(WORKER_HEARTBEAT_PATH_ENV, "/tmp/heartbeat")


def elapsed_ms(start: float) -> int:
    return max(0, int((time.perf_counter() - start) * 1000))


def validate_event_timing_contract(handler_timeout_seconds: int) -> None:
    """핸들러·재배달·claim 신선도 타이머의 안전 순서를 기동 시점에 강제한다.

    handler_timeout < ack_wait  : 처리 중 JetStream 재배달 금지 창 보장.
    ack_wait < processing_stale : 재배달이 와도 원 claim 이 stale 로 오판되어
                                  탈취되지 않음을 보장.
    이 순서가 깨지면 같은 이벤트를 두 소비자가 동시에 처리할 수 있다(조용한
    중복 실행). env 로만 존재하던 계약을 코드로 강제해, 잘못 조합된 배포는
    조용히 오작동하는 대신 부팅에서 즉시 실패한다.
    """
    from packages.storage.repositories.event import PROCESSING_STALE_SECONDS

    ack_wait = ack_wait_seconds()
    if not handler_timeout_seconds < ack_wait < PROCESSING_STALE_SECONDS:
        raise ValueError(
            "unsafe event timing contract: require "
            f"WORKER_HANDLER_TIMEOUT_SECONDS({handler_timeout_seconds}) "
            f"< NATS_ACK_WAIT_SECONDS({ack_wait}) "
            f"< EVENT_PROCESSING_STALE_SECONDS({PROCESSING_STALE_SECONDS})"
        )


@dataclass(frozen=True)
class EventRetryPolicy:
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    retry_delay_seconds: int = DEFAULT_RETRY_DELAY_SECONDS
    fetch_batch_size: int = DEFAULT_FETCH_BATCH_SIZE
    fetch_timeout_seconds: float = DEFAULT_FETCH_TIMEOUT_SECONDS
    idle_sleep_seconds: float = DEFAULT_IDLE_SLEEP_SECONDS
    handler_timeout_seconds: int = DEFAULT_HANDLER_TIMEOUT_SECONDS
    dead_letter_timeout_seconds: int = DEFAULT_DEAD_LETTER_TIMEOUT_SECONDS
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY


@dataclass(frozen=True)
class EventHandlerSpec:
    service_name: str
    subjects: tuple[str, ...]
    handler_factory: Callable[[EventClient, Database], EventHandler]
    durable_name: str | None = None
    retry_policy: EventRetryPolicy = EventRetryPolicy()
    # 병합 전 워커의 consumer identity 보존용 (subject, consumer) 쌍.
    # durable 이름과 ledger·DLQ consumer 를 subject 별로 기존 값 그대로 유지한다.
    consumers: tuple[tuple[str, str], ...] = ()

    @property
    def durable(self) -> str:
        return durable_name(self.service_name, self.durable_name)

    def consumer_for(self, subject: str) -> str:
        """ledger·DLQ 에 기록되는 consumer 이름. 지정이 없으면 서비스 이름."""
        return dict(self.consumers).get(subject, self.service_name)

    def durable_for(self, subject: str) -> str:
        """subject 여러 개면 컨슈머 이름 충돌 방지 위해 subject 로 namespace."""
        pinned = dict(self.consumers).get(subject)
        if pinned is not None:
            return durable_name(pinned, self.durable_name)
        if len(self.subjects) <= 1:
            return self.durable
        slug = subject.replace(".", "-").replace(">", "all").replace("*", "any")
        return f"{self.durable}-{slug}"


async def record_consumer_lag_metrics(
    bus: EventConsumerBus,
    store: object,
    spec: EventHandlerSpec,
) -> None:
    consumer_metrics = getattr(bus, "consumer_metrics", None)
    record_metrics_batch = getattr(store, "record_event_consumer_metrics_batch", None)
    record_metrics = getattr(store, "record_event_consumer_metrics", None)
    if not callable(consumer_metrics) or not (
        callable(record_metrics_batch) or callable(record_metrics)
    ):
        return

    semaphore = asyncio.Semaphore(spec.retry_policy.max_concurrency)

    async def collect(subject: str) -> EventConsumerMetrics | None:
        durable = spec.durable_for(subject)
        try:
            async with semaphore:
                return await consumer_metrics(subject, durable)
        except Exception as exc:
            LOGGER.warning(
                "consumer_metrics_error",
                extra={"context": {"consumer": durable, "subject": subject}},
                exc_info=exc,
            )
            return None

    samples = tuple(
        sample
        for sample in await asyncio.gather(*(collect(subject) for subject in spec.subjects))
        if sample is not None
    )
    if not samples:
        return

    try:
        if callable(record_metrics_batch):
            record_metrics_batch(samples)
        else:
            assert callable(record_metrics)
            for sample in samples:
                record_metrics(sample)
    except Exception as exc:
        LOGGER.warning(
            "consumer_metrics_persist_error",
            extra={"context": {"consumer": spec.service_name, "sample_count": len(samples)}},
            exc_info=exc,
        )


class Codec(Protocol):
    def decode(self, message: EventMessage) -> EventEnvelope: ...


class JsonCodec:
    def decode(self, message: EventMessage) -> EventEnvelope:
        return EventEnvelope.from_mapping(json.loads(message.data.decode()))


class DeadLetterPort(Protocol):
    async def capture(
        self, evt: EventEnvelope, consumer: str, error: Exception, attempts: int
    ) -> EventEnvelope: ...

    async def capture_raw(self, raw: bytes, consumer: str, error: Exception) -> EventEnvelope: ...


class EventProcessor:
    def __init__(
        self,
        spec: EventHandlerSpec,
        handler: EventHandler,
        store: EventProcessingStore,
        dead_letters: DeadLetterPort,
        codec: Codec | None = None,
        ledger: Ledger | None = None,
    ) -> None:
        self.service_name = spec.service_name
        self.retry_policy = spec.retry_policy
        self.consumers = dict(spec.consumers)
        self.ledgers: dict[str, Ledger] = {}
        self.handler = handler
        self.store = store
        self.dead_letters = dead_letters
        self.codec = codec if codec is not None else JsonCodec()
        self.ledger = ledger if ledger is not None else Ledger(store, self.service_name)

    def consumer_for(self, evt: EventEnvelope) -> str:
        return self.consumers.get(evt.subject, self.service_name)

    def ledger_for(self, evt: EventEnvelope) -> Ledger:
        consumer = self.consumer_for(evt)
        if consumer == self.service_name:
            return self.ledger
        if consumer not in self.ledgers:
            self.ledgers[consumer] = Ledger(self.store, consumer)
        return self.ledgers[consumer]

    async def process(self, message: EventMessage) -> None:
        try:
            evt = self.codec.decode(message)
        except Exception as exc:
            try:
                await asyncio.wait_for(
                    self.dead_letters.capture_raw(message.data, self.service_name, exc),
                    timeout=self.retry_policy.dead_letter_timeout_seconds,
                )
            except Exception as capture_error:
                LOGGER.error(
                    "raw_dead_letter_capture_failed",
                    extra={"context": {"consumer": self.service_name}},
                    exc_info=capture_error,
                )
                await message.nak(delay=self.retry_policy.retry_delay_seconds)
                return
            LOGGER.error(
                "decode_dead_letter",
                extra={"context": {"consumer": self.service_name}},
                exc_info=exc,
            )
            await message.ack()
            return
        # 1) claim 을 별도 트랜잭션으로 먼저 커밋. attempt 증가가 핸들러 실패에 롤백되면
        #    재시도 횟수 미누적으로 영구 DLQ 미도달 위험 → claim 과 업무 분리
        consumer = self.consumer_for(evt)
        ledger = self.ledger_for(evt)
        with self.store.unit_of_work():
            processing = ledger.begin(evt)  # 처리대장에 "처리 시작" 기록 + attempt 증가
        if processing.status in TERMINAL_STATUSES:
            await message.ack()  # 이미 처리됨/소진됨(중복) → 건너뛰기(정확히 한 번 핵심)
            return
        if processing.status != EventProcessingStatus.PROCESSING:
            # claim 미획득(다른 인스턴스의 신선한 PROCESSING 등) — 종결이 아니므로
            # ack 로 소거하면 원 처리자가 죽었을 때 이벤트 유실 → nak 로 재확인 예약
            await message.nak(delay=self.retry_policy.retry_delay_seconds)
            return
        attempts = processing.attempts  # 지금까지 시도 횟수(커밋되어 누적)
        started_at = time.perf_counter()
        # 2) 업무쓰기 + outbox 적재 + ledger 완료를 한 트랜잭션으로(원자성).
        try:
            with self.store.unit_of_work() as conn:
                context = {**event_context(evt), "consumer": consumer}
                LOGGER.info("handling", extra={"context": context})
                with event_causation(evt.event_id):  # 자식 이벤트들의 부모 = 이 이벤트
                    # 핸들러 hang 상한: 초과 시 TimeoutError → 아래 except → fail()(재시도/DLQ).
                    # 트랜잭션 안이라 취소돼도 롤백 → 부분 쓰기 없음.
                    outbox_events = await asyncio.wait_for(
                        self.handler(evt), timeout=self.retry_policy.handler_timeout_seconds
                    )  # 핸들러 실행 → 다음 이벤트 봉투들 수집
                self.store.stage_events(conn, outbox_events)  # 다음 이벤트들을 outbox 에 적재
                ledger.finish(evt, elapsed_ms(started_at))  # 처리대장에 "완료" 기록
            # with 끝 = 트랜잭션 커밋(업무 + outbox 함께 저장)
        except Exception as exc:
            # claim 이 이미 커밋되어 attempt 가 누적된 상태 → fail 이 그 row 를 갱신(재시도/DLQ).
            await self.fail(message, evt, exc, attempts, elapsed_ms(started_at))
            return

        # commit 이후 ack 실패는 WorkerRuntime 으로 전파해 nak/redelivery 하되,
        # PROCESSED ledger 는 유지하여 중복 업무 실행을 막는다.
        await message.ack()

    async def fail(
        self,
        message: EventMessage,
        evt: EventEnvelope,
        error: Exception,
        attempts: int,
        duration_ms: int | None = None,
    ) -> None:
        consumer = self.consumer_for(evt)
        ledger = self.ledger_for(evt)
        context = {**event_context(evt), "consumer": consumer, "attempts": attempts}
        if attempts >= self.retry_policy.max_attempts:
            # DLQ 기록을 먼저, ledger DEAD_LETTERED 표시를 나중에 함. 둘이 한 트랜잭션이 아니라
            # 사이에 크래시할 수 있는데, 이 순서면 '유실'이 아니라 '재처리(최악 중복 DLQ)'가 됨
            # — DLQ 행은 남고 상태는 아직 안 닫혀 재배달 시 다시 처리/DLQ(복구 가능).
            try:
                await asyncio.wait_for(
                    self.dead_letters.capture(evt, consumer, error, attempts),
                    timeout=self.retry_policy.dead_letter_timeout_seconds,
                )
            except Exception as capture_error:
                ledger.retry(evt, error, duration_ms)
                LOGGER.error(
                    "dead_letter_capture_failed",
                    extra={"context": context},
                    exc_info=capture_error,
                )
                await message.nak(delay=self.retry_policy.retry_delay_seconds)
                return
            ledger.dead_letter(evt, error, duration_ms)
            LOGGER.error("dead_letter", extra={"context": context}, exc_info=error)
            await message.ack()
            return

        ledger.retry(evt, error, duration_ms)
        LOGGER.warning("retry", extra={"context": context}, exc_info=error)
        await message.nak(delay=self.retry_policy.retry_delay_seconds)


class WorkerRuntime:
    def __init__(
        self,
        spec: EventHandlerSpec,
        bus: EventConsumerBus | None = None,
        db: Database | None = None,
    ) -> None:
        self.spec = spec
        if db is None:
            from packages.storage.database import Database

            db = Database()
        self.bus = bus if bus is not None else NatsEventBus()
        self.db = db

    async def consume_subject(
        self,
        subject: str,
        subscription: EventSubscription,
        processor: EventProcessor,
        stopping: asyncio.Event,
        semaphore: asyncio.Semaphore,
    ) -> None:
        """subject 하나를 순서대로 처리하고 다른 subject와만 병렬 실행한다."""
        context = {
            "consumer": self.spec.service_name,
            "durable": self.spec.durable_for(subject),
            "subject": subject,
        }
        while not stopping.is_set():
            try:
                messages = await subscription.fetch(
                    self.spec.retry_policy.fetch_batch_size,
                    timeout=self.spec.retry_policy.fetch_timeout_seconds,
                )
            except TimeoutError:
                continue
            except Exception as exc:
                LOGGER.warning("fetch_error", extra={"context": context}, exc_info=exc)
                await self.wait_or_stop(stopping, 1.0)
                continue

            if not messages:
                await self.wait_or_stop(stopping, self.spec.retry_policy.idle_sleep_seconds)
                continue

            for message in messages:
                if stopping.is_set():
                    await message.nak(delay=self.spec.retry_policy.retry_delay_seconds)
                    continue
                try:
                    async with semaphore:
                        await processor.process(message)
                except Exception as exc:
                    LOGGER.error("processor_error", extra={"context": context}, exc_info=exc)
                    await message.nak(delay=self.spec.retry_policy.retry_delay_seconds)

    @staticmethod
    async def wait_or_stop(stopping: asyncio.Event, timeout: float) -> None:
        try:
            await asyncio.wait_for(stopping.wait(), timeout=max(0.0, timeout))
        except TimeoutError:
            pass

    async def run(self) -> None:
        from packages.storage.database import wait_for_database

        validate_event_timing_contract(self.spec.retry_policy.handler_timeout_seconds)
        Path(HEARTBEAT_PATH).touch()  # 시작 즉시 생존 표시(DB 대기 중 liveness 오살 방지)
        await wait_for_database(self.db)
        await self.bus.connect()
        # subject마다 별도 컨슈머와 fetch task를 둔다. 같은 subject는 선언 순서를
        # 보존하고, 서로 다른 subject만 semaphore 상한 안에서 병렬 처리한다.
        subs = [
            (subject, await self.bus.subscribe(subject, durable=self.spec.durable_for(subject)))
            for subject in self.spec.subjects
        ]
        events = RecordedEventClient(self.bus, self.db)
        handler = self.spec.handler_factory(events, self.db)
        processor = EventProcessor(
            self.spec,
            handler,
            self.db,
            DeadLetterSink(events, self.db, self.spec.service_name),
        )
        relay = OutboxRelay(self.db, self.bus, self.spec.service_name)
        stopping = asyncio.Event()
        signal.signal(signal.SIGTERM, lambda *_: stopping.set())
        signal.signal(signal.SIGINT, lambda *_: stopping.set())
        lifecycle = {"consumer": self.spec.service_name, "subjects": list(self.spec.subjects)}
        LOGGER.info("subscribed", extra={"context": lifecycle})
        last_consumer_metrics_at = 0.0
        semaphore = asyncio.Semaphore(self.spec.retry_policy.max_concurrency)
        consumer_tasks = [
            asyncio.create_task(
                self.consume_subject(subject, sub, processor, stopping, semaphore),
                name=f"{self.spec.service_name}:{subject}",
            )
            for subject, sub in subs
        ]

        try:
            while not stopping.is_set():
                for task in consumer_tasks:
                    if not task.done():
                        continue
                    error = task.exception()
                    if error is not None:
                        raise error
                    raise RuntimeError(f"consumer task stopped unexpectedly: {task.get_name()}")
                Path(HEARTBEAT_PATH).touch()  # liveness 하트비트(루프 생존 신호)
                now = time.monotonic()
                if now - last_consumer_metrics_at >= DEFAULT_CONSUMER_METRICS_INTERVAL_SECONDS:
                    await record_consumer_lag_metrics(self.bus, self.db, self.spec)
                    last_consumer_metrics_at = now
                try:
                    await relay.run_once()  # outbox → NATS 발행
                except Exception as exc:
                    LOGGER.warning("relay_error", extra={"context": lifecycle}, exc_info=exc)
                await self.wait_or_stop(stopping, self.spec.retry_policy.idle_sleep_seconds)
        finally:
            stopping.set()
            await asyncio.gather(*consumer_tasks, return_exceptions=True)

        await self.bus.close()
        dispose_async = getattr(self.db, "dispose_async", None)
        if dispose_async is not None:
            await dispose_async()
        dispose = getattr(self.db, "dispose", None)
        if dispose is not None:
            dispose()
