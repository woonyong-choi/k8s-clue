"""App — 서비스 하나를 선언하는 단일 진입점(FastAPI 의 app 처럼).

서비스 파일(app.py) 한 곳에서:
    app = App("rca-worker")          # 서비스 이름 = 정체성(여기 한 번만)

    @app.on(ClusterEvidenceReceivedBody)  # 구독: 이 이벤트 오면 이 함수
    async def on_evidence(evt, ctx):
        yield EvidenceBuiltBody(...)  # 체이닝 = yield

    if __name__ == "__main__":
        app.run()                       # NATS 붙여 실행

settings.py / 별도 핸들러 모듈 / import 배선 없음. 내부(NATS/ledger/
runtime)는 App 이 은닉.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from packages.config.errors import fail, require
from packages.contracts.event_bus.interfaces import EventConsumerBus
from packages.contracts.event_bus.registry import Subscription, events
from packages.contracts.event_bus.subjects import EventSubject
from packages.contracts.event_bus.subscriptions import ALL_EVENTS_SUBJECT
from packages.runtime.dispatch import (
    EventContext,
    make_event_handler,
    make_raw_handler,
    make_router,
)
from packages.runtime.spec import ServiceSpec

# App 과 EventContext 함께 쓰므로 여기서 재노출.
__all__ = ["App", "EventContext", "ServiceSpec"]


class App:
    def __init__(self, spec: str | ServiceSpec) -> None:
        self.spec = ServiceSpec(name=spec) if isinstance(spec, str) else spec
        self.name = self.spec.name
        self._handlers: dict[EventSubject, Subscription] = {}
        self._raw: tuple[Callable[..., Any], bool] | None = None
        self._consumers: dict[EventSubject, str] = {}

    def on(self, body_type: type, *, consumer: str | None = None) -> Callable[..., Any]:
        """
        타입 구독: 이 body 가 실린 이벤트 1종을 받음.

        consumer: 워커를 병합할 때 기존 durable·ledger·DLQ consumer 이름을 subject 별로 유지.
        """
        require(self._raw is None, f"{self.name}: @app.on·@app.on_any 혼용 불가", TypeError)
        subject = ensure_registered(body_type)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            ensure_unique_handler(self._handlers, subject)
            sub = Subscription(subject, body_type, fn, ensure_handler_signature(fn))
            self._handlers[subject] = sub
            if consumer is not None:
                self._consumers[subject] = consumer
            events.note_handler(self.name, sub)  # 카탈로그 표시용
            return fn

        return decorator

    def on_any(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        """
        전체(>) 구독: 모든 이벤트를 봉투(EventEnvelope) 그대로 받음.
        """
        require(not self._handlers and self._raw is None, f"{self.name}: 구독은 하나만", TypeError)
        self._raw = (fn, ensure_handler_signature(fn))
        events.note_raw_handler(self.name, fn.__name__)
        return fn

    @property
    def subscriptions(self) -> tuple[Subscription, ...]:
        return tuple(self._handlers.values())

    @property
    def raw_subscription(self) -> tuple[Callable[..., Any], bool] | None:
        return self._raw

    def run(self, bus: EventConsumerBus | None = None) -> None:
        """
        등록된 구독자를 이벤트 버스에 붙여 실행. 기본값은 NATS. (런타임은 지연 import)
        """
        from packages.runtime.service import WorkerService

        subjects, factory = self._resolve()
        WorkerService(
            self.name,
            tuple(subjects),
            factory,
            bus=bus,
            consumers=self.consumer_pins(),
        ).run()

    def handler_spec(self) -> Any:
        """Return the worker runtime spec for an external composition root."""
        from packages.runtime.worker import EventHandlerSpec

        subjects, factory = self._resolve()
        return EventHandlerSpec(
            service_name=self.name,
            subjects=tuple(subjects),
            handler_factory=factory,
            consumers=self.consumer_pins(),
        )

    def consumer_pins(self) -> tuple[tuple[str, str], ...]:
        """(subject, 유지할 consumer 이름) 쌍. 지정한 subject 만 포함."""
        return tuple((str(subject), name) for subject, name in self._consumers.items())

    def _resolve(self) -> tuple[list[str], Callable[..., Any]]:
        """구독 방식별 (subject 목록, 핸들러 factory) 구성 — 전체구독은 '>' 하나, 타입구독은 subject별 라우터."""
        if self._raw is not None:
            fn, wants_ctx = self._raw

            def raw_factory(client: Any, db: Any) -> Callable[..., Any]:
                return make_raw_handler(fn, wants_ctx, db, self.name)

            return [ALL_EVENTS_SUBJECT], raw_factory

        ensure_has_handler(self.name, self._handlers)
        subs = self.subscriptions

        def factory(client: Any, db: Any) -> Callable[..., Any]:
            return make_router({s.subject: make_event_handler(s, db, self.name) for s in subs})

        return [s.subject for s in subs], factory


# 등록 확인


def ensure_registered(body_type: type) -> Any:
    subject = getattr(body_type, "__subject__", None)
    require(subject is not None, f"{body_type.__name__} 미등록 — @event 필요", TypeError)
    return subject


def ensure_handler_signature(fn: Callable[..., Any]) -> bool:
    params = [p for p in inspect.signature(fn).parameters.values() if p.name != "self"]
    require(1 <= len(params) <= 2, f"{fn.__name__} 시그니처는 (evt) 또는 (evt, ctx)", TypeError)
    return len(params) == 2


def ensure_unique_handler(handlers: dict[Any, Any], subject: Any) -> None:
    if subject in handlers:
        fail(f"{subject} 구독자 중복: {handlers[subject].fn.__name__}", TypeError)


def ensure_has_handler(service: str, handlers: dict[Any, Any]) -> None:
    require(len(handlers) >= 1, f"{service}: 구독 핸들러가 없음(@app.on 필요)", RuntimeError)
