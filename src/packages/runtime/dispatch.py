"""이벤트 핸들러 실행 기계장치(런타임).

registry(카탈로그, "어떤 이벤트가 있나")와 분리. 여기는 런타임 —
"이벤트를 받아 body 로 디코드 → 핸들러 실행 → yield 된 다음 이벤트 수집".
수집한 이벤트는 EventProcessor 가 한 트랜잭션으로 outbox 에 적재(직접 발행 안 함).
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

from packages.config.errors import fail
from packages.contracts.event_bus.bodies.base import EventBody
from packages.contracts.event_bus.interfaces import EventEnvelope
from packages.contracts.event_bus.registry import Subscription
from packages.contracts.event_bus.subscriptions import ALL_EVENTS_SUBJECT
from packages.events.envelope import event
from packages.runtime.async_db import AsyncDb


@dataclass(frozen=True)
class EventContext[DbT]:
    """핸들러가 받는 꾸러미: 흐름 정보 + 도구(db). 안 쓰면 생략 가능."""

    event_id: str
    subject: str
    correlation_id: str
    causation_id: str | None
    db: DbT
    source: str = ""
    created_at: str = ""
    workspace_id: str | None = None

    @classmethod
    def of(cls, evt: EventEnvelope, db: DbT) -> EventContext[DbT]:
        return cls(
            event_id=evt.event_id,
            subject=evt.subject,
            correlation_id=evt.correlation_id,
            causation_id=evt.causation_id,
            db=db,
            source=evt.source,
            created_at=evt.created_at,
            workspace_id=evt.workspace_id,
        )


@dataclass(frozen=True)
class CorrelatedResult:
    body: EventBody
    correlation_id: str


async def _iter_results(result: Any) -> AsyncIterator[Any]:
    """핸들러 결과를 body 스트림으로 통일: yield형 / list / 단건 / None."""
    if inspect.isasyncgen(result):
        async for item in result:
            yield item
        return
    value = await result
    if value is None:
        return
    if isinstance(value, list):
        for item in value:
            yield item
    else:
        yield value


async def _collect(source: str, evt: EventEnvelope, result: Any) -> list[EventEnvelope]:
    """yield 된 body 를 발행할 EventEnvelope 로 수집(causation=부모 event_id)."""
    out: list[EventEnvelope] = []
    async for item in _iter_results(result):
        body = item.body if isinstance(item, CorrelatedResult) else item
        correlation_id = (
            item.correlation_id if isinstance(item, CorrelatedResult) else evt.correlation_id
        )
        if not correlation_id:
            raise ValueError("event correlation id is required")
        out.append(
            event(
                body.__subject__,
                source,
                body.to_body(),
                correlation_id,
                evt.event_id,
                workspace_id=evt.workspace_id,
            )
        )
    return out


def make_event_handler(sub: Subscription, db: Any, source: str) -> Callable[[EventEnvelope], Any]:
    """타입 구독: 봉투 → body 디코드 → 콜백 → yield된 body 수집."""

    async def handle(evt: EventEnvelope) -> list[EventEnvelope]:
        body = sub.body_type.from_body(evt.payload, strict=False)
        ctx = EventContext.of(evt, AsyncDb(db))
        result = sub.fn(body, ctx) if sub.wants_ctx else sub.fn(body)
        return await _collect(source, evt, result)

    return handle


def make_router(
    handlers: dict[str, Callable[[EventEnvelope], Any]],
) -> Callable[[EventEnvelope], Any]:
    """한 워커가 여러 subject 구독 시: evt.subject 로 핸들러 선택(없으면 '>' 전체구독)."""

    async def route(evt: EventEnvelope) -> list[EventEnvelope]:
        handler = handlers.get(evt.subject) or handlers.get(ALL_EVENTS_SUBJECT)
        if handler is None:
            fail(f"{evt.subject} 구독 핸들러 없음", RuntimeError)
        return await handler(evt)

    return route


def make_raw_handler(
    fn: Callable[..., Any], wants_ctx: bool, db: Any, source: str
) -> Callable[[EventEnvelope], Any]:
    """전체(>) 구독: 디코드 없이 봉투 그대로 → 콜백 → yield된 body 수집."""

    async def handle(evt: EventEnvelope) -> list[EventEnvelope]:
        ctx = EventContext.of(evt, AsyncDb(db))
        result = fn(evt, ctx) if wants_ctx else fn(evt)
        return await _collect(source, evt, result)

    return handle
