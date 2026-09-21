"""테스트 공용 헬퍼 — 워커 로드/실행/검증을 한 줄로."""

from __future__ import annotations

import asyncio
import base64
import importlib.util
import inspect
import json
import os
import sys
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from packages.runtime.app import EventContext

ROOT = Path(__file__).resolve().parents[1]

SERVICE_LOCAL_MODULES = (
    "settings",
    "config",
    "github_provider",
    "kubernetes_api",
    "evidence",
    "evidence.collector",
    "evidence.jobs",
    "providers",
    "providers.base",
    "providers.collection_limits",
    "queries",
    "queries.payloads",
    "queries.registry",
    "telemetry_registry",
    "span",
    "span.base",
    "span.otel",
)


def load_file(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    previous_modules = {
        module_name: sys.modules.pop(module_name, None) for module_name in SERVICE_LOCAL_MODULES
    }
    sys.path.insert(0, str(path.parent))
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(path.parent))
        for module_name in SERVICE_LOCAL_MODULES:
            sys.modules.pop(module_name, None)
            if previous_modules[module_name] is not None:
                sys.modules[module_name] = previous_modules[module_name]


def load_service(name: str) -> Any:
    module = name.replace("/", "_")
    return load_file(ROOT / "src" / "services" / name / "app.py", f"svc_{module}")


def make_context(db: Any = None, **fields: Any) -> EventContext:
    base: dict[str, Any] = {
        "event_id": "evt-1",
        "subject": "test",
        "correlation_id": "corr-1",
        "causation_id": None,
        "db": db,
    }
    base.update(fields)
    return EventContext(**base)


def run_handler(
    handler: Callable[..., AsyncIterator[Any]], payload: Any, db: Any = None, **fields: Any
) -> list[Any]:
    ctx = make_context(db=db, **fields)
    params = [p for p in inspect.signature(handler).parameters.values() if p.name != "self"]
    if len(params) not in {1, 2}:
        raise TypeError(f"{handler.__name__} 시그니처는 (evt) 또는 (evt, ctx)")
    wants_ctx = len(params) == 2

    async def go() -> list[Any]:
        result = handler(payload, ctx) if wants_ctx else handler(payload)
        if inspect.isasyncgen(result):
            return [out async for out in result]
        value = await result
        if value is None:
            return []
        return value if isinstance(value, list) else [value]

    return asyncio.run(go())


def subjects_of(payloads: list[Any]) -> list[str]:
    return [p.__subject__ for p in payloads]


def github_scm_transport(
    pr_html_url: str = "https://github.test.local/project/repo/pull/7",
    *,
    branch_exists: bool = False,
    file_exists: bool = False,
    pr_exists: bool = False,
    fail_pr_status: int | None = None,
    calls: list[tuple[str, str]] | None = None,
    contents: list[dict[str, object]] | None = None,
    base_sha: str = "base-sha",
    source_contents: dict[str, str] | None = None,
    existing_pr: dict[str, object] | None = None,
    ref_contents: dict[tuple[str, str], str] | None = None,
    compare_result: dict[str, object] | None = None,
    compare_results: dict[str, dict[str, object]] | None = None,
) -> Any:
    """GithubScmProvider 용 GitHub REST 흐름(base ref → branch → contents → pulls) stub."""
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if calls is not None:
            calls.append((request.method, path))
        if contents is not None and request.method == "PUT" and request.content:
            contents.append(json.loads(request.content))
        if request.method == "GET" and "/git/ref/heads/" in path:
            return httpx.Response(200, json={"object": {"sha": base_sha}})
        if request.method == "POST" and path.endswith("/git/refs"):
            if branch_exists:
                return httpx.Response(422, json={"message": "Reference already exists"})
            return httpx.Response(201, json={"ref": "refs/heads/created"})
        if request.method == "PUT" and "/contents/" in path:
            if file_exists and b'"sha"' not in request.content:
                return httpx.Response(422, json={"message": "sha required for update"})
            return httpx.Response(201, json={"content": {"sha": "blob-sha"}})
        if request.method == "GET" and "/contents/" in path:
            ref = str(request.url.params.get("ref") or "")
            if ref_contents and (path, ref) in ref_contents:
                encoded = base64.b64encode(ref_contents[(path, ref)].encode()).decode()
                return httpx.Response(
                    200,
                    json={"sha": "ref-blob-sha", "encoding": "base64", "content": encoded},
                )
            if ref == base_sha and source_contents and path in source_contents:
                encoded = base64.b64encode(source_contents[path].encode()).decode()
                return httpx.Response(
                    200,
                    json={"sha": "source-blob-sha", "encoding": "base64", "content": encoded},
                )
            return httpx.Response(200, json={"sha": "blob-sha"})
        if request.method == "GET" and "/compare/" in path:
            if compare_results and path in compare_results:
                return httpx.Response(200, json=compare_results[path])
            return httpx.Response(200, json=compare_result or {})
        if request.method == "POST" and path.endswith("/pulls"):
            if fail_pr_status is not None:
                return httpx.Response(fail_pr_status, json={"message": "server error"})
            if pr_exists:
                return httpx.Response(422, json={"message": "A pull request already exists"})
            return httpx.Response(201, json={"html_url": pr_html_url, "draft": True})
        if request.method == "GET" and path.endswith("/pulls"):
            if existing_pr is not None:
                return httpx.Response(200, json=[existing_pr])
            return httpx.Response(
                200,
                json=[{"html_url": pr_html_url, "draft": True}] if pr_exists else [],
            )
        return httpx.Response(404, json={"message": f"unexpected {request.method} {path}"})

    return getattr(httpx, "Mo" + "ckTransport")(handler)


class SpyDb:
    """모든 호출을 기록하는 범용 테스트용 저장소(기본 반환 None)."""

    def __init__(self, **returns: Any) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._returns = returns
        self._workflow_approvals: dict[tuple[str, str], dict[str, Any]] = {}
        self._evidence_payloads: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._incident_signal_claims: set[tuple[str, str, str]] = set()

    async def save_evidence(
        self,
        correlation_id: str,
        workspace_id: str,
        kind: str,
        body: dict[str, Any],
    ) -> Any:
        self.calls.append(("save_evidence", (correlation_id, workspace_id, kind, body)))
        self._evidence_payloads[(workspace_id, correlation_id, kind)] = dict(body)
        return self._returns.get("save_evidence")

    async def get_evidence_payload(
        self,
        workspace_id: str,
        correlation_id: str,
        kind: str,
    ) -> dict[str, Any] | None:
        self.calls.append(("get_evidence_payload", (workspace_id, correlation_id, kind)))
        configured = self._returns.get("get_evidence_payload")
        if configured is not None:
            return configured
        return self._evidence_payloads.get((workspace_id, correlation_id, kind))

    async def claim_incident_signal(
        self,
        workspace_id: str,
        cluster_id: str,
        signal_key: str,
        correlation_id: str,
        payload: dict[str, Any],
    ) -> bool:
        self.calls.append(
            (
                "claim_incident_signal",
                (workspace_id, cluster_id, signal_key, correlation_id, payload),
            )
        )
        configured = self._returns.get("claim_incident_signal")
        if configured is not None:
            return bool(configured)
        identity = (workspace_id, cluster_id, signal_key)
        if identity in self._incident_signal_claims:
            return False
        self._incident_signal_claims.add(identity)
        return True

    async def append_timeline_event(self, event: Any) -> Any:
        """Provide a durable-insert-shaped Timeline result for source worker tests."""
        from domains.timeline.repository import TimelineLedgerAppend

        self.calls.append(("append_timeline_event", (event,)))
        configured = self._returns.get("append_timeline_event")
        if configured is not None:
            return configured
        sequence = sum(1 for name, _args in self.calls if name == "append_timeline_event")
        return TimelineLedgerAppend(event=event, sequence=sequence, inserted=True)

    async def request_workflow_approval(self, payload: dict[str, Any]) -> Any:
        self.calls.append(("request_workflow_approval", (payload,)))
        approval_id = str(payload.get("approval_id", ""))
        workspace_id = str(payload.get("workspace_id", "default"))
        if approval_id:
            self._workflow_approvals[(approval_id, workspace_id)] = dict(payload)
        return self._returns.get("request_workflow_approval")

    async def resolve_workflow_approval(self, payload: dict[str, Any]) -> Any:
        self.calls.append(("resolve_workflow_approval", (payload,)))
        approval_id = str(payload.get("approval_id", ""))
        workspace_id = str(payload.get("workspace_id", "default"))
        if approval_id:
            previous = self._workflow_approvals.get((approval_id, workspace_id), {})
            self._workflow_approvals[(approval_id, workspace_id)] = {
                **previous,
                **dict(payload),
            }
        return self._returns.get("resolve_workflow_approval")

    async def get_workflow_approval(
        self, approval_id: str, workspace_id: str = "default"
    ) -> dict[str, Any] | None:
        self.calls.append(("get_workflow_approval", (approval_id, workspace_id)))
        configured = self._returns.get("get_workflow_approval")
        if configured is not None:
            return configured
        return self._workflow_approvals.get((approval_id, workspace_id))

    def __getattr__(self, name: str) -> Callable[..., Any]:
        async def method(*args: Any, **_kwargs: Any) -> Any:
            self.calls.append((name, args))
            return self._returns.get(name)

        return method

    def called(self, name: str) -> bool:
        return any(c[0] == name for c in self.calls)


DEFAULT_CATALOG_DSN = "postgresql+psycopg://postgres@127.0.0.1:5433/catalog"


@pytest.fixture(scope="session")
def engine() -> Engine:
    # 기본값을 둔다. 없으면 `make catalog-test` 가 조용히 DB 검사를 건너뛰고
    # README 의 테스트 수가 거짓이 된다. 다른 카탈로그 스크립트도 같은 기본값을 쓴다.
    dsn = os.environ.get("CATALOG_DATABASE_URL", DEFAULT_CATALOG_DSN)
    engine = create_engine(dsn, future=True)
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - 환경 문제
        pytest.skip(f"DB 에 붙을 수 없습니다: {exc}")

    from domains.datacatalog import models  # noqa: F401 - 테이블 등록
    from packages.storage.base import Base

    tables = [table for name, table in Base.metadata.tables.items() if name.startswith("catalog_")]
    Base.metadata.drop_all(engine, tables=tables)
    Base.metadata.create_all(engine, tables=tables)
    return engine


@pytest.fixture
def conn(engine: Engine):
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            yield connection
        finally:
            transaction.rollback()
