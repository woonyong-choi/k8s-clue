"""사건 이력의 실제 Postgres 검증: 원자성, 재수신 중복, UID 구분.

INCIDENT_HISTORY_TEST_DATABASE_URL이 없으면 건너뛴다. 합성 데이터만 쓰며 대상 DB의
스키마를 직접 만든다. 격리된 DB에서만 실행한다.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi import Request
from sqlalchemy import text

DB_URL = os.environ.get("INCIDENT_HISTORY_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not DB_URL, reason="INCIDENT_HISTORY_TEST_DATABASE_URL 미설정")

BASE = datetime(2026, 10, 15, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def db() -> Iterator[Any]:
    os.environ["DATABASE_URL"] = DB_URL
    from domains.registry import Database

    database = Database()
    database.init()
    try:
        yield database
    finally:
        database.dispose()  # close pooled connections instead of leaving them to the collector


@pytest.fixture
def scope() -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    return f"ws-{suffix}", f"cluster-{suffix}"


def _payload(minutes: float, pods: list[tuple[str, str]], *, complete: bool = True) -> dict:
    return {
        "collected_at": (BASE + timedelta(minutes=minutes)).isoformat(),
        "replace": complete,
        "resources": [
            {
                "resource_type": "pod",
                "api_version": "v1",
                "kind": "Pod",
                "namespace": "default",
                "name": name,
                "uid": uid,
                "status": "Running",
            }
            for name, uid in pods
        ],
        "summary": {"resources_complete": complete, "labels_complete": complete},
    }


def _count(db: Any, sql: str, **params: object) -> int:
    with db.connection() as conn:
        return int(conn.execute(text(sql), params).scalar_one())


def _ledger_count(db: Any, workspace_id: str) -> int:
    return _count(
        db,
        "select count(*) from timeline_events where workspace_id = :w",
        w=workspace_id,
    )


def _snapshot_count(db: Any, workspace_id: str) -> int:
    return _count(
        db,
        "select count(*) from cluster_inventory_snapshots where workspace_id = :w",
        w=workspace_id,
    )


def _save(db: Any, scope: tuple[str, str], payload: dict) -> dict:
    return db.save_inventory_snapshot_with_timeline(
        workspace_id=scope[0],
        cluster_id=scope[1],
        agent_id="agent-1",
        payload=payload,
        append_timeline_events=db.append_timeline_events,
    )


def test_ledger_failure_rolls_back_the_snapshot(db: Any, scope: tuple[str, str]) -> None:
    def failing_append(events: Any) -> None:
        raise RuntimeError("ledger unavailable")

    with pytest.raises(RuntimeError, match="ledger unavailable"):
        db.save_inventory_snapshot_with_timeline(
            workspace_id=scope[0],
            cluster_id=scope[1],
            agent_id="agent-1",
            payload=_payload(1, [("api-0", "uid-1")]),
            append_timeline_events=failing_append,
        )

    assert _snapshot_count(db, scope[0]) == 0
    assert _ledger_count(db, scope[0]) == 0


def test_snapshot_and_ledger_commit_together(db: Any, scope: tuple[str, str]) -> None:
    _save(db, scope, _payload(1, [("api-0", "uid-1")]))

    assert _snapshot_count(db, scope[0]) == 1
    assert _ledger_count(db, scope[0]) == 1


def test_resent_identical_cut_adds_no_ledger_fact(db: Any, scope: tuple[str, str]) -> None:
    _save(db, scope, _payload(1, [("api-0", "uid-1")]))
    _save(db, scope, _payload(2, [("api-0", "uid-1")]))

    assert _ledger_count(db, scope[0]) == 1


def test_same_ledger_event_appended_twice_is_absorbed(db: Any, scope: tuple[str, str]) -> None:
    _save(db, scope, _payload(1, [("api-0", "uid-1")]))
    with db.connection() as conn:
        row = conn.execute(
            text("select source_key from timeline_events where workspace_id = :w"),
            {"w": scope[0]},
        ).one()
    assert row.source_key


def test_recreated_pod_with_new_uid_keeps_both_identities(db: Any, scope: tuple[str, str]) -> None:
    _save(db, scope, _payload(1, [("api-0", "uid-old")]))
    _save(db, scope, _payload(6, [("api-0", "uid-new")]))

    with db.connection() as conn:
        rows = conn.execute(
            text(
                "select event_type, resource->>'uid' as uid "
                "from timeline_events where workspace_id = :w order by sequence"
            ),
            {"w": scope[0]},
        ).all()
    uids = {row.uid for row in rows}
    assert uids == {"uid-old", "uid-new"}, rows


def test_recreated_pod_projects_the_old_uid_as_deleted(db: Any, scope: tuple[str, str]) -> None:
    from domains.rca.history import project_resources
    from domains.timeline.predicate import TimelineEvidencePredicate
    from domains.timeline.repository import TimelineLedgerReadScope
    from packages.contracts.parity import ClusterScope
    from packages.contracts.timeline import TimelineQuery, TimelineWindow

    _save(db, scope, _payload(1, [("api-0", "uid-old")]))
    _save(db, scope, _payload(6, [("api-0", "uid-new")]))
    cluster = ClusterScope(workspace_id=scope[0], cluster_id=scope[1])
    read_scope = TimelineLedgerReadScope(
        workspace_id=scope[0],
        scopes=(cluster,),
        inventory_cluster_ids=frozenset({scope[1]}),
    )
    window = TimelineWindow(
        from_ms=int(BASE.timestamp() * 1_000) - 3_600_000,
        to_ms=int(BASE.timestamp() * 1_000) + 3_600_000,
    )
    query = TimelineQuery(scopes=(cluster,), window=window, mode="frozen")
    snapshot = db.snapshot_timeline_events(
        read_scope, predicate=TimelineEvidencePredicate.from_query(read_scope, query), limit=50
    )

    resources = {
        item.subject.resource.uid: item.lifecycle
        for item in project_resources(snapshot.events, coverage=())
    }
    assert resources == {"uid-old": "deleted", "uid-new": "present"}


def _evidence_call(db: Any, scope: tuple[str, str], key: str, events: tuple = ()) -> dict:
    from packages.events.envelope import event

    envelope = event(
        "evidence.built", "api-gateway", {"k": key}, f"corr-{key}", workspace_id=scope[0]
    )
    return db.record_evidence_event_once(
        evidence_key=key,
        workspace_id=scope[0],
        cluster_id=scope[1],
        source_id="agent",
        window_start="w-1",
        agent_id="agent-1",
        event_envelope=envelope,
        payload={"k": key},
        timeline_events=events,
    )


def _evidence_rows(db: Any, key: str) -> tuple[int, int]:
    windows = _count(db, "select count(*) from evidence_windows where evidence_key = :k", k=key)
    outbox = _count(db, "select count(*) from outbox where payload->>'k' = :k", k=key)
    return windows, outbox


def _incident_event(scope: tuple[str, str], tag: str) -> Any:
    from domains.rca.events import IncidentRecord
    from domains.rca.timeline import incident_timeline_event

    incident = IncidentRecord(
        incident_id=f"inc-{tag}",
        workspace_id=scope[0],
        cluster_id=scope[1],
        resource_kind="Pod",
        resource_name="api-0",
        namespace="default",
        symptom="CrashLoopBackOff",
        severity="critical",
        first_seen_at=BASE.isoformat(),
        summary="synthetic",
    )
    return incident_timeline_event(
        source_event_id=f"evt-{tag}",
        source_created_at=BASE.isoformat(),
        correlation_id=f"corr-{tag}",
        incident=incident,
    )


def test_evidence_window_outbox_and_ledger_commit_together(db: Any, scope: tuple[str, str]) -> None:
    key = f"k-{uuid.uuid4().hex[:8]}"

    result = _evidence_call(db, scope, key, (_incident_event(scope, key),))

    assert result["duplicate"] is False
    assert _evidence_rows(db, key) == (1, 1)
    assert _ledger_count(db, scope[0]) == 1


def test_ledger_failure_rolls_back_window_and_outbox(
    db: Any, scope: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    key = f"k-{uuid.uuid4().hex[:8]}"

    def broken(events: Any) -> None:
        raise RuntimeError("ledger unavailable")

    monkeypatch.setattr(db, "append_timeline_events", broken)
    with pytest.raises(RuntimeError, match="ledger unavailable"):
        _evidence_call(db, scope, key, (_incident_event(scope, key),))

    assert _evidence_rows(db, key) == (0, 0)
    assert _ledger_count(db, scope[0]) == 0


def test_resent_evidence_key_does_not_append_again(db: Any, scope: tuple[str, str]) -> None:
    key = f"k-{uuid.uuid4().hex[:8]}"
    first = _evidence_call(db, scope, key, (_incident_event(scope, key),))

    second = _evidence_call(db, scope, key, (_incident_event(scope, key + "-again"),))

    assert second["duplicate"] is True
    assert second["event_id"] == first["event_id"]
    assert _evidence_rows(db, key) == (1, 1)
    assert _ledger_count(db, scope[0]) == 1


def test_ledger_row_written_before_a_later_failure_is_rolled_back(
    db: Any, scope: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    key = f"k-{uuid.uuid4().hex[:8]}"
    real_append = db.append_timeline_events

    def append_then_fail(events: Any) -> None:
        real_append(events)
        raise RuntimeError("failure after the ledger insert")

    monkeypatch.setattr(db, "append_timeline_events", append_then_fail)
    with pytest.raises(RuntimeError, match="after the ledger insert"):
        _evidence_call(db, scope, key, (_incident_event(scope, key),))

    assert _ledger_count(db, scope[0]) == 0
    assert _evidence_rows(db, key) == (0, 0)


# --- 격리 합성 데이터 API 서버 ---------------------------------------------------
# 실행: INCIDENT_HISTORY_TEST_DATABASE_URL=... uv run python -c \
#   "import sys; sys.path[:0]=['src','tests']; import test_incident_history_db as t; t.serve_synthetic(8765)"
# 실제 Postgres와 실제 history 라우터를 쓰고, 세션과 권한 판정만 합성이다.
# X-Synthetic-Grants 헤더(쉼표 구분 permission)로 권한을 고르며 생략하면 모두 허용한다.

SYNTHETIC_WORKSPACE = "synthetic-ws"
SYNTHETIC_CLUSTER = "synthetic-cluster"
SYNTHETIC_CASES = (
    "syn-complete",
    "syn-gap",
    "syn-nodata",
    "syn-complete-empty",
    "syn-truncated",
    "syn-source-unavailable",
    "syn-history-fail",
    "syn-history-404",
)
# Cases whose history is answered by an injected fault while the bundle stays 200.
SYNTHETIC_INJECTED_HISTORY_FAULTS = {"syn-history-fail": 503, "syn-history-404": 404}
# Grants fixed per case so a browser, which cannot send headers, still sees them.
SYNTHETIC_CASE_GRANTS = {"syn-source-unavailable": "rca.read,inventory.read"}
SYNTHETIC_EMPTY_CLUSTER = "synthetic-empty-cluster"
SYNTHETIC_TRUNCATED_CLUSTER = "synthetic-truncated-cluster"
SYNTHETIC_CASE_CLUSTERS = {
    "syn-complete": SYNTHETIC_CLUSTER,
    "syn-gap": "synthetic-gap-cluster",
    "syn-nodata": "synthetic-nodata-cluster",
    "syn-complete-empty": SYNTHETIC_EMPTY_CLUSTER,
    "syn-truncated": SYNTHETIC_TRUNCATED_CLUSTER,
    "syn-source-unavailable": SYNTHETIC_CLUSTER,
    "syn-history-fail": SYNTHETIC_CLUSTER,
    "syn-history-404": SYNTHETIC_CLUSTER,
}


def _event_capture_summary(observed_at: datetime) -> dict:
    return {
        "kubernetes_event_capture": {
            "complete": True,
            "truncated": False,
            "reason": "complete",
            "freshness": {"observed_at": observed_at.isoformat(), "max_age_seconds": 600},
            "coverage": {
                "scope": "all_namespaces",
                "pagination": "continue",
                "gap": False,
                "event_count": 0,
            },
        },
        "kubernetes_event_facts": [],
    }


def _synthetic_cut(
    at: datetime, pods: list[tuple[str, str]], *, complete: bool = True, capture: bool = True
) -> dict:
    payload = {
        "collected_at": at.isoformat(),
        "replace": complete,
        "resources": [
            {
                "resource_type": "pod",
                "api_version": "v1",
                "kind": "Pod",
                "namespace": "default",
                "name": name,
                "uid": uid,
                "status": "Running",
            }
            for name, uid in pods
        ],
        "summary": {"resources_complete": complete, "labels_complete": complete},
    }
    if capture:
        payload["summary"].update(_event_capture_summary(at))
    return payload


def seed_synthetic(db: Any, *, now: datetime | None = None) -> dict[str, datetime]:
    """Idempotent only on an empty database; seeds three incidents in the past hour."""
    anchor = (now or datetime.now(UTC)) - timedelta(minutes=30)
    scope = (SYNTHETIC_WORKSPACE, SYNTHETIC_CLUSTER)
    start = anchor - timedelta(minutes=55)
    # Complete cuts every 5 minutes; api-0 is recreated with a new UID, worker-0 disappears.
    for step in range(17):
        at = start + timedelta(minutes=5 * step)
        pods = [("api-0", "uid-api-old" if step < 8 else "uid-api-new")]
        if step < 6:
            pods.append(("worker-0", "uid-worker"))
        _save(db, scope, _synthetic_cut(at, pods))
    # A collection gap is a separate cluster so each case stays independent.
    gap_scope = (SYNTHETIC_WORKSPACE, "synthetic-gap-cluster")
    _save(db, gap_scope, _synthetic_cut(start, [("api-0", "uid-gap-a")]))
    _save(db, gap_scope, _synthetic_cut(start + timedelta(minutes=50), [("api-0", "uid-gap-b")]))
    # A steady cluster: complete cuts cover the whole window and nothing changes inside it,
    # so the history is complete and empty (the first cut predates the window).
    empty_scope = (SYNTHETIC_WORKSPACE, SYNTHETIC_EMPTY_CLUSTER)
    for step in range(19):
        at = anchor - timedelta(minutes=75) + timedelta(minutes=5 * step)
        _save(db, empty_scope, _synthetic_cut(at, [("steady-0", "uid-steady")]))
    # More new Pods in one cut than the history returns, so the response is truncated.
    flood = [(f"burst-{index}", f"uid-burst-{index}") for index in range(210)]
    _save(
        db,
        (SYNTHETIC_WORKSPACE, SYNTHETIC_TRUNCATED_CLUSTER),
        _synthetic_cut(anchor - timedelta(minutes=20), flood),
    )
    for case, cluster in SYNTHETIC_CASE_CLUSTERS.items():
        db.save_rca_report(
            case,
            SYNTHETIC_WORKSPACE,
            "synthetic_root_cause",
            "none",
            {
                "incident": {
                    "incident_id": f"inc-{case}",
                    "cluster_id": cluster,
                    "first_seen_at": anchor.isoformat(),
                    "resource_kind": "Pod",
                    "resource_name": "api-0",
                    "namespace": "default",
                    "symptom": "synthetic",
                    "severity": "warning",
                }
            },
        )
    from packages.events.envelope import event

    envelope = event(
        "evidence.built",
        "api-gateway",
        {"k": "syn"},
        "syn-complete",
        workspace_id=SYNTHETIC_WORKSPACE,
    )
    db.record_evidence_event_once(
        evidence_key="syn-evidence-1",
        workspace_id=SYNTHETIC_WORKSPACE,
        cluster_id=SYNTHETIC_CLUSTER,
        source_id="synthetic-agent",
        window_start=anchor.isoformat(),
        agent_id="synthetic-agent",
        event_envelope=envelope,
        payload={"k": "syn"},
    )
    return {"anchor": anchor}


SYNTHETIC_TABLES = (
    "rca_reports",
    "recovery_plans",
    "timeline_events",
    "timeline_event_cursors",
    "cluster_inventory_resources",
    "cluster_inventory_snapshots",
    "evidence_windows",
    "outbox",
)


def reset_synthetic(db: Any) -> None:
    """Delete only the synthetic workspace so repeated starts never accumulate or touch others."""
    with db.connection() as conn:
        for table in SYNTHETIC_TABLES:
            conn.execute(
                text(f"delete from {table} where workspace_id = :w"),  # noqa: S608
                {"w": SYNTHETIC_WORKSPACE},
            )


def serve_synthetic(port: int) -> None:
    from types import SimpleNamespace

    import uvicorn
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import JSONResponse

    from domains.rca import history_router
    from domains.rca_bundle import router as rca_bundle_router
    from packages.runtime.dependencies import get_db

    os.environ["DATABASE_URL"] = DB_URL
    from domains.registry import Database

    database = Database()
    database.init()
    reset_synthetic(database)
    seed_synthetic(database)

    def synthetic_access(_db, current, _workspace, _cluster, action, **_):
        if action not in current.grants:
            raise HTTPException(status_code=403, detail="denied")

    # 권한 대역: 실제 인증·권한 체인을 거치지 않고 헤더의 permission 집합으로 판정한다.
    history_router.require_cluster_access = synthetic_access  # type: ignore[assignment]
    rca_bundle_router.require_cluster_access = synthetic_access  # type: ignore[assignment]
    app = FastAPI()

    @app.middleware("http")
    async def strip_api_prefix(request: Request, call_next):
        path = request.scope["path"]
        if path.startswith("/api/"):
            request.scope["path"] = path[4:]
        return await call_next(request)

    @app.middleware("http")
    async def inject_history_fault(request: Request, call_next):
        # 주입된 오류: bundle은 정상이고 이 사례의 history 조회만 실패한다.
        parts = request.scope["path"].strip("/").split("/")
        if parts[:1] == ["api"]:
            parts = parts[1:]
        if len(parts) == 3 and parts[0] == "incidents" and parts[2] == "history":
            status = SYNTHETIC_INJECTED_HISTORY_FAULTS.get(parts[1])
            if status is not None:
                return JSONResponse(
                    status_code=status, content={"detail": "synthetic injected history fault"}
                )
        return await call_next(request)

    async def synthetic_session(request: Request) -> Any:
        case_grants = SYNTHETIC_CASE_GRANTS.get(str(request.path_params.get("correlation_id")))
        default = case_grants or "rca.read,inventory.read,evidence.read"
        raw = request.headers.get("x-synthetic-grants", default)
        return SimpleNamespace(
            workspace_id=SYNTHETIC_WORKSPACE,
            user_id="synthetic-user",
            grants={item.strip() for item in raw.split(",") if item.strip()},
        )

    # 화면은 rca bundle 조회가 성공해야 history를 그리므로 기존 bundle 라우터도 함께 올린다.
    app.include_router(rca_bundle_router.router)
    app.include_router(history_router.router)
    app.dependency_overrides[history_router.require_session] = synthetic_session
    app.dependency_overrides[rca_bundle_router.require_session] = synthetic_session
    app.dependency_overrides[get_db] = lambda: database
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


# --- 실제 게이트웨이(인증·권한 체인 포함) 합성 데이터 서버 -------------------------------
# 실제 ApiGateway를 Postgres와 Redis 세션 저장소, 프로세스 내 이벤트 버스로 올린다.
# 인증은 실제 로그인 세션 쿠키, 에이전트는 실제 토큰 해시 조회를 거친다. 합성 값만 쓴다.
# 실행: INCIDENT_HISTORY_TEST_DATABASE_URL=... INCIDENT_HISTORY_TEST_REDIS_URL=... uv run python -c \
#   "import sys; sys.path[:0]=['src','tests','src/services/gateway/api-gateway']; \
#    import test_incident_history_db as t; t.serve_real_gateway(18766)"

REAL_WORKSPACE = "real-ws"
REAL_CLUSTER = "real-cluster"
REAL_AGENT_TOKEN = "synthetic-agent-token-real-cluster"
REAL_PASSWORD = "Synthetic-Passw0rd!"
REAL_VIEWER = ("real-viewer", "viewer@synthetic.invalid")
REAL_OUTSIDER = ("real-outsider", "outsider@synthetic.invalid")


@dataclass(frozen=True)
class RealCase:
    """One isolated workspace, cluster, agent token and pair of users."""

    workspace: str
    cluster: str
    agent_token: str
    viewer: tuple[str, str]
    outsider: tuple[str, str]


def real_case(suffix: str = "") -> RealCase:
    tag = f"-{suffix}" if suffix else ""
    return RealCase(
        workspace=f"{REAL_WORKSPACE}{tag}",
        cluster=f"{REAL_CLUSTER}{tag}",
        agent_token=f"{REAL_AGENT_TOKEN}{tag}",
        viewer=(f"real-viewer{tag}", f"viewer{tag}@synthetic.invalid"),
        outsider=(f"real-outsider{tag}", f"outsider{tag}@synthetic.invalid"),
    )


def reset_real_gateway(db: Any, case: RealCase) -> None:
    """Remove only this case's rows so a restart never accumulates or touches other cases."""
    with db.connection() as conn:
        for table in SYNTHETIC_TABLES:
            conn.execute(
                text(f"delete from {table} where workspace_id = :w"),  # noqa: S608
                {"w": case.workspace},
            )


def seed_real_gateway(db: Any, case: RealCase | None = None) -> RealCase:
    from domains.identity.dependencies import hash_agent_token
    from packages.contracts.identity import AccessResourceType

    case = case or real_case()
    passwords = __import__("passwords")
    db.register_target_cluster(
        {
            "user_id": f"owner-{case.workspace}",
            "workspace_id": case.workspace,
            "organization_id": case.workspace,
            "cluster_id": case.cluster,
            "agent_token_hash": hash_agent_token(case.agent_token),
        }
    )
    for user_id, email in (case.viewer, case.outsider):
        db.create_user(
            user_id, email, passwords.hash_password(REAL_PASSWORD), user_id, "active", "user"
        )
    db.grant_resource_access(
        {
            "user_id": case.viewer[0],
            "organization_id": case.workspace,
            "resource_type": AccessResourceType.CLUSTER.value,
            "resource_id": case.cluster,
            "role": "observer",
        }
    )
    return case


def serve_real_gateway(port: int, *, reset: bool = True) -> None:
    """``reset=False`` keeps the case's rows, which is how a process restart is verified."""
    import uvicorn

    from domains.registry import Database
    from packages.events.in_memory import InMemoryEventBus

    os.environ["DATABASE_URL"] = DB_URL
    os.environ["REDIS_URL"] = os.environ["INCIDENT_HISTORY_TEST_REDIS_URL"]
    Database().init()
    gateway = __import__("gateway")
    app = gateway.create_app(event_bus=InMemoryEventBus())
    case = real_case()
    if reset:
        reset_real_gateway(app.state.db, case)
    seed_real_gateway(app.state.db, case)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


# --- 기존 agent evidence 재사용 연결: record_evidence_event_once ----------------------


def _kubernetes_payload(
    at: datetime, uid: str, *, pods: bool = True, events: list | None = None
) -> dict:
    body: dict = {
        "cluster": {"cluster_id": "c", "collected_at": at.isoformat()},
        "collection_scopes": [{"namespace": "default", "label_selector": None}],
        "pods": (
            [
                {
                    "uid": uid,
                    "name": "api-0",
                    "namespace": "default",
                    "resource_version": uid,
                    "phase": "Running",
                    "restart_total": 0,
                }
            ]
            if pods
            else []
        ),
    }
    if events is not None:
        body["event_capture"] = {
            "complete": True,
            "truncated": False,
            "reason": "complete",
            "freshness": {"observed_at": at.isoformat(), "max_age_seconds": 600},
            "coverage": {
                "scope": "all_namespaces",
                "pagination": "continue",
                "gap": False,
                "event_count": len(events),
            },
            "events": events,
        }
    return body


def _scoped_key(scope: tuple[str, str], key: str) -> str:
    """Agents scope evidence keys by workspace and cluster; the table key is global."""
    return f"{scope[0]}:{scope[1]}:{key}"


def _ingest(db: Any, scope: tuple[str, str], key: str, kubernetes: dict, **extra: Any) -> dict:
    from domains.rca.history_ingest import evidence_inventory_payload
    from packages.events.envelope import event

    key = _scoped_key(scope, key)

    envelope = event(
        "evidence.built", "api-gateway", {"k": key}, f"corr-{key}", workspace_id=scope[0]
    )
    return db.record_evidence_event_once(
        evidence_key=key,
        workspace_id=scope[0],
        cluster_id=scope[1],
        source_id="agent",
        window_start=key,
        agent_id="agent-1",
        event_envelope=envelope,
        payload={"k": key},
        inventory_payload=evidence_inventory_payload(kubernetes),
        **extra,
    )


def _first_observed_flags(db: Any, workspace_id: str) -> list[bool]:
    with db.connection() as conn:
        rows = conn.execute(
            text(
                "select metadata->>'observation_basis' as basis from timeline_events "
                "where workspace_id = :w and source = 'inventory' order by sequence"
            ),
            {"w": workspace_id},
        ).all()
    return [row.basis == "first_observed" for row in rows]


def _ledger_rows(db: Any, workspace_id: str) -> list[tuple[str, str, str | None]]:
    with db.connection() as conn:
        rows = conn.execute(
            text(
                "select source, event_type, resource->>'uid' as uid "
                "from timeline_events where workspace_id = :w order by sequence"
            ),
            {"w": workspace_id},
        ).all()
    return [(row.source, row.event_type, row.uid) for row in rows]


def test_evidence_ingest_records_uid_change_as_an_update(db: Any, scope: tuple[str, str]) -> None:
    _ingest(db, scope, "e-1", _kubernetes_payload(BASE, "uid-old"))
    # The first scoped cut is kept as an observation, never as a creation.
    assert _ledger_rows(db, scope[0]) == [("inventory", "add", "uid-old")]
    assert _first_observed_flags(db, scope[0]) == [True]

    _ingest(db, scope, "e-2", _kubernetes_payload(BASE + timedelta(minutes=5), "uid-new"))

    assert _ledger_rows(db, scope[0]) == [
        ("inventory", "add", "uid-old"),
        ("inventory", "update", "uid-new"),
    ]
    assert _snapshot_count(db, scope[0]) == 2


def test_resent_evidence_adds_no_snapshot_or_ledger_fact(db: Any, scope: tuple[str, str]) -> None:
    _ingest(db, scope, "e-1", _kubernetes_payload(BASE, "uid-old"))
    _ingest(db, scope, "e-2", _kubernetes_payload(BASE + timedelta(minutes=5), "uid-new"))

    again = _ingest(db, scope, "e-2", _kubernetes_payload(BASE + timedelta(minutes=5), "uid-new"))

    assert again["duplicate"] is True
    assert _snapshot_count(db, scope[0]) == 2
    assert _ledger_rows(db, scope[0]) == [
        ("inventory", "add", "uid-old"),
        ("inventory", "update", "uid-new"),
    ]


def test_scoped_evidence_never_deletes_what_it_does_not_list(
    db: Any, scope: tuple[str, str]
) -> None:
    _ingest(db, scope, "e-1", _kubernetes_payload(BASE, "uid-1"))

    _ingest(db, scope, "e-2", _kubernetes_payload(BASE + timedelta(minutes=5), "uid-1", pods=False))

    assert _ledger_rows(db, scope[0]) == [("inventory", "add", "uid-1")]
    assert (
        _count(
            db,
            "select count(*) from cluster_inventory_resources "
            "where workspace_id = :w and deleted_at is not null",
            w=scope[0],
        )
        == 0
    )


def test_complete_event_capture_becomes_ledger_facts_once(db: Any, scope: tuple[str, str]) -> None:
    fact = {
        "uid": "evt-1",
        "api_version": "v1",
        "namespace": "default",
        "name": "api-0.1",
        "type": "Warning",
        "count": 3,
        "last_occurrence_at": BASE.isoformat(),
    }
    _ingest(db, scope, "e-1", _kubernetes_payload(BASE, "uid-1", events=[fact]))
    _ingest(
        db, scope, "e-2", _kubernetes_payload(BASE + timedelta(minutes=1), "uid-1", events=[fact])
    )

    kinds = [row for row in _ledger_rows(db, scope[0]) if row[0] == "kubernetes_event"]
    assert kinds == [("kubernetes_event", "k8s_event", "evt-1")]


def test_failure_after_the_snapshot_rolls_back_window_snapshot_and_ledger(
    db: Any, scope: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _ingest(db, scope, "e-1", _kubernetes_payload(BASE, "uid-old"))
    real_append = db.append_timeline_events

    def append_then_fail(events: Any) -> Any:
        real_append(events)
        raise RuntimeError("failure after the ledger insert")

    monkeypatch.setattr(db, "append_timeline_events", append_then_fail)
    with pytest.raises(RuntimeError, match="after the ledger insert"):
        _ingest(db, scope, "e-2", _kubernetes_payload(BASE + timedelta(minutes=5), "uid-new"))

    assert _snapshot_count(db, scope[0]) == 1
    assert _ledger_rows(db, scope[0]) == [("inventory", "add", "uid-old")]
    assert _evidence_rows(db, _scoped_key(scope, "e-2")) == (0, 0)


# --- 실제 게이트웨이: 공개 HTTP agent ingest -> DB -> history ---------------------------

REDIS_URL = os.environ.get("INCIDENT_HISTORY_TEST_REDIS_URL", "")


@pytest.mark.skipif(not REDIS_URL, reason="INCIDENT_HISTORY_TEST_REDIS_URL 미설정")
def test_public_http_agent_ingest_to_history_with_real_auth(
    db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys
    from pathlib import Path

    from fastapi.testclient import TestClient

    from packages.events.in_memory import InMemoryEventBus

    sys.path[:0] = [str(Path(__file__).resolve().parents[1] / "src/services/gateway/api-gateway")]
    monkeypatch.setenv("REDIS_URL", REDIS_URL)
    gateway = __import__("gateway")
    suffix = uuid.uuid4().hex[:8]
    case = seed_real_gateway(db, real_case(suffix))
    correlation = f"http-{suffix}"
    agent = {"x-agent-token": case.agent_token}
    now = datetime.now(UTC)

    def evidence(key: str, at: datetime, uid: str) -> dict:
        return {
            "correlation_id": correlation,
            "evidence_key": f"{key}-{suffix}",
            "source_id": "agent",
            "window_start": at.isoformat(),
            "kubernetes": _kubernetes_payload(at, uid),
        }

    def login(client: TestClient, user: tuple[str, str]) -> None:
        response = client.post("/auth/login", json={"email": user[1], "password": REAL_PASSWORD})
        assert response.status_code == 200

    first = evidence("h-1", now - timedelta(minutes=8), "uid-old")
    second = evidence("h-2", now - timedelta(minutes=3), "uid-new")
    with TestClient(gateway.create_app(event_bus=InMemoryEventBus()), base_url="https://t") as app:
        assert app.post("/agent/evidence", json=first, headers=agent).status_code == 200
        assert app.post("/agent/evidence", json=second, headers=agent).status_code == 200
        assert app.post("/agent/evidence", json=second, headers=agent).status_code == 200
        assert (
            app.post("/agent/evidence", json=second, headers={"x-agent-token": "bad"}).status_code
            == 401
        )
        db.save_rca_report(
            correlation,
            case.workspace,
            "synthetic",
            "none",
            {
                "incident": {
                    "incident_id": f"inc-{suffix}",
                    "cluster_id": case.cluster,
                    "first_seen_at": (now - timedelta(minutes=2)).isoformat(),
                    "symptom": "synthetic",
                    "severity": "warning",
                }
            },
        )
        login(app, case.viewer)
        viewer_cookie = app.cookies.get("service_session")
        body = app.get(f"/incidents/{correlation}/history").json()
        assert body["completeness"]["status"] == "partial"
        assert "partial_scope" in body["completeness"]["reasons"]
        assert [(e["event_type"], e["resource"]["uid"]) for e in body["events"]] == [
            ("add", "uid-old"),
            ("update", "uid-new"),
        ]
        # Both identities stay readable: the first observation is kept, the replaced UID is
        # marked gone with an uncertain (upper-bound) time.
        lifecycle = {
            item["subject"]["resource"]["uid"]: (item["lifecycle"], item["deleted_at_bound"])
            for item in body["resources"]
        }
        assert lifecycle == {"uid-old": ("deleted", "gap_bounded"), "uid-new": ("present", None)}
        assert {item["evidence_key"] for item in body["evidence"]} == {
            f"{case.workspace}:{case.cluster}:h-1-{suffix}",
            f"{case.workspace}:{case.cluster}:h-2-{suffix}",
        }
        app.cookies.clear()
        assert app.get(f"/incidents/{correlation}/history").status_code == 401
        login(app, case.outsider)
        assert app.get(f"/incidents/{correlation}/history").status_code == 404

    # 프로세스 재시작: 새 게이트웨이 인스턴스가 같은 DB와 Redis 세션을 이어받는다.
    with TestClient(gateway.create_app(event_bus=InMemoryEventBus()), base_url="https://t") as app:
        # A real agent sends no session cookie; the browser session is attached afterwards.
        assert app.post("/agent/evidence", json=second, headers=agent).status_code == 200
        app.cookies.set("service_session", viewer_cookie)
        restarted = app.get(f"/incidents/{correlation}/history").json()
    assert restarted["events"] == body["events"]
    assert len(restarted["evidence"]) == 2


@pytest.mark.skipif(not REDIS_URL, reason="INCIDENT_HISTORY_TEST_REDIS_URL 미설정")
def test_login_history_and_logout_keep_the_session_key_value_and_ttl(
    db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json
    import sys
    from pathlib import Path

    import redis
    from fastapi.testclient import TestClient

    from packages.events.in_memory import InMemoryEventBus

    sys.path[:0] = [str(Path(__file__).resolve().parents[1] / "src/services/gateway/api-gateway")]
    monkeypatch.setenv("REDIS_URL", REDIS_URL)
    gateway = __import__("gateway")
    settings = __import__("settings").Settings
    suffix = uuid.uuid4().hex[:8]
    case = seed_real_gateway(db, real_case(suffix))
    correlation = f"session-{suffix}"
    db.save_rca_report(
        correlation,
        case.workspace,
        "synthetic",
        "none",
        {
            "incident": {
                "incident_id": f"inc-{suffix}",
                "cluster_id": case.cluster,
                "first_seen_at": datetime.now(UTC).isoformat(),
                "symptom": "synthetic",
                "severity": "warning",
            }
        },
    )
    ttl = gateway.ApiGateway._session_store_config().ttl_seconds
    store = redis.Redis.from_url(REDIS_URL)
    with TestClient(gateway.create_app(event_bus=InMemoryEventBus()), base_url="https://t") as app:
        login = app.post("/auth/login", json={"email": case.viewer[1], "password": REAL_PASSWORD})
        assert login.status_code == 200
        key = f"{settings.SESSION_KEY_PREFIX}:{app.cookies.get('service_session')}"
        stored = json.loads(store.get(key))
        assert stored["user_id"] == case.viewer[0]
        assert stored["workspace_id"] == case.workspace
        assert stored["email"] == case.viewer[1] and stored["auth_mode"] == "password"
        assert ttl - 10 <= store.ttl(key) <= ttl  # the expiry set with the value is preserved
        assert app.get(f"/incidents/{correlation}/history").status_code == 200
        intent = {settings.CSRF_INTENT_HEADER: settings.CSRF_INTENT_VALUE}
        assert app.post("/auth/logout", headers=intent).status_code == 200
        assert store.exists(key) == 0
        assert app.get(f"/incidents/{correlation}/history").status_code == 401
