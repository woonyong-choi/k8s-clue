"""사건 이력의 실제 Postgres 검증: 원자성, 재수신 중복, UID 구분.

INCIDENT_HISTORY_TEST_DATABASE_URL이 없으면 건너뛴다. 합성 데이터만 쓰며 대상 DB의
스키마를 직접 만든다. 격리된 DB에서만 실행한다.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi import Request
from sqlalchemy import text

DB_URL = os.environ.get("INCIDENT_HISTORY_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not DB_URL, reason="INCIDENT_HISTORY_TEST_DATABASE_URL 미설정")

BASE = datetime(2026, 10, 15, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def db() -> Any:
    os.environ["DATABASE_URL"] = DB_URL
    from domains.registry import Database

    database = Database()
    database.init()
    return database


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
SYNTHETIC_CASES = ("syn-complete", "syn-gap", "syn-nodata")


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
    for case, cluster in (
        ("syn-complete", SYNTHETIC_CLUSTER),
        ("syn-gap", "synthetic-gap-cluster"),
        ("syn-nodata", "synthetic-nodata-cluster"),
    ):
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


def serve_synthetic(port: int) -> None:
    from types import SimpleNamespace

    import uvicorn
    from fastapi import FastAPI, HTTPException

    from domains.rca import history_router
    from packages.runtime.dependencies import get_db

    os.environ["DATABASE_URL"] = DB_URL
    from domains.registry import Database

    database = Database()
    database.init()
    if _count(database, "select count(*) from rca_reports") == 0:
        seed_synthetic(database)

    def synthetic_access(_db, current, _workspace, _cluster, action, **_):
        if action not in current.grants:
            raise HTTPException(status_code=403, detail="denied")

    history_router.require_cluster_access = synthetic_access  # type: ignore[assignment]
    app = FastAPI()

    @app.middleware("http")
    async def strip_api_prefix(request: Request, call_next):
        path = request.scope["path"]
        if path.startswith("/api/"):
            request.scope["path"] = path[4:]
        return await call_next(request)

    async def synthetic_session(request: Request) -> Any:
        raw = request.headers.get("x-synthetic-grants", "rca.read,inventory.read,evidence.read")
        return SimpleNamespace(
            workspace_id=SYNTHETIC_WORKSPACE,
            user_id="synthetic-user",
            grants={item.strip() for item in raw.split(",") if item.strip()},
        )

    app.include_router(history_router.router)
    app.dependency_overrides[history_router.require_session] = synthetic_session
    app.dependency_overrides[get_db] = lambda: database
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
