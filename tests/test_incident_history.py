"""사건 이력 backend: UID 구분, 완전성 판정, 권한, 원자 기록 계약.

수집 누락·부분 관측·잘림은 "이력 없음"이나 복구로 읽히면 안 된다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from domains.inventory.repository import InventoryRepository
from domains.inventory.snapshot_evidence import COMPLETE_CUT_PROOF_KEY
from domains.rca import history_router
from domains.rca.history import (
    OBSERVATION_MAX_GAP_MS,
    InventoryObservation,
    assess_completeness,
    incident_history_window,
    observation_from_snapshot_row,
    project_resources,
)
from domains.rca.history_ingest import evidence_inventory_payload
from domains.timeline.mapping import inventory_timeline_event
from packages.contracts.identity import Permission
from packages.contracts.parity import ClusterScope
from packages.contracts.timeline import TimelineCoverage, TimelineEvent, TimelineWindow
from packages.contracts.timeline.history import (
    IncidentHistoryCompleteness,
    IncidentHistoryResponse,
)
from packages.runtime.dependencies import get_db

WORKSPACE = "ws-1"
CLUSTER = "cluster-1"
BASE = datetime(2026, 10, 15, 0, 0, tzinfo=UTC)
BASE_MS = int(BASE.timestamp() * 1_000)
MINUTE_MS = 60_000


def _at(minutes: float) -> datetime:
    return BASE + timedelta(minutes=minutes)


def _event(
    event_type: str,
    minutes: float,
    *,
    uid: str | None,
    name: str = "api-0",
    key: str | None = None,
) -> TimelineEvent:
    source_key = key or f"inventory:{event_type}:{name}:{uid}:{minutes}"
    return inventory_timeline_event(
        event_id=source_key,
        source_key=source_key,
        native_id=f"v1/Pod/default/{name}",
        occurred_at=_at(minutes),
        workspace_id=WORKSPACE,
        cluster_id=CLUSTER,
        api_version="v1",
        resource_kind="Pod",
        namespace="default",
        name=name,
        uid=uid,
        title=f"Pod {name} {event_type}",
        event_type=event_type,  # type: ignore[arg-type]
    )


def _window(from_minutes: float = 0, to_minutes: float = 30) -> TimelineWindow:
    return TimelineWindow(
        from_ms=BASE_MS + int(from_minutes * MINUTE_MS),
        to_ms=BASE_MS + int(to_minutes * MINUTE_MS),
    )


def _complete(minutes: float) -> InventoryObservation:
    return InventoryObservation(BASE_MS + int(minutes * MINUTE_MS), True, True)


def _partial(minutes: float) -> InventoryObservation:
    return InventoryObservation(BASE_MS + int(minutes * MINUTE_MS), False)


def _assess(
    observations: list[InventoryObservation],
    *,
    window: TimelineWindow | None = None,
    capture: tuple[TimelineCoverage, ...] = (),
    truncated: bool = False,
    inventory_authorized: bool = True,
    source_unavailable: bool = False,
) -> IncidentHistoryCompleteness:
    return assess_completeness(
        window=window or _window(),
        now_ms=BASE_MS + 24 * 60 * MINUTE_MS,
        observations=observations,
        capture_coverage=capture,
        scope_cluster=(WORKSPACE, CLUSTER),
        truncated=truncated,
        source_unavailable=source_unavailable,
        inventory_authorized=inventory_authorized,
    )


# --- UID 구분 -----------------------------------------------------------------


def test_same_name_with_different_uid_is_a_separate_resource() -> None:
    events = [
        _event("add", 1, uid="uid-old"),
        _event("delete", 5, uid="uid-old"),
        _event("add", 6, uid="uid-new"),
    ]

    resources = project_resources(events, coverage=())

    by_uid = {item.subject.resource.uid: item for item in resources}  # type: ignore[union-attr]
    assert set(by_uid) == {"uid-old", "uid-new"}
    assert by_uid["uid-old"].lifecycle == "deleted"
    assert by_uid["uid-new"].lifecycle == "present"
    assert by_uid["uid-new"].deleted_at is None


def test_uid_replaced_under_the_same_inventory_key_is_not_left_present() -> None:
    # A recreated Pod keeps its inventory key, so the ledger holds add(old) then update(new).
    events = [_event("add", 1, uid="uid-old"), _event("update", 6, uid="uid-new")]

    resources = {
        item.subject.resource.uid: item  # type: ignore[union-attr]
        for item in project_resources(events, coverage=())
    }

    assert resources["uid-old"].lifecycle == "deleted"
    assert resources["uid-old"].deleted_at == _at(6)
    assert resources["uid-old"].deleted_at_bound == "gap_bounded"
    assert resources["uid-new"].lifecycle == "present"


def test_uid_less_inventory_stays_a_locator_and_is_not_promoted() -> None:
    resources = project_resources([_event("add", 1, uid=None)], coverage=())

    assert len(resources) == 1
    assert resources[0].subject.kind == "inventory_locator"
    assert resources[0].lifecycle == "present"


def test_kubernetes_event_objects_are_not_listed_as_resources() -> None:
    # The Event fact names the Event object itself, never the involved resource.
    event = _event("add", 2, uid="uid-1").model_copy(update={"source": "kubernetes_event"})

    assert project_resources([event], coverage=()) == ()


def test_deletion_overlapping_a_gap_is_gap_bounded() -> None:
    gap = TimelineCoverage(
        scope=ClusterScope(workspace_id=WORKSPACE, cluster_id=CLUSTER),
        source="inventory",
        from_ms=BASE_MS + 2 * MINUTE_MS,
        to_ms=BASE_MS + 9 * MINUTE_MS,
        reason="collection_gap",
    )
    events = [
        _event("add", 1, uid="uid-a", name="a"),
        _event("delete", 9, uid="uid-a", name="a"),
        _event("add", 10, uid="uid-b", name="b"),
        _event("delete", 12, uid="uid-b", name="b"),
    ]

    resources = {
        item.subject.resource.uid: item  # type: ignore[union-attr]
        for item in project_resources(events, coverage=(gap,))
    }

    assert resources["uid-a"].deleted_at_bound == "gap_bounded"
    assert resources["uid-b"].deleted_at_bound == "observed"


# --- 완전성 -------------------------------------------------------------------


def test_no_observation_is_unavailable_never_complete() -> None:
    result = _assess([])

    assert result.status == "unavailable"
    assert result.reasons == ("no_observation",)


def test_observations_outside_the_window_do_not_prove_it() -> None:
    result = _assess([_complete(-120)], window=_window(0, 30))

    assert result.status == "unavailable"


def test_complete_cuts_across_the_window_are_complete() -> None:
    result = _assess([_complete(1), _complete(6), _complete(12), _complete(18), _complete(25)])

    assert result.status == "complete"
    assert result.reasons == ()
    assert result.coverage == ()


def test_missing_cut_proof_is_partial_not_complete() -> None:
    result = _assess([_complete(1), _partial(6), _complete(12), _complete(18), _complete(25)])

    assert result.status == "partial"
    assert "partial_scope" in result.reasons
    assert any(item.reason == "partial_scope" for item in result.coverage)


def test_observation_gap_is_reported_and_never_filled() -> None:
    result = _assess([_complete(1), _complete(25)])

    assert result.status == "partial"
    assert "collection_gap" in result.reasons
    gap = next(item for item in result.coverage if item.reason == "collection_gap")
    assert gap.from_ms == BASE_MS + MINUTE_MS
    assert gap.to_ms == BASE_MS + 25 * MINUTE_MS
    assert gap.to_ms - gap.from_ms > OBSERVATION_MAX_GAP_MS


def test_window_edges_without_observation_are_gaps() -> None:
    result = _assess([_complete(12)], window=_window(0, 30))

    assert result.status == "partial"
    assert len([item for item in result.coverage if item.reason == "collection_gap"]) == 2


def test_truncation_downgrades_complete_history() -> None:
    cuts = [_complete(1), _complete(6), _complete(12), _complete(18), _complete(25)]

    result = _assess(cuts, truncated=True)

    assert result.status == "partial"
    assert result.reasons == ("truncated",)


def test_unauthorized_inventory_is_partial_with_a_generic_reason() -> None:
    result = _assess([], inventory_authorized=False, source_unavailable=True)

    assert result.status == "partial"
    assert result.reasons == ("source_unavailable",)


def test_snapshot_rows_without_proof_or_stale_are_never_complete() -> None:
    proven = {"collected_at": BASE, "status": "accepted", "summary": {}}
    proven["summary"] = {COMPLETE_CUT_PROOF_KEY: {"resources_complete": True}}
    legacy = {"collected_at": BASE, "status": "accepted", "summary": {"summary": {}}}
    stale = {"collected_at": BASE, "status": "ignored_stale", "summary": proven["summary"]}

    assert observation_from_snapshot_row(proven).resources_complete is True  # type: ignore[union-attr]
    assert observation_from_snapshot_row(legacy).resources_complete is False  # type: ignore[union-attr]
    assert observation_from_snapshot_row(stale) is None


def test_default_window_brackets_the_incident() -> None:
    window = incident_history_window(anchor=BASE, from_ms=None, to_ms=None)

    assert window.from_ms == BASE_MS - 3_600_000
    assert window.to_ms == BASE_MS + 900_000


# --- 응답 계약 ----------------------------------------------------------------


def test_contract_rejects_complete_history_with_reasons() -> None:
    with pytest.raises(ValidationError):
        IncidentHistoryCompleteness(status="complete", reasons=("collection_gap",))


def test_contract_rejects_no_observation_outside_unavailable() -> None:
    with pytest.raises(ValidationError):
        IncidentHistoryCompleteness(status="partial", reasons=("no_observation",))


def test_contract_rejects_truncated_response_without_reason() -> None:
    with pytest.raises(ValidationError):
        IncidentHistoryResponse(
            correlation_id="c-1",
            cluster_id=CLUSTER,
            window=_window(),
            completeness=IncidentHistoryCompleteness(status="complete"),
            truncated=True,
        )


def test_contract_rejects_events_in_unavailable_history() -> None:
    with pytest.raises(ValidationError):
        IncidentHistoryResponse(
            correlation_id="c-1",
            cluster_id=CLUSTER,
            window=_window(),
            events=(_event("add", 1, uid="uid-1"),),
            completeness=IncidentHistoryCompleteness(
                status="unavailable", reasons=("no_observation",)
            ),
        )


# --- 라우터: 권한과 판정 ---------------------------------------------------------


@dataclass
class _Snapshot:
    events: tuple[TimelineEvent, ...]
    truncated: bool = False


class _FakeDb:
    def __init__(
        self,
        *,
        events: tuple[TimelineEvent, ...] = (),
        observations: list[dict[str, Any]] | None = None,
        truncated: bool = False,
        report: dict[str, Any] | None = None,
    ) -> None:
        self.events = events
        self.observations = observations or []
        self.truncated = truncated
        self.report = report
        self.read_scope: Any = None
        self.deployment_plan: dict[str, Any] | None = None
        self.git_source: dict[str, Any] | None = None
        self.argo_observations: list[dict[str, Any]] = []
        self.image_observations: list[dict[str, Any]] = []

    def get_recovery_plan_by_correlation(self, *_: Any) -> dict[str, Any] | None:
        return self.deployment_plan

    def get_git_source_observation(self, *_: Any) -> dict[str, Any] | None:
        return self.git_source

    def list_argo_application_observations(self, *_: Any) -> list[Any]:
        return self.argo_observations

    def list_image_pull_observations(self, *_: Any) -> list[Any]:
        return self.image_observations

    def list_rca_report_records(self, workspace_id: str, **_: Any) -> list[dict[str, Any]]:
        return [self.report] if self.report else []

    def snapshot_timeline_events(self, read_scope: Any, *, predicate: Any, limit: int) -> _Snapshot:
        self.read_scope = read_scope
        kept = tuple(event for event in self.events if predicate.matches_snapshot(event))
        return _Snapshot(events=kept, truncated=self.truncated)

    def snapshot_timeline_coverage(self, read_scope: Any, *, window: Any) -> tuple[()]:
        return ()

    def list_inventory_snapshot_observations(self, **_: Any) -> list[dict[str, Any]]:
        return self.observations

    def list_evidence_windows_by_correlation(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
        return [
            {
                "evidence_key": "k-1",
                "source_id": "agent",
                "window_start": "w-1",
                "event_id": "e-1",
                "updated_at": BASE,
            }
        ]


def _report() -> dict[str, Any]:
    return {
        "workspace_id": WORKSPACE,
        "correlation_id": "c-1",
        "cluster_id": CLUSTER,
        "first_seen_at": BASE.isoformat(),
        "created_at": BASE,
    }


def _proven(minutes: float) -> dict[str, Any]:
    return {
        "collected_at": _at(minutes),
        "status": "accepted",
        "summary": {COMPLETE_CUT_PROOF_KEY: {"resources_complete": True}},
        "event_capture": {"complete": True, "truncated": False, "reason": "complete"},
    }


def _client(monkeypatch: pytest.MonkeyPatch, db: _FakeDb, granted: set[Permission]) -> TestClient:
    def fake_access(
        _db: Any, _current: Any, _workspace: str, _cluster: str, action: str, **__: Any
    ) -> None:
        if action not in {permission.value for permission in granted}:
            raise HTTPException(status_code=403, detail="denied")

    monkeypatch.setattr(history_router, "require_cluster_access", fake_access)
    monkeypatch.setattr(history_router, "_now_ms", lambda: BASE_MS + 24 * 60 * MINUTE_MS)
    app = FastAPI()
    app.include_router(history_router.router)
    app.dependency_overrides[history_router.require_session] = lambda: SimpleNamespace(
        workspace_id=WORKSPACE, user_id="user-1"
    )
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


ALL = {Permission.RCA_READ, Permission.INVENTORY_READ, Permission.EVIDENCE_READ}


def _window_query() -> str:
    return f"from_ms={BASE_MS}&to_ms={BASE_MS + 30 * MINUTE_MS}"


def test_unknown_incident_and_denied_cluster_are_indistinguishable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = _client(monkeypatch, _FakeDb(report=None), ALL)
    denied = _client(monkeypatch, _FakeDb(report=_report()), {Permission.INVENTORY_READ})

    first = missing.get("/incidents/c-1/history")
    second = denied.get("/incidents/c-1/history")

    assert first.status_code == second.status_code == 404
    assert first.json() == second.json()


def test_complete_history_with_no_events_is_reported_as_observed_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observations = [_proven(m) for m in (1, 6, 12, 18, 25)]
    db = _FakeDb(report=_report(), observations=observations)

    body = _client(monkeypatch, db, ALL).get(f"/incidents/c-1/history?{_window_query()}").json()

    assert body["completeness"] == {"status": "complete", "coverage": [], "reasons": []}
    assert body["events"] == []
    assert body["evidence"][0]["event_id"] == "e-1"


def test_events_without_any_observation_are_kept_but_never_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _FakeDb(report=_report(), events=(_event("add", 1, uid="uid-1"),))

    body = _client(monkeypatch, db, ALL).get(f"/incidents/c-1/history?{_window_query()}").json()

    assert body["completeness"]["status"] == "partial"
    assert "collection_gap" in body["completeness"]["reasons"]
    assert len(body["events"]) == 1


def test_no_events_and_no_observation_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _FakeDb(report=_report())

    body = _client(monkeypatch, db, ALL).get(f"/incidents/c-1/history?{_window_query()}").json()

    assert body["completeness"]["status"] == "unavailable"
    assert body["events"] == []


def test_evidence_references_are_hidden_without_evidence_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _FakeDb(report=_report(), observations=[_proven(m) for m in (1, 6, 12, 18, 25)])
    client = _client(monkeypatch, db, {Permission.RCA_READ, Permission.INVENTORY_READ})

    response = client.get(f"/incidents/c-1/history?{_window_query()}")

    assert response.json()["evidence"] == []
    assert "k-1" not in response.text and "e-1" not in response.text
    assert response.json()["completeness"]["reasons"] == ["source_unavailable"]


def test_incident_history_tracks_merge_apply_and_two_pod_recovery_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _FakeDb(report=_report())
    now = datetime.now(UTC)
    sha = "a" * 40
    db.deployment_plan = {
        "payload": {
            "target": {"cluster_id": CLUSTER},
            "lifecycle": {
                "merge": {
                    "binding_id": "binding-1",
                    "merge_commit_sha": sha,
                    "merged_at": (now - timedelta(minutes=2)).isoformat(),
                    "deployment_request": {"manifest_path": "deploy/game.yaml"},
                }
            },
        }
    }
    db.git_source = {
        "status": "observed",
        "image": "game:v2",
        "cluster_id": CLUSTER,
        "observed_at": now,
    }
    client = _client(monkeypatch, db, ALL | {Permission.APPLICATION_READ})

    def state() -> str:
        response = client.get(f"/incidents/c-1/history?{_window_query()}")
        assert response.status_code == 200
        return response.json()["deployment"]["status"]

    assert state() == "merged"
    db.argo_observations = [{"expected_sha": sha, "status": "pending", "last_checked_at": now}]
    assert state() == "deploy_pending"
    db.argo_observations = [
        {
            "expected_sha": sha,
            "status": "reported_applied",
            "sync_revision": sha,
            "last_checked_at": now,
            "reconciled_at": now - timedelta(seconds=45),
        }
    ]
    assert state() == "unknown"
    image = {
        "expected_sha": sha,
        "expected_image": "game:v2",
        "evidence_key": "window-1",
        "observed_at": now - timedelta(seconds=30),
        "status": "failing",
        "reason": "image_pull_failure_persists",
        "ready_pod_count": 0,
        "consecutive_healthy_windows": 0,
        "confirmed_recovered": False,
    }
    db.image_observations = [image]
    assert state() == "deployed_symptom_persists"
    first_healthy = {
        **image,
        "evidence_key": "window-2",
        "observed_at": now - timedelta(seconds=20),
        "status": "healthy",
        "consecutive_healthy_windows": 1,
    }
    db.image_observations = [first_healthy, image]
    assert state() == "verification_pending"
    db.image_observations = [
        {
            **first_healthy,
            "evidence_key": "window-3",
            "observed_at": now - timedelta(seconds=10),
            "consecutive_healthy_windows": 2,
            "confirmed_recovered": True,
        },
        first_healthy,
        image,
    ]
    assert state() == "recovered"
    db.argo_observations[0]["reconciled_at"] = now - timedelta(seconds=15)
    assert state() == "verification_pending"
    db.argo_observations[0]["reconciled_at"] = now - timedelta(seconds=45)

    hidden = _client(monkeypatch, db, ALL).get(f"/incidents/c-1/history?{_window_query()}")
    assert hidden.json()["deployment"] is None


def test_future_window_is_clamped_and_not_called_complete_for_unobserved_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _FakeDb(report=_report(), observations=[_proven(m) for m in (1, 6, 12, 18, 25)])
    client = _client(monkeypatch, db, ALL)
    now_ms = BASE_MS + 30 * MINUTE_MS
    monkeypatch.setattr(history_router, "_now_ms", lambda: now_ms)

    body = client.get(f"/incidents/c-1/history?from_ms={BASE_MS}&to_ms={now_ms + 3_600_000}").json()

    assert body["window"]["to_ms"] == now_ms
    assert body["completeness"]["status"] == "complete"


def test_future_only_window_is_never_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _FakeDb(report=_report(), observations=[_proven(m) for m in (1, 6, 12, 18, 25)])
    client = _client(monkeypatch, db, ALL)
    monkeypatch.setattr(history_router, "_now_ms", lambda: BASE_MS)

    body = client.get(
        f"/incidents/c-1/history?from_ms={BASE_MS + 600_000}&to_ms={BASE_MS + 1_200_000}"
    ).json()

    assert body["completeness"]["status"] == "unavailable"


def test_inventory_cuts_do_not_vouch_for_kubernetes_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [_proven(m) for m in (1, 6, 12, 18, 25)]
    for row in rows:
        row["event_capture"] = None
    db = _FakeDb(report=_report(), observations=rows)

    body = _client(monkeypatch, db, ALL).get(f"/incidents/c-1/history?{_window_query()}").json()

    assert body["completeness"]["status"] == "partial"
    assert "partial_scope" in body["completeness"]["reasons"]
    assert {item["source"] for item in body["completeness"]["coverage"]} == {"kubernetes_event"}


def test_denied_inventory_source_is_omitted_without_naming_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _FakeDb(
        report=_report(),
        events=(_event("add", 1, uid="uid-1"),),
        observations=[_proven(m) for m in (1, 6, 12, 18, 25)],
    )
    client = _client(monkeypatch, db, {Permission.RCA_READ, Permission.EVIDENCE_READ})

    body = client.get(f"/incidents/c-1/history?{_window_query()}").json()

    assert body["events"] == []
    assert body["completeness"]["status"] == "partial"
    assert body["completeness"]["reasons"] == ["source_unavailable"]
    assert db.read_scope.inventory_cluster_ids == frozenset()
    assert db.read_scope.incident_cluster_ids == frozenset({CLUSTER})


def test_truncated_history_is_partial_and_flagged(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _FakeDb(
        report=_report(),
        events=(_event("add", 1, uid="uid-1"),),
        observations=[_proven(m) for m in (1, 6, 12, 18, 25)],
        truncated=True,
    )

    body = _client(monkeypatch, db, ALL).get(f"/incidents/c-1/history?{_window_query()}").json()

    assert body["truncated"] is True
    assert body["completeness"]["status"] == "partial"
    assert "truncated" in body["completeness"]["reasons"]


def test_events_are_ordered_and_split_by_uid(monkeypatch: pytest.MonkeyPatch) -> None:
    events = (
        _event("add", 6, uid="uid-new"),
        _event("delete", 5, uid="uid-old"),
        _event("add", 1, uid="uid-old"),
    )
    db = _FakeDb(
        report=_report(),
        events=events,
        observations=[_proven(m) for m in (1, 6, 12, 18, 25)],
    )

    body = _client(monkeypatch, db, ALL).get(f"/incidents/c-1/history?{_window_query()}").json()

    assert [item["occurred_at"] for item in body["events"]] == sorted(
        item["occurred_at"] for item in body["events"]
    )
    assert body["events"][0]["occurred_at"].endswith("Z")
    assert len(body["resources"]) == 2


def test_wide_or_inverted_windows_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(monkeypatch, _FakeDb(report=_report()), ALL)

    wide = client.get(f"/incidents/c-1/history?from_ms={BASE_MS}&to_ms={BASE_MS + 90_000_000}")
    inverted = client.get(f"/incidents/c-1/history?from_ms={BASE_MS + 5}&to_ms={BASE_MS + 1}")

    assert wide.status_code == inverted.status_code == 422


# --- 원자 기록 ---------------------------------------------------------------


class _UnitOfWorkRecorder:
    """unit_of_work 안에서 호출됐는지와 예외 전파를 기록한다."""

    def __init__(self) -> None:
        self.depth = 0
        self.appended_at_depth: list[int] = []
        self.events = (_event("add", 1, uid="uid-1"),)

    def unit_of_work(self) -> Any:
        recorder = self

        class _Scope:
            def __enter__(self) -> None:
                recorder.depth += 1

            def __exit__(self, *_: object) -> None:
                recorder.depth -= 1

        return _Scope()

    def save_inventory_snapshot_mutation(self, **_: Any) -> Any:
        return SimpleNamespace(result={"accepted": True}, timeline_events=self.events)


def test_snapshot_and_ledger_append_share_one_unit_of_work() -> None:
    recorder = _UnitOfWorkRecorder()

    def append(events: Any) -> None:
        recorder.appended_at_depth.append(recorder.depth)

    result = InventoryRepository.save_inventory_snapshot_with_timeline(
        recorder,  # type: ignore[arg-type]
        workspace_id=WORKSPACE,
        cluster_id=CLUSTER,
        agent_id="agent-1",
        payload={},
        append_timeline_events=append,
    )

    assert result == {"accepted": True}
    assert recorder.appended_at_depth == [1]
    assert recorder.depth == 0


def test_ledger_failure_propagates_out_of_the_unit_of_work() -> None:
    recorder = _UnitOfWorkRecorder()

    def append(events: Any) -> None:
        raise RuntimeError("ledger unavailable")

    with pytest.raises(RuntimeError, match="ledger unavailable"):
        InventoryRepository.save_inventory_snapshot_with_timeline(
            recorder,  # type: ignore[arg-type]
            workspace_id=WORKSPACE,
            cluster_id=CLUSTER,
            agent_id="agent-1",
            payload={},
            append_timeline_events=append,
        )

    assert recorder.depth == 0


# --- agent evidence 재사용 어댑터 ------------------------------------------------


def _kubernetes(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "cluster": {"cluster_id": CLUSTER, "collected_at": BASE.isoformat()},
        "collection_scopes": [{"namespace": "default", "label_selector": None}],
        "pods": [
            {
                "uid": "uid-1",
                "name": "api-0",
                "namespace": "default",
                "resource_version": "7",
                "phase": "Running",
                "restart_total": 2,
                "node_name": "node-a",
            }
        ],
        "workloads": [],
    }
    body.update(overrides)
    return body


def test_adapter_marks_the_scoped_cut_as_incomplete_and_not_live() -> None:
    payload = evidence_inventory_payload(_kubernetes())

    assert payload is not None
    assert payload["replace"] is False
    assert payload["summary"]["live_inventory"] is False
    assert payload["summary"]["resources_complete"] is False
    assert "collection_coverage" not in payload["summary"]


def test_adapter_never_stamps_a_missing_collection_time() -> None:
    assert evidence_inventory_payload(_kubernetes(cluster={"cluster_id": CLUSTER})) is None
    assert evidence_inventory_payload({"pods": []}) is None
    assert evidence_inventory_payload("not-a-mapping") is None


def test_adapter_keeps_named_rows_without_a_uid_and_never_invents_one() -> None:
    pods = [
        {"uid": "", "name": "blank-uid", "namespace": "default", "phase": "Running"},
        {"name": "no-uid", "namespace": "default"},
        {"uid": "  ", "name": "spaces", "namespace": "default"},
        {
            "uid": "uid-2",
            "name": "api-1",
            "namespace": "default",
            "phase": "Failed",
            "node_name": "n",
        },
        {"uid": "uid-3", "namespace": "default"},
    ]

    payload = evidence_inventory_payload(_kubernetes(pods=pods))

    assert payload is not None
    by_name = {item["name"]: item for item in payload["resources"]}
    assert set(by_name) == {"blank-uid", "no-uid", "spaces", "api-1"}
    assert [by_name[name]["uid"] for name in ("blank-uid", "no-uid", "spaces")] == [None] * 3
    assert by_name["api-1"]["uid"] == "uid-2"
    assert by_name["api-1"]["summary"] == {"phase": "Failed"}


def _pod_coverage(namespace: str = "default", **overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "collection": "pods",
        "scope": "namespace",
        "namespace": namespace,
        "observed": True,
        "complete": True,
        "delete_safe": True,
        "truncated": False,
        "label_selector": None,
        "reason_codes": [],
    }
    entry.update(overrides)
    return entry


def test_adapter_forwards_only_pod_namespace_proof() -> None:
    other = {**_pod_coverage(), "collection": "workloads"}
    cluster_scoped = {**_pod_coverage(), "scope": "cluster", "namespace": None}

    payload = evidence_inventory_payload(
        _kubernetes(collection_coverage=[_pod_coverage(), other, cluster_scoped, "junk"])
    )

    assert payload is not None
    assert [entry["collection"] for entry in payload["summary"]["collection_coverage"]] == ["pods"]
    assert payload["summary"]["scoped_coverage_authority"] is True
    assert payload["summary"]["live_inventory"] is False


def test_adapter_drops_all_proof_when_the_pod_list_was_limited() -> None:
    limited = {"truncated": True, "lists": {"pods": {"truncated": True}}}

    payload = evidence_inventory_payload(
        _kubernetes(collection_coverage=[_pod_coverage()], collection_limits=limited)
    )

    assert payload is not None
    assert "collection_coverage" not in payload["summary"]
    assert "scoped_coverage_authority" not in payload["summary"]


def test_adapter_without_producer_proof_grants_no_deletion_authority() -> None:
    payload = evidence_inventory_payload(_kubernetes())

    assert payload is not None
    assert "collection_coverage" not in payload["summary"]
    assert "scoped_coverage_authority" not in payload["summary"]


def test_adapter_records_a_proven_empty_namespace() -> None:
    payload = evidence_inventory_payload(
        _kubernetes(pods=[], collection_coverage=[_pod_coverage()])
    )

    assert payload is not None
    assert payload["resources"] == []
    assert payload["summary"]["collection_coverage"][0]["complete"] is True


def test_adapter_splits_event_capture_into_proof_and_facts() -> None:
    capture = {
        "complete": True,
        "truncated": False,
        "reason": "complete",
        "freshness": {"observed_at": BASE.isoformat(), "max_age_seconds": 600},
        "coverage": {"scope": "all_namespaces", "pagination": "continue", "event_count": 1},
        "events": [{"uid": "evt-1", "name": "e", "count": 1}],
    }

    payload = evidence_inventory_payload(_kubernetes(event_capture=capture))

    assert payload is not None
    summary = payload["summary"]
    assert "events" not in summary["kubernetes_event_capture"]
    assert summary["kubernetes_event_facts"] == [{"uid": "evt-1", "name": "e", "count": 1}]


# --- 잘못된 증명은 삭제 권한을 주지 않고 오류도 내지 않는다 ---------------------------------

MALFORMED_PROOFS = {
    "reasons_int": {"reason_codes": 7},
    "reasons_true": {"reason_codes": True},
    "reasons_non_string": {"reason_codes": [7]},
    "reasons_string": {"reason_codes": "collection_rbac_denied"},
    "reasons_missing": {"reason_codes": None},
    "observed_string": {"observed": "yes"},
    "observed_int": {"observed": 1},
    "complete_string": {"complete": "true"},
    "delete_safe_int": {"delete_safe": 1},
    "truncated_string": {"truncated": "false"},
    "truncated_missing": {"truncated": None},
    "selector_int": {"label_selector": 7},
    "complete_with_reason": {"reason_codes": ["collection_rbac_denied"]},
    "complete_with_selector": {"label_selector": "app=a"},
    "complete_but_truncated": {"truncated": True},
    "complete_but_unobserved": {"observed": False},
}


@pytest.mark.parametrize("name", sorted(MALFORMED_PROOFS))
def test_malformed_proof_keeps_the_observation_and_grants_no_deletion(name: str) -> None:
    entry = _pod_coverage(**MALFORMED_PROOFS[name])

    payload = evidence_inventory_payload(_kubernetes(collection_coverage=[entry]))

    assert payload is not None
    assert [item["name"] for item in payload["resources"]] == ["api-0"]
    forwarded = payload["summary"]["collection_coverage"]
    assert len(forwarded) == 1
    assert forwarded[0]["complete"] is False and forwarded[0]["delete_safe"] is False
    assert forwarded[0]["reason_codes"]


@pytest.mark.parametrize("junk", ["text", 7, {"a": 1}, True, [None, 7, "x"]])
def test_a_non_list_or_junk_coverage_value_is_ignored_without_error(junk: Any) -> None:
    payload = evidence_inventory_payload(_kubernetes(collection_coverage=junk))

    assert payload is not None
    assert not payload["summary"].get("collection_coverage")
    assert "scoped_coverage_authority" not in payload["summary"]


CONTRADICTORY_LIMITS = [
    {"truncated": True, "lists": {}},
    {"truncated": "bad", "lists": {}},
    {"truncated": "bad", "lists": {"events": {}}},
    {"truncated": False, "lists": {"events": {}}},
    {"truncated": True},
    {"lists": {}},
    {},
]


@pytest.mark.parametrize(
    "limits", ["x", 7, {"lists": 7}, {"lists": []}, [], True, *CONTRADICTORY_LIMITS]
)
def test_junk_collection_limits_drop_every_proof_without_error(limits: Any) -> None:
    payload = evidence_inventory_payload(
        _kubernetes(collection_coverage=[_pod_coverage()], collection_limits=limits)
    )

    assert payload is not None
    assert "collection_coverage" not in payload["summary"]
    assert "scoped_coverage_authority" not in payload["summary"]


def test_well_formed_limits_that_name_other_lists_keep_the_pod_proof() -> None:
    limits = {"truncated": True, "lists": {"events": {"truncated": True}}}

    payload = evidence_inventory_payload(
        _kubernetes(collection_coverage=[_pod_coverage()], collection_limits=limits)
    )

    assert payload is not None
    assert payload["summary"]["collection_coverage"][0]["complete"] is True
