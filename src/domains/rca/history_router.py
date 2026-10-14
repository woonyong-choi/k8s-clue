"""Incident history read API — Timeline ledger evidence under per-source cluster grants."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import ValidationError

from domains.identity.dependencies import require_cluster_access, require_session
from domains.rca.history import (
    OBSERVATION_MAX_GAP_MS,
    assess_completeness,
    effective_window,
    incident_history_window,
    observation_from_snapshot_row,
    project_resources,
)
from domains.rca.repository import RcaRepository
from domains.timeline.predicate import TimelineEvidencePredicate
from domains.timeline.repository import TimelineLedgerReadScope
from packages.contracts.identity import Permission
from packages.contracts.parity import ClusterScope
from packages.contracts.timeline import TimelineQuery, TimelineWindow
from packages.contracts.timeline.history import (
    INCIDENT_HISTORY_PATH,
    IncidentHistoryEvidence,
    IncidentHistoryResponse,
)
from packages.runtime.dependencies import get_db

HTTP_NOT_FOUND = 404
HTTP_FORBIDDEN = 403
HTTP_UNPROCESSABLE = 422
INCIDENT_HISTORY_NOT_FOUND = "Incident history not found"
DEFAULT_HISTORY_LIMIT = 200
MAX_HISTORY_LIMIT = 500
MAX_HISTORY_WINDOW_MS = 86_400_000
MAX_EVIDENCE_REFERENCES = 100

router = APIRouter()


@router.get(INCIDENT_HISTORY_PATH, response_model=IncidentHistoryResponse)
async def get_incident_history(
    correlation_id: str,
    from_ms: Annotated[int | None, Query(ge=0)] = None,
    to_ms: Annotated[int | None, Query(gt=0)] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_HISTORY_LIMIT)] = DEFAULT_HISTORY_LIMIT,
    current: Any = Depends(require_session),
    db: Any = Depends(get_db),
) -> IncidentHistoryResponse:
    workspace_id = getattr(current, "workspace_id", None)
    if not isinstance(workspace_id, str) or not workspace_id:
        _raise_not_found()

    report = await _incident_report(db, workspace_id, correlation_id)
    cluster_id = report.get("cluster_id")
    if not isinstance(cluster_id, str) or not cluster_id:
        _raise_not_found()
    # The gate is the same RCA grant as the bundle API; denial and absence look alike.
    _require_or_not_found(db, current, workspace_id, cluster_id, Permission.RCA_READ)

    now_ms = _now_ms()
    # The response never speaks for time that has not happened yet.
    window = effective_window(_requested_window(report, from_ms=from_ms, to_ms=to_ms), now_ms)
    inventory_authorized = _is_granted(
        db, current, workspace_id, cluster_id, Permission.INVENTORY_READ
    )
    events_authorized = _is_granted(db, current, workspace_id, cluster_id, Permission.EVIDENCE_READ)
    read_scope = TimelineLedgerReadScope(
        workspace_id=workspace_id,
        scopes=(ClusterScope(workspace_id=workspace_id, cluster_id=cluster_id),),
        inventory_cluster_ids=frozenset({cluster_id}) if inventory_authorized else frozenset(),
        kubernetes_event_cluster_ids=frozenset({cluster_id}) if events_authorized else frozenset(),
        incident_cluster_ids=frozenset({cluster_id}),
    )
    query = TimelineQuery(scopes=read_scope.scopes, window=window, mode="frozen")
    predicate = TimelineEvidencePredicate.from_query(read_scope, query)

    snapshot = await asyncio.to_thread(
        db.snapshot_timeline_events, read_scope, predicate=predicate, limit=limit
    )
    capture_coverage = await asyncio.to_thread(
        db.snapshot_timeline_coverage, read_scope, window=window
    )
    observation_rows = (
        await asyncio.to_thread(
            db.list_inventory_snapshot_observations,
            workspace_id=workspace_id,
            cluster_id=cluster_id,
            window=window,
            margin_ms=OBSERVATION_MAX_GAP_MS,
        )
        if inventory_authorized
        else []
    )
    # Evidence keys and event ids are evidence.read metadata; without the grant they stay hidden.
    evidence_rows = (
        await asyncio.to_thread(
            db.list_evidence_windows_by_correlation,
            workspace_id,
            cluster_id,
            correlation_id,
            limit=MAX_EVIDENCE_REFERENCES,
        )
        if events_authorized
        else []
    )

    observations = [
        observation
        for row in observation_rows
        if (observation := observation_from_snapshot_row(row)) is not None
    ]
    completeness = assess_completeness(
        window=window,
        now_ms=now_ms,
        observations=observations,
        capture_coverage=capture_coverage,
        scope_cluster=(workspace_id, cluster_id),
        truncated=snapshot.truncated,
        source_unavailable=not (inventory_authorized and events_authorized),
        inventory_authorized=inventory_authorized,
        events_authorized=events_authorized,
        has_events=bool(snapshot.events),
    )
    unavailable = completeness.status == "unavailable"
    events = _ordered(snapshot.events)
    return IncidentHistoryResponse(
        correlation_id=correlation_id,
        cluster_id=cluster_id,
        window=window,
        events=events,
        resources=project_resources(events, coverage=completeness.coverage),
        evidence=tuple(_evidence_reference(row) for row in evidence_rows),
        completeness=completeness,
        truncated=snapshot.truncated and not unavailable,
    )


async def _incident_report(db: RcaRepository, workspace_id: str, correlation_id: str) -> Any:
    reports = await asyncio.to_thread(
        db.list_rca_report_records,
        workspace_id,
        correlation_id=correlation_id,
        limit=1,
    )
    if not reports:
        _raise_not_found()
    report = reports[0]
    if report.get("workspace_id") != workspace_id or report.get("correlation_id") != correlation_id:
        _raise_not_found()
    return report


def _requested_window(report: Any, *, from_ms: int | None, to_ms: int | None) -> TimelineWindow:
    anchor = _report_anchor(report)
    if anchor is None and (from_ms is None or to_ms is None):
        # Without an incident time the server cannot choose a window on the caller's behalf.
        _raise_not_found()
    try:
        window = incident_history_window(
            anchor=anchor or datetime.fromtimestamp(0, tz=UTC), from_ms=from_ms, to_ms=to_ms
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=HTTP_UNPROCESSABLE, detail="invalid history window"
        ) from exc
    if window.to_ms - window.from_ms > MAX_HISTORY_WINDOW_MS:
        raise HTTPException(status_code=HTTP_UNPROCESSABLE, detail="history window is too wide")
    return window


def _report_anchor(report: Any) -> datetime | None:
    for key in ("first_seen_at", "created_at"):
        value = report.get(key)
        if isinstance(value, datetime):
            return value
        if isinstance(value, str) and value.strip():
            try:
                parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            except ValueError:
                continue
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return None


def _require_or_not_found(
    db: Any, current: Any, workspace_id: str, cluster_id: str, permission: Permission
) -> None:
    try:
        require_cluster_access(
            db,
            current,
            workspace_id,
            cluster_id,
            permission.value,
            detail=INCIDENT_HISTORY_NOT_FOUND,
        )
    except HTTPException as exc:
        if exc.status_code == HTTP_FORBIDDEN:
            raise HTTPException(
                status_code=HTTP_NOT_FOUND, detail=INCIDENT_HISTORY_NOT_FOUND
            ) from exc
        raise


def _is_granted(
    db: Any, current: Any, workspace_id: str, cluster_id: str, permission: Permission
) -> bool:
    try:
        require_cluster_access(db, current, workspace_id, cluster_id, permission.value)
    except HTTPException as exc:
        if exc.status_code == HTTP_FORBIDDEN:
            return False
        raise
    return True


def _now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1_000)


def _ordered(events: Any) -> tuple[Any, ...]:
    return tuple(sorted(events, key=lambda event: (event.occurred_at, event.source_key)))


def _evidence_reference(row: Any) -> IncidentHistoryEvidence:
    observed_at = row["updated_at"]
    return IncidentHistoryEvidence(
        evidence_key=row["evidence_key"],
        source_id=row["source_id"],
        window_start=row["window_start"],
        event_id=row["event_id"],
        observed_at=observed_at
        if observed_at.tzinfo is not None
        else observed_at.replace(tzinfo=UTC),
    )


def _raise_not_found() -> None:
    raise HTTPException(status_code=HTTP_NOT_FOUND, detail=INCIDENT_HISTORY_NOT_FOUND)
