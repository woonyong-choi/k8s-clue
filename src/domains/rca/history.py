"""Incident history projection: resource lifecycles and honest completeness.

Absence of evidence is never reported as health or recovery.  A history is
``complete`` only when every observation in the window proves a complete cut
and no observation gap, partial cut, truncation or unauthorized source exists.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from domains.inventory.kubernetes_events import EVENT_CAPTURE_REASON_COMPLETE
from domains.inventory.snapshot_evidence import COMPLETE_CUT_PROOF_KEY
from packages.contracts.parity import ClusterScope
from packages.contracts.timeline import (
    TimelineCoverage,
    TimelineEvent,
    TimelineInventoryLocatorSubject,
    TimelineResourceSubject,
    TimelineWindow,
)
from packages.contracts.timeline.history import (
    CompletenessReason,
    IncidentHistoryCompleteness,
    IncidentHistoryResource,
)

# A collector that has not reported for this long leaves an unobserved gap.
OBSERVATION_MAX_GAP_MS = 600_000

ResourceKey = tuple[str, str]


@dataclass(frozen=True)
class InventoryObservation:
    """One accepted collection cut and whether it proved a complete resource list."""

    collected_at_ms: int
    resources_complete: bool
    event_capture_complete: bool = False


def effective_window(window: TimelineWindow, now_ms: int) -> TimelineWindow:
    """Never claim the future: clamp the end to now when the window has begun."""
    if window.to_ms > now_ms > window.from_ms:
        return TimelineWindow(from_ms=window.from_ms, to_ms=now_ms)
    return window


def assess_completeness(
    *,
    window: TimelineWindow,
    now_ms: int,
    observations: Iterable[InventoryObservation],
    capture_coverage: Iterable[TimelineCoverage],
    scope_cluster: tuple[str, str],
    truncated: bool,
    source_unavailable: bool,
    inventory_authorized: bool,
    events_authorized: bool = True,
    has_events: bool = False,
) -> IncidentHistoryCompleteness:
    """Decide ``complete`` / ``partial`` / ``unavailable`` without inferring recovery.

    Each authorized source needs its own proof.  Inventory cuts never vouch for
    Kubernetes Events, and ledger rows already read are kept even when no
    inventory observation exists.
    """
    workspace_id, cluster_id = scope_cluster
    from_ms = window.from_ms
    to_ms = min(window.to_ms, now_ms)
    reasons: list[CompletenessReason] = []
    coverage: list[TimelineCoverage] = list(capture_coverage)

    if source_unavailable:
        reasons.append("source_unavailable")
    if truncated:
        reasons.append("truncated")
    if coverage:
        reasons.append("collection_gap")

    if not inventory_authorized:
        # Without authority over the observation record, completeness is unprovable.
        reasons.append("source_unavailable")
        return _finish(reasons, coverage, status="partial")

    ordered = sorted(observations, key=lambda item: item.collected_at_ms)
    in_range = [
        item for item in ordered if from_ms - OBSERVATION_MAX_GAP_MS <= item.collected_at_ms < to_ms
    ]
    if has_events and (not in_range or to_ms <= from_ms):
        # Ledger rows exist without any inventory observation: keep them, but the
        # unobserved inventory side is a gap, never a clean history.
        if from_ms < to_ms:
            coverage.append(
                TimelineCoverage(
                    scope=ClusterScope(workspace_id=workspace_id, cluster_id=cluster_id),
                    source="inventory",
                    from_ms=from_ms,
                    to_ms=to_ms,
                    reason="collection_gap",
                )
            )
        reasons.append("collection_gap")
        return _finish(reasons, coverage, status="partial")
    if not in_range or to_ms <= from_ms:
        # No events are returned for an unobserved window, so nothing was truncated.
        reasons = [reason for reason in reasons if reason != "truncated"]
        reasons.append("no_observation")
        return IncidentHistoryCompleteness(
            status="unavailable",
            coverage=(),
            reasons=_unique(reasons),
        )

    gaps = _observation_gaps(
        in_range,
        from_ms=from_ms,
        to_ms=to_ms,
        scope=(workspace_id, cluster_id),
    )
    if gaps:
        coverage.extend(gaps)
        reasons.append("collection_gap")
    scope = (workspace_id, cluster_id)
    partial = _partial_cut_intervals(
        in_range,
        to_ms=to_ms,
        scope=scope,
        source="inventory",
        proven=lambda o: o.resources_complete,
    )
    if events_authorized:
        partial += _partial_cut_intervals(
            in_range,
            to_ms=to_ms,
            scope=scope,
            source="kubernetes_event",
            proven=lambda o: o.event_capture_complete,
        )
    if partial:
        coverage.extend(partial)
        reasons.append("partial_scope")
    return _finish(reasons, coverage, status="partial" if reasons else "complete")


def project_resources(
    events: Sequence[TimelineEvent],
    *,
    coverage: Iterable[TimelineCoverage],
) -> tuple[IncidentHistoryResource, ...]:
    """Derive per-identity lifecycles; the same name with another UID stays separate."""
    gaps = tuple(coverage)
    grouped: dict[ResourceKey, list[TimelineEvent]] = {}
    for event in events:
        # A Kubernetes Event fact names the Event object, not the resource it concerns.
        if event.source != "inventory":
            continue
        key = _resource_key(event)
        if key is not None:
            grouped.setdefault(key, []).append(event)

    replacements = _uid_replacements(events)
    resources: list[IncidentHistoryResource] = []
    for key in sorted(grouped):
        items = sorted(grouped[key], key=lambda item: (item.occurred_at, item.source_key))
        subject = _history_subject(items[-1])
        first = _utc(items[0].occurred_at)
        last = _utc(items[-1].occurred_at)
        latest = items[-1]
        replaced = replacements.get(key)
        if (
            replaced is not None
            and latest.event_type != "delete"
            and latest.occurred_at <= replaced.occurred_at
        ):
            # The same inventory key now carries another UID, so this identity is gone,
            # but its deletion was never observed: the time is only an upper bound.
            resources.append(
                IncidentHistoryResource(
                    subject=subject,
                    lifecycle="deleted",
                    first_observed_at=first,
                    last_observed_at=last,
                    deleted_at=_utc(replaced.occurred_at),
                    deleted_at_bound="gap_bounded",
                )
            )
            continue
        if latest.event_type == "delete":
            deleted_at = _utc(latest.occurred_at)
            resources.append(
                IncidentHistoryResource(
                    subject=subject,
                    lifecycle="deleted",
                    first_observed_at=first,
                    last_observed_at=last,
                    deleted_at=deleted_at,
                    deleted_at_bound=_deletion_bound(deleted_at, gaps),
                )
            )
        else:
            resources.append(
                IncidentHistoryResource(
                    subject=subject,
                    lifecycle="present",
                    first_observed_at=first,
                    last_observed_at=last,
                )
            )
    return tuple(resources)


def incident_history_window(
    *,
    anchor: datetime,
    from_ms: int | None,
    to_ms: int | None,
) -> TimelineWindow:
    """Server default: one hour before and fifteen minutes after the incident."""
    anchor_ms = int(_utc(anchor).timestamp() * 1_000)
    start = from_ms if from_ms is not None else max(anchor_ms - 3_600_000, 0)
    end = to_ms if to_ms is not None else anchor_ms + 900_000
    return TimelineWindow(from_ms=start, to_ms=end)


def observation_from_snapshot_row(row: Mapping[str, object]) -> InventoryObservation | None:
    """Read a snapshot row; a missing proof means partial, never complete."""
    collected_at = row.get("collected_at")
    if not isinstance(collected_at, datetime):
        return None
    if row.get("status") == "ignored_stale":
        return None
    summary = row.get("summary")
    proof = summary.get(COMPLETE_CUT_PROOF_KEY) if isinstance(summary, Mapping) else None
    complete = isinstance(proof, Mapping) and proof.get("resources_complete") is True
    capture = row.get("event_capture")
    capture_complete = (
        isinstance(capture, Mapping)
        and capture.get("complete") is True
        and capture.get("truncated") is False
        and capture.get("reason") == EVENT_CAPTURE_REASON_COMPLETE
    )
    return InventoryObservation(
        collected_at_ms=int(_utc(collected_at).timestamp() * 1_000),
        resources_complete=complete,
        event_capture_complete=capture_complete,
    )


def _uid_replacements(events: Sequence[TimelineEvent]) -> dict[ResourceKey, TimelineEvent]:
    """Map an identity to the first later inventory event that shows another UID for its key."""
    by_native: dict[str, list[TimelineEvent]] = {}
    for event in events:
        if event.source == "inventory" and isinstance(event.subject, TimelineResourceSubject):
            by_native.setdefault(event.native_id, []).append(event)
    replacements: dict[ResourceKey, TimelineEvent] = {}
    for items in by_native.values():
        ordered = sorted(items, key=lambda item: (item.occurred_at, item.source_key))
        for earlier, later in zip(ordered, ordered[1:], strict=False):
            old = _resource_key(earlier)
            new = _resource_key(later)
            if old != new and old is not None and old not in replacements:
                replacements[old] = later
    return replacements


def _resource_key(event: TimelineEvent) -> ResourceKey | None:
    subject = event.subject
    if isinstance(subject, TimelineResourceSubject):
        return ("uid", subject.resource.uid)
    if isinstance(subject, TimelineInventoryLocatorSubject):
        return ("locator", subject.inventory_key)
    return None


def _history_subject(
    event: TimelineEvent,
) -> TimelineResourceSubject | TimelineInventoryLocatorSubject:
    subject = event.subject
    if isinstance(subject, TimelineResourceSubject | TimelineInventoryLocatorSubject):
        return subject
    raise ValueError("history resources require a resource or inventory locator subject")


def _deletion_bound(
    deleted_at: datetime, gaps: Sequence[TimelineCoverage]
) -> Literal["observed", "gap_bounded"]:
    deleted_ms = int(deleted_at.timestamp() * 1_000)
    for gap in gaps:
        if gap.reason != "collection_gap":
            continue  # a partial cut was still observed; only unobserved time bounds a deletion
        # A deletion seen at the observation that opens a gap is exact; one seen when the
        # collector returns after the gap is only an upper bound.
        if gap.from_ms < deleted_ms <= gap.to_ms:
            return "gap_bounded"
    return "observed"


def _observation_gaps(
    observations: Sequence[InventoryObservation],
    *,
    from_ms: int,
    to_ms: int,
    scope: tuple[str, str],
) -> list[TimelineCoverage]:
    cluster_scope = ClusterScope(workspace_id=scope[0], cluster_id=scope[1])
    points = [item.collected_at_ms for item in observations]
    gaps: list[TimelineCoverage] = []
    edges = [(from_ms, points[0])] if points[0] - from_ms > OBSERVATION_MAX_GAP_MS else []
    edges += [
        (left, right)
        for left, right in zip(points, points[1:], strict=False)
        if right - left > OBSERVATION_MAX_GAP_MS
    ]
    if to_ms - points[-1] > OBSERVATION_MAX_GAP_MS:
        edges.append((points[-1], to_ms))
    for start, end in edges:
        if start < end:
            gaps.append(
                TimelineCoverage(
                    scope=cluster_scope,
                    source="inventory",
                    from_ms=max(start, 0),
                    to_ms=max(end, 1),
                    reason="collection_gap",
                )
            )
    return gaps


def _partial_cut_intervals(
    observations: Sequence[InventoryObservation],
    *,
    to_ms: int,
    scope: tuple[str, str],
    source: Literal["inventory", "kubernetes_event"],
    proven: Callable[[InventoryObservation], bool],
) -> list[TimelineCoverage]:
    cluster_scope = ClusterScope(workspace_id=scope[0], cluster_id=scope[1])
    intervals: list[TimelineCoverage] = []
    for index, item in enumerate(observations):
        if proven(item):
            continue
        end = observations[index + 1].collected_at_ms if index + 1 < len(observations) else to_ms
        start = max(item.collected_at_ms, 0)
        if start < end:
            intervals.append(
                TimelineCoverage(
                    scope=cluster_scope,
                    source=source,
                    from_ms=start,
                    to_ms=end,
                    reason="partial_scope",
                )
            )
    return intervals


def _finish(
    reasons: Sequence[CompletenessReason],
    coverage: Sequence[TimelineCoverage],
    *,
    status: Literal["complete", "partial"],
) -> IncidentHistoryCompleteness:
    unique = _unique(reasons)
    if status == "complete" and unique:
        status = "partial"
    return IncidentHistoryCompleteness(
        status=status,
        coverage=tuple(coverage) if status == "partial" else (),
        reasons=unique,
    )


def _unique(reasons: Sequence[CompletenessReason]) -> tuple[CompletenessReason, ...]:
    return tuple(dict.fromkeys(reasons))


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
