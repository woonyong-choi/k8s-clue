"""inventory 도메인 repository — 클러스터 리소스 스냅샷·read model 영속."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from sqlalchemy import (
    Text,
    and_,
    column,
    func,
    or_,
    select,
    true,
    tuple_,
    union_all,
    update,
    values,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from domains.inventory.change_correlation import correlate_inventory_timeline_events
from domains.inventory.coverage import (
    InventoryDeleteScope,
    inventory_delete_scope_predicate,
    inventory_deletion_scopes,
    inventory_row_in_deletion_scopes,
)
from domains.inventory.kubernetes_events import (
    EVENT_CAPTURE_REASON_COMPLETE,
    EVENT_CAPTURE_SUMMARY_KEY,
    KubernetesEventCapture,
    KubernetesEventFactBatch,
    kubernetes_event_fact_timeline_events,
)
from domains.inventory.models import (
    ClusterInventoryResourceRecord,
    ClusterInventorySnapshotRecord,
    ClusterUsageSampleRecord,
    live_inventory_snapshot_clause,
    timeline_coverage_snapshot_clause,
)
from domains.inventory.resource_types import project_inventory_product_counts
from domains.inventory.snapshot_evidence import (
    COMPLETE_CUT_PROOF_KEY,
    FIRST_OBSERVATION_BASIS,
    SCOPED_COVERAGE_AUTHORITY_KEY,
)
from domains.inventory_filter.repository import (
    inventory_snapshot_lock_key,
    sync_inventory_filter_projection,
)
from domains.timeline.coverage import project_kubernetes_event_capture_coverage
from domains.timeline.mapping import inventory_timeline_event
from packages.contracts.event_bus.interfaces import JsonObject
from packages.contracts.timeline import TimelineCoverage, TimelineEvent, TimelineWindow
from packages.storage.engine import DatabaseConnection, iso_or_none

# 스냅샷 리소스 배치 업서트 청크 크기 — 다중 VALUES 1문으로 실행되는 행 수 상한.
# (파라미터 수 제한과 단일 트랜잭션 락 시간 사이의 절충값, env 아님: 계약이 아니라 내부 상수)
INVENTORY_UPSERT_CHUNK = 500
TIMELINE_COVERAGE_READ_CHUNK = 128
TIMELINE_COVERAGE_RESPONSE_LIMIT = 256
KUBERNETES_EVENT_CAPTURE_CANDIDATE_LIMIT = 128

SYNTHETIC_NAMESPACE = None
HEALTH_RESOURCE_TYPE = "health"
USAGE_RESOURCE_TYPE = "usage"
UNKNOWN_STATUS = "unknown"

# fleet 롤업 대상 리소스 타입·판정 기준값 — kubernetes_snapshot 이 기록하는 값과 동일해야 함.
POD_RESOURCE_TYPE = "pod"
NODE_RESOURCE_TYPE = "node"
WORKLOAD_RESOURCE_TYPE = "workload"
EVENT_RESOURCE_TYPE = "event"
TLS_SECRET_TYPE = "kubernetes.io/tls"
CERT_MANAGER_API_GROUP = "cert-manager.io"
FLEET_ROLLUP_RESOURCE_TYPES = (POD_RESOURCE_TYPE, NODE_RESOURCE_TYPE, WORKLOAD_RESOURCE_TYPE)
POD_RUNNING_STATUS = "Running"
NODE_READY_STATUS = "Ready"
DEGRADED_HEALTH = "degraded"

# 이 필드들은 수집 시점·read-model bookkeeping 이 아니라 실제 inventory resource의
# 상태를 뜻한다. 스냅샷 ID/관측시각은 매 수집마다 달라질 수 있으므로 timeline 변경 판정에
# 포함하지 않는다.
INVENTORY_TIMELINE_SEMANTIC_FIELDS = (
    "resource_type",
    "api_version",
    "kind",
    "namespace",
    "name",
    "uid",
    "resource_version",
    "status",
    "health",
    "labels",
    "annotations",
    "summary",
    "raw",
)
InventoryTimelineChange = Literal["add", "update", "delete"]


@dataclass(frozen=True)
class InventorySnapshotMutation:
    """수집 저장의 내부 결과.

    ``timeline_events``는 ledger append 전의 도메인 사실이다. HTTP 응답은 ``result``만
    사용하므로 내부 mutation/ledger sequence가 외부 계약으로 새지 않는다.
    """

    result: JsonObject
    timeline_events: tuple[TimelineEvent, ...] = ()


def _latest_inventory_snapshots_statement(
    workspace_id: str,
    cluster_ids: Iterable[str],
) -> Any:
    """Build one bounded index lookup per requested cluster inside one SQL statement.

    A window rank over the append-only snapshot history reads every historical row
    for every selected cluster before it can retain rank one.  The lateral lookup
    instead performs one ordered probe against
    ``ix_inventory_snapshots_live_scope_latest`` and stops at the first live
    snapshot for each distinct requested cluster.
    """

    canonical_ids = tuple(sorted(set(cluster_ids)))
    requested = (
        values(column("cluster_id", Text), name="requested_inventory_clusters")
        .data([(cluster_id,) for cluster_id in canonical_ids])
        .alias("requested_inventory_clusters")
    )
    table = ClusterInventorySnapshotRecord.__table__
    latest = (
        select(
            table.c.snapshot_id,
            table.c.workspace_id,
            table.c.cluster_id,
            table.c.agent_id,
            table.c.source,
            table.c.status,
            table.c.collected_at,
            table.c.resource_count,
            table.c.summary,
            table.c.created_at,
        )
        .where(
            table.c.workspace_id == workspace_id,
            table.c.cluster_id == requested.c.cluster_id,
            table.c.status != "ignored_stale",
            live_inventory_snapshot_clause(table),
        )
        .order_by(table.c.created_at.desc(), table.c.snapshot_id.desc())
        .limit(1)
        .lateral("latest_inventory_snapshot")
    )
    return select(latest).select_from(requested.join(latest, true()))


def _latest_inventory_snapshot_id_scalar(workspace_id: str, cluster_id: str) -> Any:
    table = ClusterInventorySnapshotRecord.__table__
    return (
        select(table.c.snapshot_id)
        .where(
            table.c.workspace_id == workspace_id,
            table.c.cluster_id == cluster_id,
            table.c.status != "ignored_stale",
            live_inventory_snapshot_clause(table),
        )
        .order_by(table.c.created_at.desc(), table.c.snapshot_id.desc())
        .limit(1)
        .scalar_subquery()
    )


def _inventory_resource_counts_by_cluster_statement(
    workspace_id: str,
    cluster_ids: set[str],
) -> Any:
    table = ClusterInventoryResourceRecord.__table__
    latest = _latest_inventory_snapshots_statement(
        workspace_id,
        cluster_ids,
    ).subquery("latest_inventory_count_snapshots")
    return (
        select(
            table.c.cluster_id,
            table.c.resource_type,
            table.c.health,
            func.count().label("count"),
        )
        .select_from(
            table.join(
                latest,
                and_(
                    latest.c.workspace_id == table.c.workspace_id,
                    latest.c.cluster_id == table.c.cluster_id,
                    latest.c.snapshot_id == table.c.snapshot_id,
                ),
            )
        )
        .where(
            table.c.workspace_id == workspace_id,
            table.c.cluster_id.in_(sorted(cluster_ids)),
            table.c.deleted_at.is_(None),
        )
        .group_by(table.c.cluster_id, table.c.resource_type, table.c.health)
        .order_by(table.c.cluster_id, table.c.resource_type, table.c.health)
    )


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def parse_observed_at(value: str | None) -> datetime:
    return parse_timestamp(value) or datetime.now(UTC)


def inventory_resource_times(
    resource_type: str,
    summary: JsonObject,
    collected_at: datetime,
) -> tuple[datetime, datetime]:
    """Return first/last occurrence time without conflating collection time.

    Kubernetes Event objects can remain in the API long after the underlying failure was
    resolved. Their first/last timestamps describe the event; ``collected_at`` only says
    when the agent happened to read that object.
    """
    if resource_type != EVENT_RESOURCE_TYPE:
        return collected_at, collected_at
    parsed_first = parse_timestamp(
        str(summary.get("first_timestamp")) if summary.get("first_timestamp") else None
    )
    parsed_last = parse_timestamp(
        str(summary.get("last_timestamp")) if summary.get("last_timestamp") else None
    )
    first_seen = parsed_first or parsed_last or collected_at
    last_seen = parsed_last or parsed_first or collected_at
    return first_seen, last_seen


def inventory_resource_key(
    workspace_id: str,
    cluster_id: str,
    resource_type: str,
    api_version: str,
    namespace: str | None,
    kind: str,
    name: str,
) -> str:
    identity = [
        workspace_id,
        cluster_id,
        resource_type,
        api_version,
        namespace or "",
        kind,
        name,
    ]
    raw = json.dumps(identity, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def inventory_resource_identity(resource: Mapping[str, object]) -> tuple[str, ...]:
    """Return the complete identity encoded by ``inventory_resource_key``."""
    namespace = resource.get("namespace")
    return (
        str(resource.get("workspace_id") or ""),
        str(resource.get("cluster_id") or ""),
        str(resource.get("resource_type") or ""),
        str(resource.get("api_version") or ""),
        str(namespace) if namespace is not None else "",
        str(resource.get("kind") or ""),
        str(resource.get("name") or ""),
    )


def preserve_existing_inventory_keys(
    current_rows: Sequence[JsonObject],
    previous_rows: Sequence[Mapping[str, object]],
) -> list[JsonObject]:
    """Preserve history keys only when the complete Kubernetes identity is unchanged."""
    existing_keys = {
        inventory_resource_identity(row): str(row["inventory_key"])
        for row in previous_rows
        if row.get("inventory_key")
    }
    preserved = [
        {
            **row,
            "inventory_key": existing_keys.get(
                inventory_resource_identity(row), row["inventory_key"]
            ),
        }
        for row in current_rows
    ]
    return dedupe_inventory_rows(preserved)


def resource_type_of(resource: JsonObject) -> str:
    return str(resource.get("resource_type") or "custom").strip().lower()


def normalize_inventory_resource(
    resource: JsonObject,
    *,
    workspace_id: str,
    cluster_id: str,
    snapshot_id: str,
    observed_at: datetime,
) -> JsonObject:
    resource_type = resource_type_of(resource)
    api_version = str(resource.get("api_version") or "")
    kind = str(resource.get("kind") or resource_type)
    namespace = resource.get("namespace")
    name = str(resource.get("name") or resource.get("uid") or f"{resource_type}-resource")
    summary = dict(resource.get("summary") or {})
    first_seen_at, last_seen_at = inventory_resource_times(
        resource_type,
        summary,
        observed_at,
    )
    return {
        "inventory_key": inventory_resource_key(
            workspace_id,
            cluster_id,
            resource_type,
            api_version,
            str(namespace) if namespace is not None else None,
            kind,
            name,
        ),
        "snapshot_id": snapshot_id,
        "workspace_id": workspace_id,
        "cluster_id": cluster_id,
        "resource_type": resource_type,
        "api_version": api_version,
        "kind": kind,
        "namespace": str(namespace) if namespace is not None else None,
        "name": name,
        "uid": resource.get("uid"),
        "resource_version": resource.get("resource_version"),
        "status": str(resource.get("status") or UNKNOWN_STATUS),
        "health": str(resource.get("health") or UNKNOWN_STATUS),
        "labels": dict(resource.get("labels") or {}),
        "annotations": dict(resource.get("annotations") or {}),
        "summary": summary,
        "raw": dict(resource.get("raw") or {}),
        "observed_at": last_seen_at,
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
        "deleted_at": None,
    }


def dedupe_inventory_rows(rows: list[JsonObject]) -> list[JsonObject]:
    """conflict key(inventory_key) 중복 행 제거 — 마지막 관측 승리(last-wins).

    kubernetes provider 가 namespace 별 쿼리 결과를 병합하면 cluster-scoped 리소스
    (node 등)가 같은 스냅샷 안에 중복 수집될 수 있다. 같은 배치 VALUES 에 같은
    conflict key 가 두 번 들어가면 postgres 가 "ON CONFLICT DO UPDATE command cannot
    affect row a second time"(CardinalityViolation) 으로 스냅샷 저장 전체를 실패시키므로,
    upsert 전에 키당 1행으로 줄인다. dict 삽입 순서 특성상 위치는 첫 관측, 값은 마지막
    관측이 남는다.
    """
    by_key: dict[str, JsonObject] = {}
    for row in rows:
        by_key[str(row["inventory_key"])] = row
    return list(by_key.values())


def snapshot_resources(payload: JsonObject) -> list[JsonObject]:
    resources = [dict(item) for item in payload.get("resources", [])]
    health = dict(payload.get("health") or {})
    if health:
        resources.append(
            {
                "resource_type": HEALTH_RESOURCE_TYPE,
                "api_version": "platform/v1",
                "kind": "ClusterHealth",
                "namespace": SYNTHETIC_NAMESPACE,
                "name": "cluster",
                "status": str(health.get("status") or UNKNOWN_STATUS),
                "health": str(health.get("health") or health.get("status") or UNKNOWN_STATUS),
                "summary": health,
                "raw": health,
            }
        )
    usage = dict(payload.get("usage") or {})
    if usage:
        resources.append(
            {
                "resource_type": USAGE_RESOURCE_TYPE,
                "api_version": "platform/v1",
                "kind": "ClusterUsage",
                "namespace": SYNTHETIC_NAMESPACE,
                "name": "cluster",
                "status": str(usage.get("status") or "sampled"),
                "health": UNKNOWN_STATUS,
                "summary": usage,
                "raw": usage,
            }
        )
    return resources


def _certificate_observation(row: Mapping[str, Any]) -> JsonObject:
    secret = {
        "inventory_key": str(row["secret_inventory_key"]),
        "api_version": str(row["secret_api_version"]),
        "kind": str(row["secret_kind"]),
        "namespace": (
            str(row["secret_namespace"]) if row.get("secret_namespace") is not None else None
        ),
        "name": str(row["secret_name"]),
        "uid": str(row["secret_uid"]) if row.get("secret_uid") is not None else None,
        "observed_at": iso_or_none(row.get("secret_observed_at")),
    }
    if row.get("certificate_inventory_key") is None:
        return {"secret": secret, "certificate": None}
    return {
        "secret": secret,
        "certificate": {
            "inventory_key": str(row["certificate_inventory_key"]),
            "api_version": str(row["certificate_api_version"]),
            "kind": str(row["certificate_kind"]),
            "namespace": (
                str(row["certificate_namespace"])
                if row.get("certificate_namespace") is not None
                else None
            ),
            "name": str(row["certificate_name"]),
            "uid": (
                str(row["certificate_uid"]) if row.get("certificate_uid") is not None else None
            ),
            "raw": dict(row.get("certificate_raw") or {}),
            "observed_at": iso_or_none(row.get("certificate_observed_at")),
        },
    }


def snapshot_summary(payload: JsonObject) -> JsonObject:
    return {
        "summary": dict(payload.get("summary") or {}),
        "health": dict(payload.get("health") or {}),
        "usage": dict(payload.get("usage") or {}),
    }


def inventory_timeline_events(
    *,
    workspace_id: str,
    cluster_id: str,
    observed_at: datetime,
    previous_rows: Sequence[Mapping[str, object]],
    current_rows: Sequence[Mapping[str, object]],
    resources_complete: bool,
    previous_event_batch: KubernetesEventFactBatch | None = None,
    current_event_batch: KubernetesEventFactBatch | None = None,
    delete_scopes: Sequence[InventoryDeleteScope] = (),
) -> tuple[TimelineEvent, ...]:
    """Derive durable inventory and Kubernetes Event facts from one collection cut.

    An incomplete collection may be missing arbitrary namespaces or resource kinds, so it
    cannot truthfully establish a creation.  It records what it saw: a first observation
    (never dated as a creation), a change of a resource seen before, and an absence only
    inside a ``delete_scopes`` entry the producer proved complete.  Kubernetes Event facts
    use a separate complete-capture proof because Event absence never means deletion.
    """
    inventory_events: tuple[TimelineEvent, ...] = ()
    if not resources_complete:
        previous_by_key = {
            str(row["inventory_key"]): row
            for row in previous_rows
            if is_timeline_inventory_resource(row)
        }
        current_by_key = {
            str(row["inventory_key"]): row
            for row in current_rows
            if is_timeline_inventory_resource(row)
        }
        scoped_changes: list[tuple[InventoryTimelineChange, Mapping[str, object], bool]] = []
        for inventory_key in sorted(current_by_key):
            current = current_by_key[inventory_key]
            previous = previous_by_key.get(inventory_key)
            if previous is None:
                scoped_changes.append(("add", current, True))
            elif inventory_resource_changed(previous, current):
                scoped_changes.append(("update", current, False))
        for inventory_key in sorted(set(previous_by_key) - set(current_by_key)):
            previous = previous_by_key[inventory_key]
            if inventory_row_in_deletion_scopes(previous, delete_scopes):
                scoped_changes.append(("delete", previous, False))
        inventory_events = tuple(
            inventory_change_timeline_event(
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                observed_at=observed_at,
                event_type=event_type,
                resource=resource,
                first_observation=first_observation,
            )
            for event_type, resource, first_observation in scoped_changes
        )
    if resources_complete:
        previous_by_key = {
            str(row["inventory_key"]): row
            for row in previous_rows
            if is_timeline_inventory_resource(row)
        }
        current_by_key = {
            str(row["inventory_key"]): row
            for row in current_rows
            if is_timeline_inventory_resource(row)
        }
        changes: list[tuple[InventoryTimelineChange, Mapping[str, object]]] = []
        for inventory_key in sorted(current_by_key):
            current = current_by_key[inventory_key]
            previous = previous_by_key.get(inventory_key)
            if previous is None:
                changes.append(("add", current))
            elif inventory_resource_changed(previous, current):
                changes.append(("update", current))
        for inventory_key in sorted(set(previous_by_key) - set(current_by_key)):
            changes.append(("delete", previous_by_key[inventory_key]))
        inventory_events = tuple(
            inventory_change_timeline_event(
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                observed_at=observed_at,
                event_type=event_type,
                resource=resource,
            )
            for event_type, resource in changes
        )
    event_events = (
        kubernetes_event_fact_timeline_events(
            workspace_id=workspace_id,
            cluster_id=cluster_id,
            observed_at=observed_at,
            previous=previous_event_batch,
            current=current_event_batch,
        )
        if current_event_batch is not None
        else ()
    )
    return inventory_events + event_events


def latest_complete_kubernetes_event_batch(
    conn: Any,
    *,
    workspace_id: str,
    cluster_id: str,
) -> KubernetesEventFactBatch | None:
    """Read a recent complete Event fact cut without scanning snapshot JSON history.

    ``summary`` is a large TOAST value and the old JSON-path predicates forced
    PostgreSQL to decode historical payloads until it found a complete capture.  The
    dedicated ``event_capture`` projection is covered by the Timeline capture partial
    index, so inspect only a bounded set of recent projections in Python.  Once a safe
    authoritative candidate is found, fetch that single snapshot body by primary key.

    Returning ``None`` when no authoritative capture exists inside the bounded window
    is deliberately fail-closed: it may suppress an Event delta, but it can never
    invent a previous complete cut or block current inventory/metric persistence.
    """
    snapshot = ClusterInventorySnapshotRecord.__table__
    candidate_statement = (
        select(snapshot.c.snapshot_id, snapshot.c.event_capture)
        .where(
            snapshot.c.workspace_id == workspace_id,
            snapshot.c.cluster_id == cluster_id,
            snapshot.c.status != "ignored_stale",
            snapshot.c.event_capture.is_not(None),
            snapshot.c.event_capture_observed_at.is_not(None),
        )
        .order_by(
            snapshot.c.event_capture_observed_at.desc(),
            snapshot.c.collected_at.desc(),
            snapshot.c.created_at.desc(),
            snapshot.c.snapshot_id.desc(),
        )
        .limit(KUBERNETES_EVENT_CAPTURE_CANDIDATE_LIMIT)
    )

    candidate_snapshot_id: str | None = None
    for row in conn.execute(candidate_statement).mappings().all():
        if not isinstance(row, Mapping):
            continue
        capture_projection = row.get("event_capture")
        if not isinstance(capture_projection, Mapping):
            continue
        capture = KubernetesEventCapture.from_snapshot_summary(
            {EVENT_CAPTURE_SUMMARY_KEY: dict(capture_projection)}
        )
        if capture.authoritative:
            snapshot_id = row.get("snapshot_id")
            if snapshot_id is not None:
                candidate_snapshot_id = str(snapshot_id)
                break

    if candidate_snapshot_id is None:
        return None

    snapshot_summary = conn.execute(
        select(snapshot.c.summary)
        .where(
            snapshot.c.snapshot_id == candidate_snapshot_id,
            snapshot.c.workspace_id == workspace_id,
            snapshot.c.cluster_id == cluster_id,
        )
        .limit(1)
    ).scalar_one_or_none()
    if not isinstance(snapshot_summary, Mapping):
        return None
    source_summary = snapshot_summary.get("summary")
    if not isinstance(source_summary, Mapping):
        return None
    batch = KubernetesEventFactBatch.from_snapshot_summary(source_summary)
    return batch if batch.capture.authoritative else None


def is_timeline_inventory_resource(resource: Mapping[str, object]) -> bool:
    """Exclude derived rollups and Event facts from generic inventory change mapping."""
    return str(resource.get("resource_type") or "") not in {
        HEALTH_RESOURCE_TYPE,
        USAGE_RESOURCE_TYPE,
        # Kubernetes Event has UID/count/last-occurrence semantics and enters
        # Timeline only through domains.inventory.kubernetes_events.
        EVENT_RESOURCE_TYPE,
    }


def inventory_resource_changed(
    previous: Mapping[str, object], current: Mapping[str, object]
) -> bool:
    """Compare persisted resource facts without collection bookkeeping noise."""
    return any(
        inventory_timeline_semantic_value(field, previous.get(field))
        != inventory_timeline_semantic_value(field, current.get(field))
        for field in INVENTORY_TIMELINE_SEMANTIC_FIELDS
    )


def inventory_timeline_semantic_value(field: str, value: object) -> object:
    """Strip a known collection-only annotation before comparing source facts."""
    if field == "summary" and isinstance(value, Mapping):
        summary = dict(value)
        summary.pop("collected_at", None)
        return summary
    return value


def inventory_change_timeline_event(
    *,
    workspace_id: str,
    cluster_id: str,
    observed_at: datetime,
    event_type: InventoryTimelineChange,
    resource: Mapping[str, object],
    first_observation: bool = False,
) -> TimelineEvent:
    """Map one resource delta without exposing raw resource data or ledger position.

    ``first_observation`` marks an add that only says the resource was seen; it is dated
    by the collection, never by an invented creation time.
    """
    inventory_key = str(resource["inventory_key"])
    kind = str(resource.get("kind") or resource.get("resource_type") or "Resource")
    name = str(resource.get("name") or inventory_key)
    namespace_value = resource.get("namespace")
    namespace = str(namespace_value) if namespace_value is not None else None
    uid_value = resource.get("uid")
    uid = str(uid_value) if uid_value is not None else None
    source_key = inventory_timeline_source_key(event_type, resource)
    verb = {"add": "added", "update": "updated", "delete": "deleted"}[event_type]
    if first_observation:
        verb = "first observed"
    return inventory_timeline_event(
        event_id=source_key,
        source_key=source_key,
        native_id=inventory_key,
        occurred_at=observed_at,
        workspace_id=workspace_id,
        cluster_id=cluster_id,
        api_version=str(resource.get("api_version") or ""),
        resource_kind=kind,
        namespace=namespace,
        name=name,
        uid=uid,
        title=f"{kind} {name} {verb}",
        event_type=event_type,
        metadata={"observation_basis": FIRST_OBSERVATION_BASIS} if first_observation else None,
    )


def inventory_timeline_source_key(
    event_type: InventoryTimelineChange,
    resource: Mapping[str, object],
) -> str:
    """Use a fact fingerprint, never a generated snapshot ID, for ledger idempotency."""
    inventory_key = str(resource["inventory_key"])
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                field: inventory_timeline_semantic_value(field, resource.get(field))
                for field in INVENTORY_TIMELINE_SEMANTIC_FIELDS
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()
    return f"inventory:{event_type}:{inventory_key}:{fingerprint}"


def _timeline_coverage_snapshot_rows(
    partitions: Iterable[Sequence[Mapping[str, object]]],
    *,
    cancelled: Callable[[], bool] | None,
) -> Iterator[dict[str, object]]:
    """Rebuild the projector's narrow input without retaining DB partitions."""
    for partition in partitions:
        if cancelled is not None and cancelled():
            return
        for row in partition:
            if cancelled is not None and cancelled():
                return
            yield {
                "cluster_id": row.get("cluster_id"),
                "status": row.get("status"),
                "summary": {
                    "summary": {
                        EVENT_CAPTURE_SUMMARY_KEY: row.get("event_capture"),
                    }
                },
            }


def first_container_image(raw: JsonObject, summary: JsonObject) -> str | None:
    """K8s 리소스 raw/summary 에서 첫 컨테이너 이미지를 찾음(workload → pod → summary 순)."""
    for path in (("spec", "template", "spec", "containers"), ("spec", "containers")):
        node: object = raw
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
            if node is None:
                break
        if isinstance(node, list):
            for container in node:
                image = container.get("image") if isinstance(container, dict) else None
                if image:
                    return str(image)
    image = summary.get("image")
    return str(image) if image else None


def _timeline_coverage_statement(
    *,
    workspace_id: str,
    cluster_ids: Iterable[str],
    window: TimelineWindow,
) -> Any:
    """Build a bounded ordered coverage proof scan backed by its partial index.

    Coverage before the requested window matters only when a gap was still open
    at ``window.from``.  Reading every older snapshot to reconstruct that single
    state made a short Timeline request proportional to the cluster lifetime.
    Two ordered lateral index probes now retain at most one opening gap per
    cluster: the first gap after its latest proven recovery.  The main scan then
    reads only rows observed inside the requested window.

    The selected opening row keeps the original observed failure bound.  Using
    merely the latest pre-window row would silently move that bound forward when
    a collector reported the same gap more than once.
    """
    canonical_cluster_ids = tuple(sorted(set(cluster_ids)))
    snapshots = ClusterInventorySnapshotRecord.__table__
    event_capture = snapshots.c.event_capture
    capture_observed_at = snapshots.c.event_capture_observed_at
    requested = (
        values(column("cluster_id", Text), name="requested_timeline_coverage_clusters")
        .data([(cluster_id,) for cluster_id in canonical_cluster_ids])
        .alias("requested_timeline_coverage_clusters")
    )
    window_from = datetime.fromtimestamp(window.from_ms / 1_000, tz=UTC)
    window_to = datetime.fromtimestamp(window.to_ms / 1_000, tz=UTC)

    latest_recovery = (
        select(
            capture_observed_at,
            snapshots.c.collected_at,
            snapshots.c.created_at,
            snapshots.c.snapshot_id,
        )
        .where(
            snapshots.c.workspace_id == workspace_id,
            snapshots.c.cluster_id == requested.c.cluster_id,
            timeline_coverage_snapshot_clause(snapshots),
            capture_observed_at < window_from,
            _timeline_capture_recovery_clause(event_capture),
        )
        .order_by(
            capture_observed_at.desc(),
            snapshots.c.collected_at.desc(),
            snapshots.c.created_at.desc(),
            snapshots.c.snapshot_id.desc(),
        )
        .limit(1)
        .lateral("latest_timeline_coverage_recovery")
    )
    after_latest_recovery = or_(
        latest_recovery.c.snapshot_id.is_(None),
        tuple_(
            capture_observed_at,
            snapshots.c.collected_at,
            snapshots.c.created_at,
            snapshots.c.snapshot_id,
        )
        > tuple_(
            latest_recovery.c.event_capture_observed_at,
            latest_recovery.c.collected_at,
            latest_recovery.c.created_at,
            latest_recovery.c.snapshot_id,
        ),
    )
    opening_gap = (
        select(
            snapshots.c.cluster_id,
            snapshots.c.status,
            event_capture.label("event_capture"),
            capture_observed_at,
            snapshots.c.collected_at,
            snapshots.c.created_at,
            snapshots.c.snapshot_id,
        )
        .where(
            snapshots.c.workspace_id == workspace_id,
            snapshots.c.cluster_id == requested.c.cluster_id,
            timeline_coverage_snapshot_clause(snapshots),
            capture_observed_at < window_from,
            _timeline_capture_gap_clause(event_capture),
            after_latest_recovery,
        )
        .order_by(
            capture_observed_at.asc(),
            snapshots.c.collected_at.asc(),
            snapshots.c.created_at.asc(),
            snapshots.c.snapshot_id.asc(),
        )
        .limit(1)
        .lateral("opening_timeline_coverage_gap")
    )
    baseline = select(
        opening_gap.c.cluster_id,
        opening_gap.c.status,
        opening_gap.c.event_capture,
        opening_gap.c.event_capture_observed_at,
        opening_gap.c.collected_at,
        opening_gap.c.created_at,
        opening_gap.c.snapshot_id,
    ).select_from(requested.outerjoin(latest_recovery, true()).join(opening_gap, true()))
    window_rows = select(
        snapshots.c.cluster_id,
        snapshots.c.status,
        event_capture.label("event_capture"),
        capture_observed_at,
        snapshots.c.collected_at,
        snapshots.c.created_at,
        snapshots.c.snapshot_id,
    ).where(
        snapshots.c.workspace_id == workspace_id,
        snapshots.c.cluster_id.in_(canonical_cluster_ids),
        timeline_coverage_snapshot_clause(snapshots),
        capture_observed_at >= window_from,
        capture_observed_at < window_to,
    )
    bounded_rows = union_all(baseline, window_rows).subquery("bounded_timeline_coverage_rows")
    return select(
        bounded_rows.c.cluster_id,
        bounded_rows.c.status,
        bounded_rows.c.event_capture,
    ).order_by(
        bounded_rows.c.cluster_id.asc(),
        bounded_rows.c.event_capture_observed_at.asc(),
        bounded_rows.c.collected_at.asc(),
        bounded_rows.c.created_at.asc(),
        bounded_rows.c.snapshot_id.asc(),
    )


def _timeline_capture_gap_clause(event_capture: Any) -> Any:
    """Match the explicit gap evidence accepted by the pure projector."""
    coverage = event_capture["coverage"]
    gap = func.btrim(coverage["gap"].astext)
    reason = func.btrim(event_capture["reason"].astext)
    observed_at = func.btrim(event_capture["freshness"]["observed_at"].astext)
    return and_(
        func.jsonb_typeof(event_capture["complete"]) == "boolean",
        event_capture["complete"].astext == "false",
        func.jsonb_typeof(event_capture["truncated"]) == "boolean",
        observed_at.is_not(None),
        observed_at != "",
        gap.is_not(None),
        gap != "",
        reason == gap,
        coverage["scope"].astext == "all_namespaces",
        coverage["pagination"].astext == "continue",
    )


def _timeline_capture_recovery_clause(event_capture: Any) -> Any:
    """Match a complete global capture that can close a coverage gap.

    Numeric regular expressions deliberately avoid casts: malformed JSON must
    fail closed instead of aborting the whole Timeline request.
    """
    coverage = event_capture["coverage"]
    freshness = event_capture["freshness"]
    observed_at = func.btrim(freshness["observed_at"].astext)
    return and_(
        func.jsonb_typeof(event_capture["complete"]) == "boolean",
        event_capture["complete"].astext == "true",
        func.jsonb_typeof(event_capture["truncated"]) == "boolean",
        event_capture["truncated"].astext == "false",
        func.btrim(event_capture["reason"].astext) == EVENT_CAPTURE_REASON_COMPLETE,
        observed_at.is_not(None),
        observed_at != "",
        freshness["max_age_seconds"].astext.op("~")(r"^[1-9][0-9]*$"),
        coverage["scope"].astext == "all_namespaces",
        coverage["pagination"].astext == "continue",
        or_(coverage["gap"].astext.is_(None), coverage["gap"].astext == ""),
        coverage["page_count"].astext.op("~")(r"^[1-9][0-9]*$"),
        and_(
            func.jsonb_typeof(coverage["event_count"]) == "number",
            coverage["event_count"].astext.op("~")(r"^(0|[1-9][0-9]*)$"),
        ),
        func.btrim(coverage["resource_version"].astext) != "",
    )


def _event_capture_projection(capture: KubernetesEventCapture) -> JsonObject | None:
    """Persist only the small proof Timeline needs, detached from the inventory summary."""
    if capture.observed_at is None:
        return None
    return {
        "complete": capture.complete,
        "truncated": capture.truncated,
        "reason": capture.reason,
        "freshness": {
            "observed_at": capture.observed_at.isoformat(),
            "max_age_seconds": capture.max_age_seconds,
        },
        "coverage": dict(capture.coverage),
    }


class InventoryRepository(DatabaseConnection):
    def snapshot_timeline_coverage(
        self,
        read_scope: Any,
        *,
        window: TimelineWindow,
        cancelled: Callable[[], bool] | None = None,
    ) -> tuple[TimelineCoverage, ...]:
        """Read durable global Event capture evidence for an authorized Timeline scope.

        Snapshot evidence, rather than the Timeline event ledger, is the source
        of completeness.  The pure projector proves both failure and recovery
        bounds and emits no coverage where either bound is unknown.  This read
        projects only the small capture proof from each snapshot and streams it
        in fixed-size partitions.  Full inventory summaries can contain every
        resource and must never be materialized for Timeline coverage.
        """
        cluster_ids = frozenset(
            str(cluster_id).strip()
            for cluster_id in getattr(read_scope, "kubernetes_event_cluster_ids", frozenset())
            if str(cluster_id).strip()
        )
        if not cluster_ids:
            return ()
        workspace_id = str(getattr(read_scope, "workspace_id", "") or "").strip()
        if not workspace_id:
            return ()
        statement = _timeline_coverage_statement(
            workspace_id=workspace_id,
            cluster_ids=cluster_ids,
            window=window,
        )
        with self.connection() as conn:
            rows = conn.execution_options(
                stream_results=True,
                max_row_buffer=TIMELINE_COVERAGE_READ_CHUNK,
            ).execute(statement)
            return project_kubernetes_event_capture_coverage(
                read_scope,
                window=window,
                snapshots=_timeline_coverage_snapshot_rows(
                    rows.mappings().partitions(TIMELINE_COVERAGE_READ_CHUNK),
                    cancelled=cancelled,
                ),
                snapshots_ordered=True,
                max_intervals=TIMELINE_COVERAGE_RESPONSE_LIMIT,
            )

    def save_live_cluster_usage_sample(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        sampled_at: datetime,
        usage: JsonObject,
    ) -> bool:
        """Persist one real realtime-gateway sample against the current inventory cut.

        The browser live path used to terminate at the gateway's in-memory hub.  That made
        alert evaluation and replay depend on the much slower evidence snapshot cadence.
        Reusing the latest authoritative snapshot id keeps the existing temporal join and
        authorization boundary intact while storing only values the agent actually sent.
        """
        if not workspace_id or not cluster_id or not usage:
            return False
        snapshot = ClusterInventorySnapshotRecord.__table__
        samples = ClusterUsageSampleRecord.__table__
        with self.connection() as conn:
            snapshot_id = conn.execute(
                select(snapshot.c.snapshot_id)
                .where(
                    snapshot.c.workspace_id == workspace_id,
                    snapshot.c.cluster_id == cluster_id,
                    snapshot.c.status != "ignored_stale",
                )
                .order_by(snapshot.c.collected_at.desc(), snapshot.c.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            if snapshot_id is None:
                return False
            conn.execute(
                pg_insert(samples).values(
                    snapshot_id=str(snapshot_id),
                    workspace_id=workspace_id,
                    cluster_id=cluster_id,
                    sampled_at=sampled_at,
                    usage=dict(usage),
                )
            )
        return True

    def save_inventory_snapshot(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        agent_id: str,
        payload: JsonObject,
    ) -> JsonObject:
        """Compatibility persistence entry point without exposing internal timeline facts.

        The delegated mutation retains the complete-cut replacement boundary
        (``snapshot_id != current snapshot``); callers of this legacy response-only method
        cannot observe its internal timeline facts.
        """
        return self.save_inventory_snapshot_mutation(
            workspace_id=workspace_id,
            cluster_id=cluster_id,
            agent_id=agent_id,
            payload=payload,
        ).result

    def save_inventory_snapshot_with_timeline(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        agent_id: str,
        payload: JsonObject,
        append_timeline_events: Callable[[Sequence[TimelineEvent]], object],
    ) -> JsonObject:
        """Persist a snapshot and its Timeline facts in one transaction.

        ``append_timeline_events`` is the ledger append of the same database
        (``Database.append_timeline_events``).  Both writes join this unit of
        work, so a ledger failure rolls the snapshot back and a re-sent cut is
        absorbed by the ledger's source-key uniqueness instead of duplicating.
        """
        with self.unit_of_work():
            mutation = self.save_inventory_snapshot_mutation(
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                agent_id=agent_id,
                payload=payload,
            )
            if mutation.timeline_events:
                append_timeline_events(mutation.timeline_events)
        return mutation.result

    def list_inventory_snapshot_observations(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        window: TimelineWindow,
        margin_ms: int,
        limit: int = 1_000,
    ) -> list[JsonObject]:
        """Read accepted collection cuts around a window, newest bounded by ``limit``."""
        table = ClusterInventorySnapshotRecord.__table__
        lower = datetime.fromtimestamp(max(window.from_ms - margin_ms, 0) / 1_000, tz=UTC)
        upper = datetime.fromtimestamp(window.to_ms / 1_000, tz=UTC)
        statement = (
            select(table.c.collected_at, table.c.status, table.c.summary, table.c.event_capture)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.status != "ignored_stale",
                table.c.collected_at >= lower,
                table.c.collected_at < upper,
            )
            .order_by(table.c.collected_at.desc(), table.c.snapshot_id.desc())
            .limit(limit)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [dict(row) for row in reversed(rows)]

    def save_inventory_snapshot_mutation(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        agent_id: str,
        payload: JsonObject,
    ) -> InventorySnapshotMutation:
        snapshot_id = str(uuid.uuid4())
        observed_at = parse_observed_at(payload.get("collected_at"))
        resources = snapshot_resources(payload)
        # 중복 inventory_key 는 upsert 전에 제거 — 배치 안 중복은 CardinalityViolation 을 유발.
        normalized = dedupe_inventory_rows(
            [
                normalize_inventory_resource(
                    resource,
                    workspace_id=workspace_id,
                    cluster_id=cluster_id,
                    snapshot_id=snapshot_id,
                    observed_at=observed_at,
                )
                for resource in resources
            ]
        )

        snapshot_table = ClusterInventorySnapshotRecord.__table__
        resource_table = ClusterInventoryResourceRecord.__table__
        usage_table = ClusterUsageSampleRecord.__table__
        summary = snapshot_summary(payload)
        seen_types = {resource["resource_type"] for resource in normalized}
        marked_deleted = 0
        source_summary = dict(summary.get("summary") or {})
        current_event_batch = KubernetesEventFactBatch.from_snapshot_summary(source_summary)
        event_capture_projection = _event_capture_projection(current_event_batch.capture)
        collection_limits = source_summary.get("collection_limits")
        source_truncated = (
            isinstance(collection_limits, dict) and collection_limits.get("truncated") is True
        )
        declared_resources_complete = source_summary.get("resources_complete") is True
        resources_complete = bool(payload.get("replace")) and declared_resources_complete
        resources_complete = resources_complete and not source_truncated
        labels_complete = resources_complete and source_summary.get("labels_complete") is True
        partial_reason_codes: list[str] = []
        if not labels_complete:
            partial_reason_codes.append("source_labels_truncated")
        if source_truncated:
            partial_reason_codes.append("source_resources_truncated")
        elif not resources_complete:
            partial_reason_codes.append("source_resources_incomplete")
        # A scoped evidence snapshot is not fleet truth (live_inventory false) but its producer
        # proof may still authorize deletions in exactly the namespaces it proved.
        scoped_delete_scopes = inventory_deletion_scopes(
            {**source_summary, "live_inventory": True}
            if source_summary.get(SCOPED_COVERAGE_AUTHORITY_KEY) is True
            else source_summary
        )
        # Only the writer knows whether this cut replaced the full resource list, so the
        # proof is persisted; rows without it can never be read back as complete.
        summary = {**summary, COMPLETE_CUT_PROOF_KEY: {"resources_complete": resources_complete}}

        with self.connection() as conn:
            conn.execute(
                select(
                    func.pg_advisory_xact_lock(
                        inventory_snapshot_lock_key(workspace_id, cluster_id)
                    )
                )
            )
            latest_observed_at = conn.execute(
                select(func.max(snapshot_table.c.collected_at)).where(
                    snapshot_table.c.workspace_id == workspace_id,
                    snapshot_table.c.cluster_id == cluster_id,
                )
            ).scalar_one_or_none()
            if latest_observed_at is not None and observed_at < latest_observed_at:
                conn.execute(
                    pg_insert(snapshot_table).values(
                        snapshot_id=snapshot_id,
                        workspace_id=workspace_id,
                        cluster_id=cluster_id,
                        agent_id=agent_id,
                        source=str(payload.get("source") or "cluster-agent"),
                        status="ignored_stale",
                        collected_at=observed_at,
                        event_capture_observed_at=current_event_batch.capture.observed_at,
                        event_capture=event_capture_projection,
                        resource_count=len(normalized),
                        summary=summary,
                    )
                )
                return InventorySnapshotMutation(
                    result={
                        "accepted": False,
                        "snapshot_id": snapshot_id,
                        "cluster_id": cluster_id,
                        "resource_count": len(normalized),
                        "marked_deleted": 0,
                        "resource_types": sorted(seen_types),
                    }
                )
            # Read the prior live cut only after taking the same transaction-scoped advisory
            # lock used by all inventory writers. This makes concurrent collectors observe a
            # single ordered state transition before either one builds timeline facts.
            previous_rows = [
                dict(row)
                for row in conn.execute(
                    select(resource_table).where(
                        resource_table.c.workspace_id == workspace_id,
                        resource_table.c.cluster_id == cluster_id,
                        resource_table.c.deleted_at.is_(None),
                    )
                )
                .mappings()
                .all()
            ]
            normalized = preserve_existing_inventory_keys(normalized, previous_rows)
            # A non-authoritative cut cannot emit Event Timeline entries, so avoid
            # reading an older fact batch that it must never compare or append from.
            previous_event_batch = (
                latest_complete_kubernetes_event_batch(
                    conn,
                    workspace_id=workspace_id,
                    cluster_id=cluster_id,
                )
                if current_event_batch.capture.authoritative
                else None
            )
            timeline_events = inventory_timeline_events(
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                observed_at=observed_at,
                previous_rows=previous_rows,
                current_rows=normalized,
                resources_complete=resources_complete,
                previous_event_batch=previous_event_batch,
                current_event_batch=current_event_batch,
                delete_scopes=scoped_delete_scopes,
            )
            previous_rows_for_deletion = (
                previous_rows
                if resources_complete
                else [
                    row
                    for row in previous_rows
                    if inventory_row_in_deletion_scopes(row, scoped_delete_scopes)
                ]
            )
            missing_inventory_keys = sorted(
                {str(row["inventory_key"]) for row in previous_rows_for_deletion}
                - {str(row["inventory_key"]) for row in normalized}
            )
            conn.execute(
                pg_insert(snapshot_table).values(
                    snapshot_id=snapshot_id,
                    workspace_id=workspace_id,
                    cluster_id=cluster_id,
                    agent_id=agent_id,
                    source=str(payload.get("source") or "cluster-agent"),
                    status=str(payload.get("status") or "accepted"),
                    collected_at=observed_at,
                    event_capture_observed_at=current_event_batch.capture.observed_at,
                    event_capture=event_capture_projection,
                    resource_count=len(normalized),
                    summary=summary,
                )
            )
            # 배치 업서트 — 리소스당 1문(5천 팟 = 5천 쿼리)이던 것을 청크당 1문으로.
            # excluded.* 로 충돌 행을 새 값으로 갱신하므로 행별 set_ 값을 만들 필요가 없다.
            for start in range(0, len(normalized), INVENTORY_UPSERT_CHUNK):
                chunk = normalized[start : start + INVENTORY_UPSERT_CHUNK]
                insert = pg_insert(resource_table).values(
                    [{**resource, "deleted_at": None} for resource in chunk]
                )
                update_columns = {
                    column: insert.excluded[column]
                    for column in (
                        "snapshot_id",
                        "api_version",
                        "kind",
                        "namespace",
                        "name",
                        "uid",
                        "resource_version",
                        "status",
                        "health",
                        "labels",
                        "annotations",
                        "summary",
                        "raw",
                        "observed_at",
                        "last_seen_at",
                    )
                }
                conn.execute(
                    insert.on_conflict_do_update(
                        index_elements=[resource_table.c.inventory_key],
                        set_={
                            **update_columns,
                            "first_seen_at": func.least(
                                resource_table.c.first_seen_at,
                                insert.excluded.first_seen_at,
                            ),
                            "deleted_at": None,
                            "updated_at": func.now(),
                        },
                    )
                )
            if summary["usage"]:
                conn.execute(
                    pg_insert(usage_table).values(
                        snapshot_id=snapshot_id,
                        workspace_id=workspace_id,
                        cluster_id=cluster_id,
                        sampled_at=observed_at,
                        usage=summary["usage"],
                    )
                )
            if (resources_complete or scoped_delete_scopes) and missing_inventory_keys:
                delete_predicates = [
                    resource_table.c.workspace_id == workspace_id,
                    resource_table.c.cluster_id == cluster_id,
                    resource_table.c.snapshot_id != snapshot_id,
                    resource_table.c.inventory_key.in_(missing_inventory_keys),
                    resource_table.c.deleted_at.is_(None),
                ]
                if not resources_complete:
                    delete_predicates.append(
                        inventory_delete_scope_predicate(resource_table, scoped_delete_scopes)
                    )
                result = conn.execute(
                    update(resource_table)
                    .where(*delete_predicates)
                    .values(deleted_at=func.now(), updated_at=func.now())
                )
                marked_deleted = int(result.rowcount or 0)

            projection = sync_inventory_filter_projection(
                conn,
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                snapshot_id=snapshot_id,
                observed_at=observed_at,
                labels_complete=labels_complete,
                resources_complete=resources_complete,
                partial_reason_codes=partial_reason_codes,
                deletion_scopes=scoped_delete_scopes,
            )
            timeline_events = correlate_inventory_timeline_events(
                timeline_events,
                source_snapshot_id=snapshot_id,
                projection=projection,
            )

        return InventorySnapshotMutation(
            result={
                "accepted": True,
                "snapshot_id": snapshot_id,
                "cluster_id": cluster_id,
                "resource_count": len(normalized),
                "marked_deleted": marked_deleted,
                "resource_types": sorted(seen_types),
            },
            timeline_events=timeline_events,
        )

    def list_inventory_resources(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        resource_type: str | None = None,
        namespace: str | None = None,
        include_deleted: bool = False,
        limit: int = 200,
    ) -> list[JsonObject]:
        table = ClusterInventoryResourceRecord.__table__
        statement = select(table).where(
            table.c.workspace_id == workspace_id,
            table.c.cluster_id == cluster_id,
        )
        if resource_type:
            statement = statement.where(table.c.resource_type == resource_type)
        if namespace:
            statement = statement.where(table.c.namespace == namespace)
        if not include_deleted:
            statement = statement.where(
                table.c.deleted_at.is_(None),
                table.c.snapshot_id
                == _latest_inventory_snapshot_id_scalar(workspace_id, cluster_id),
            )
        statement = statement.order_by(
            table.c.resource_type,
            table.c.namespace.nullsfirst(),
            table.c.name,
        ).limit(max(1, min(limit, 1000)))
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [self.serialize_inventory_resource(dict(row)) for row in rows]

    def list_inventory_resources_by_kind(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        resource_type: str,
        kind: str,
        namespace: str | None = None,
        include_deleted: bool = False,
        limit: int = 200,
    ) -> list[JsonObject]:
        """List an exact Kubernetes kind inside one canonical resource family."""
        table = ClusterInventoryResourceRecord.__table__
        predicates = [
            table.c.workspace_id == workspace_id,
            table.c.cluster_id == cluster_id,
            table.c.resource_type == resource_type.strip().casefold(),
            func.lower(table.c.kind) == kind.strip().casefold(),
        ]
        if namespace:
            predicates.append(table.c.namespace == namespace)
        if not include_deleted:
            predicates.extend(
                (
                    table.c.deleted_at.is_(None),
                    table.c.snapshot_id
                    == _latest_inventory_snapshot_id_scalar(workspace_id, cluster_id),
                )
            )
        statement = (
            select(table)
            .where(*predicates)
            .order_by(table.c.namespace.nullsfirst(), table.c.name)
            .limit(max(1, min(limit, 1000)))
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [self.serialize_inventory_resource(dict(row)) for row in rows]

    def list_tls_secret_certificate_observations(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        limit: int,
    ) -> JsonObject:
        """Read TLS Secret identities with at most one matching cert-manager observation.

        Secret ``raw`` and ``data`` columns are deliberately absent from the
        selected response. Only the Certificate object is retained for the
        existing redacted provider-detail projector.
        """

        effective_limit = max(1, min(limit, 500))
        table = ClusterInventoryResourceRecord.__table__
        secret = table.alias("certificate_expiry_secret")
        certificate = table.alias("certificate_expiry_certificate")
        certificate_secret_name = certificate.c.raw["spec"]["secretName"].astext
        ranked_certificates = (
            select(
                certificate.c.inventory_key,
                certificate.c.api_version,
                certificate.c.kind,
                certificate.c.namespace,
                certificate.c.name,
                certificate.c.uid,
                certificate.c.raw,
                certificate.c.observed_at,
                certificate_secret_name.label("secret_name"),
                func.row_number()
                .over(
                    partition_by=(
                        certificate.c.workspace_id,
                        certificate.c.cluster_id,
                        certificate.c.namespace,
                        certificate_secret_name,
                    ),
                    order_by=(
                        certificate.c.observed_at.desc(),
                        certificate.c.inventory_key.desc(),
                    ),
                )
                .label("rank"),
            )
            .where(
                certificate.c.workspace_id == workspace_id,
                certificate.c.cluster_id == cluster_id,
                func.split_part(certificate.c.api_version, "/", 1) == CERT_MANAGER_API_GROUP,
                func.lower(certificate.c.kind) == "certificate",
                certificate.c.deleted_at.is_(None),
                certificate_secret_name.is_not(None),
                certificate_secret_name != "",
            )
            .cte("ranked_certificate_expiry_sources")
        )
        latest_certificate = (
            select(ranked_certificates)
            .where(ranked_certificates.c.rank == 1)
            .cte("latest_certificate_expiry_sources")
        )
        secret_type = func.coalesce(
            secret.c.raw["type"].astext,
            secret.c.summary["type"].astext,
            "",
        )
        statement = (
            select(
                secret.c.inventory_key.label("secret_inventory_key"),
                secret.c.api_version.label("secret_api_version"),
                secret.c.kind.label("secret_kind"),
                secret.c.namespace.label("secret_namespace"),
                secret.c.name.label("secret_name"),
                secret.c.uid.label("secret_uid"),
                secret.c.observed_at.label("secret_observed_at"),
                latest_certificate.c.inventory_key.label("certificate_inventory_key"),
                latest_certificate.c.api_version.label("certificate_api_version"),
                latest_certificate.c.kind.label("certificate_kind"),
                latest_certificate.c.namespace.label("certificate_namespace"),
                latest_certificate.c.name.label("certificate_name"),
                latest_certificate.c.uid.label("certificate_uid"),
                latest_certificate.c.raw.label("certificate_raw"),
                latest_certificate.c.observed_at.label("certificate_observed_at"),
            )
            .select_from(
                secret.outerjoin(
                    latest_certificate,
                    and_(
                        latest_certificate.c.namespace == secret.c.namespace,
                        latest_certificate.c.secret_name == secret.c.name,
                    ),
                )
            )
            .where(
                secret.c.workspace_id == workspace_id,
                secret.c.cluster_id == cluster_id,
                func.lower(secret.c.kind) == "secret",
                secret.c.deleted_at.is_(None),
                secret_type == TLS_SECRET_TYPE,
            )
            .order_by(
                secret.c.namespace.nullsfirst(),
                secret.c.name,
                secret.c.inventory_key,
            )
            .limit(effective_limit + 1)
        )
        with self.connection() as conn:
            rows = [dict(row) for row in conn.execute(statement).mappings().all()]
        return {
            "items": [_certificate_observation(row) for row in rows[:effective_limit]],
            "has_more": len(rows) > effective_limit,
        }

    def get_inventory_resource(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        resource_type: str,
        kind: str,
        name: str,
        namespace: str | None = None,
    ) -> JsonObject | None:
        """단일 resource identity 조회 — 드릴다운은 list 결과 추론 대신 이 계약을 사용."""
        table = ClusterInventoryResourceRecord.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.resource_type == resource_type.strip().lower(),
                func.lower(table.c.kind) == kind.strip().lower(),
                table.c.name == name,
                table.c.deleted_at.is_(None),
            )
            .order_by(table.c.last_seen_at.desc())
            .limit(1)
        )
        if namespace is None:
            statement = statement.where(table.c.namespace.is_(None))
        else:
            statement = statement.where(table.c.namespace == namespace)
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return self.serialize_inventory_resource(dict(row)) if row else None

    def get_latest_inventory_resource(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        resource_type: str,
        kind: str,
        name: str,
        namespace: str | None = None,
    ) -> JsonObject | None:
        """Read one resource only from the latest accepted live inventory snapshot."""

        table = ClusterInventoryResourceRecord.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.resource_type == resource_type.strip().lower(),
                func.lower(table.c.kind) == kind.strip().lower(),
                table.c.name == name,
                table.c.deleted_at.is_(None),
                table.c.snapshot_id
                == _latest_inventory_snapshot_id_scalar(workspace_id, cluster_id),
            )
            .limit(1)
        )
        if namespace is None:
            statement = statement.where(table.c.namespace.is_(None))
        else:
            statement = statement.where(table.c.namespace == namespace)
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return self.serialize_inventory_resource(dict(row)) if row else None

    def get_inventory_resource_by_api_version(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        resource_type: str,
        api_version: str,
        kind: str,
        name: str,
        namespace: str | None = None,
    ) -> JsonObject | None:
        """Read one resource only when its complete API-version identity matches.

        ``kind``/``namespace``/``name`` alone are not a Kubernetes identity:
        dynamic resources from different API groups can use the same kind and
        object name.  Contextual routes that carry an API group/version must
        use this query rather than the legacy detail lookup above.
        """
        table = ClusterInventoryResourceRecord.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.resource_type == resource_type.strip().lower(),
                table.c.api_version == api_version.strip(),
                func.lower(table.c.kind) == kind.strip().lower(),
                table.c.name == name,
                table.c.deleted_at.is_(None),
            )
            .order_by(table.c.last_seen_at.desc())
            .limit(1)
        )
        if namespace is None:
            statement = statement.where(table.c.namespace.is_(None))
        else:
            statement = statement.where(table.c.namespace == namespace)
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return self.serialize_inventory_resource(dict(row)) if row else None

    def list_inventory_resources_by_api_version(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        resource_type: str,
        api_version: str,
        kind: str,
        limit: int = 200,
    ) -> list[JsonObject]:
        """List only one complete API identity for safe contextual consumers.

        This is deliberately separate from the generic resource list: callers
        that carry a Kubernetes group/version must never broaden a candidate
        set by kind alone.
        """
        table = ClusterInventoryResourceRecord.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.resource_type == resource_type.strip().lower(),
                table.c.api_version == api_version.strip(),
                func.lower(table.c.kind) == kind.strip().lower(),
                table.c.deleted_at.is_(None),
            )
            .order_by(table.c.namespace.nullsfirst(), table.c.name)
            .limit(max(1, min(limit, 1000)))
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [self.serialize_inventory_resource(dict(row)) for row in rows]

    def get_inventory_resource_by_key(
        self,
        *,
        workspace_id: str,
        inventory_key: str,
    ) -> JsonObject | None:
        """서버가 발급한 inventory_key를 세션 workspace 안에서 다시 물질화한다."""
        table = ClusterInventoryResourceRecord.__table__
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.inventory_key == inventory_key,
                table.c.deleted_at.is_(None),
            )
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return self.serialize_inventory_resource(dict(row)) if row else None

    def read_inventory_cascade(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        resource: JsonObject,
        limit: int = 200,
    ) -> JsonObject:
        """Read a bounded, same-snapshot owner-UID cascade without kind/name guesses."""

        effective_limit = max(1, min(limit, 200))
        snapshot = self.latest_inventory_snapshot(workspace_id, cluster_id)
        snapshot_id = str((snapshot or {}).get("snapshot_id") or "")
        snapshot_envelope = (snapshot or {}).get("summary")
        source_summary = (
            snapshot_envelope.get("summary") if isinstance(snapshot_envelope, Mapping) else None
        )
        resources_complete = bool(
            isinstance(source_summary, Mapping) and source_summary.get("resources_complete") is True
        )
        root_uid = str(resource.get("uid") or "")
        if not snapshot_id or str(resource.get("snapshot_id") or "") != snapshot_id or not root_uid:
            return {
                "snapshot_id": snapshot_id,
                "resources_complete": False,
                "truncated": False,
                "dependents": [],
            }
        if not resources_complete:
            return {
                "snapshot_id": snapshot_id,
                "resources_complete": False,
                "truncated": False,
                "dependents": [],
            }

        table = ClusterInventoryResourceRecord.__table__
        frontier = {root_uid}
        visited = {root_uid}
        dependents: list[JsonObject] = []
        truncated = False
        with self.connection() as conn:
            while frontier:
                remaining = effective_limit - len(dependents)
                if remaining <= 0:
                    truncated = True
                    break
                rows = (
                    conn.execute(
                        select(table)
                        .where(
                            table.c.workspace_id == workspace_id,
                            table.c.cluster_id == cluster_id,
                            table.c.snapshot_id == snapshot_id,
                            table.c.deleted_at.is_(None),
                            table.c.summary["owner_uid"].astext.in_(tuple(sorted(frontier))),
                        )
                        .order_by(
                            table.c.kind,
                            table.c.namespace.nullsfirst(),
                            table.c.name,
                            table.c.inventory_key,
                        )
                        .limit(remaining + 1)
                    )
                    .mappings()
                    .all()
                )
                if len(rows) > remaining:
                    rows = rows[:remaining]
                    truncated = True
                next_frontier: set[str] = set()
                for row in rows:
                    item = self.serialize_inventory_resource(dict(row))
                    summary = item.get("summary")
                    if (
                        not isinstance(summary, Mapping)
                        or summary.get("owner_references_complete") is not True
                    ):
                        resources_complete = False
                        continue
                    uid = str(item.get("uid") or "")
                    if not uid or uid in visited:
                        resources_complete = False
                        continue
                    visited.add(uid)
                    next_frontier.add(uid)
                    dependents.append(item)
                if truncated:
                    break
                frontier = next_frontier
        return {
            "snapshot_id": snapshot_id,
            "resources_complete": resources_complete,
            "truncated": truncated,
            "dependents": dependents,
        }

    def list_related_inventory_resources(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        resource: JsonObject,
        limit: int = 100,
    ) -> dict[str, list[JsonObject]]:
        """실제 inventory 필드로 계산한 1-hop 관계.

        - node -> scheduled pods(summary.node_name)
        - service -> selector 와 pod labels 매칭
        - workload -> selector 또는 pod owner 매칭
        """
        resource_type = str(resource.get("resource_type") or "").lower()
        namespace = resource.get("namespace")
        name = str(resource.get("name") or "")
        summary = dict(resource.get("summary") or {})
        related: dict[str, list[JsonObject]] = {}

        if resource_type == NODE_RESOURCE_TYPE:
            pods = self.list_inventory_resources(
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                resource_type=POD_RESOURCE_TYPE,
                include_deleted=False,
                limit=1000,
            )
            related["pods"] = [
                pod for pod in pods if dict(pod.get("summary") or {}).get("node_name") == name
            ][: max(1, min(limit, 1000))]
            return related

        if resource_type == "service":
            selector = selector_labels(summary.get("selector"))
            if selector:
                pods = self.list_inventory_resources(
                    workspace_id=workspace_id,
                    cluster_id=cluster_id,
                    resource_type=POD_RESOURCE_TYPE,
                    namespace=str(namespace) if namespace is not None else None,
                    include_deleted=False,
                    limit=1000,
                )
                related["pods"] = [
                    pod for pod in pods if labels_match(selector, pod_summary_labels(pod))
                ][: max(1, min(limit, 1000))]
            return related

        if resource_type == WORKLOAD_RESOURCE_TYPE:
            selector = selector_labels(summary.get("selector"))
            kind = str(resource.get("kind") or "")
            pods = self.list_inventory_resources(
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                resource_type=POD_RESOURCE_TYPE,
                namespace=str(namespace) if namespace is not None else None,
                include_deleted=False,
                limit=1000,
            )
            related["pods"] = [
                pod
                for pod in pods
                if (selector and labels_match(selector, pod_summary_labels(pod)))
                or pod_owner_matches(pod, kind=kind, name=name)
            ][: max(1, min(limit, 1000))]
            return related

        return related

    def list_scheduled_run_inventory(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        namespace: str,
        owner_kind: str,
        owner_name: str,
        owner_uid: str,
        run_kinds: Sequence[str],
        limit: int = 100,
        pod_limit: int = 1000,
    ) -> JsonObject:
        """Read retained scheduled runs and their Pods with two bounded queries.

        Owner UID is mandatory so a recreated CronJob-like object cannot inherit
        runs from an older object with the same name.  Pods are fetched in one
        batch for all returned runs; no per-run query is issued.
        """

        effective_limit = max(1, min(limit, 100))
        effective_pod_limit = max(1, min(pod_limit, 1000))
        normalized_run_kinds = tuple(sorted({kind for kind in run_kinds if kind}))
        if not owner_uid or not normalized_run_kinds:
            return {
                "runs": [],
                "pods": [],
                "runs_truncated": False,
                "pods_truncated": False,
                "partial_reason_codes": [],
            }

        table = ClusterInventoryResourceRecord.__table__
        snapshot_table = ClusterInventorySnapshotRecord.__table__
        latest_snapshot_id = _latest_inventory_snapshot_id_scalar(workspace_id, cluster_id)
        snapshot_summary_statement = select(snapshot_table.c.summary).where(
            snapshot_table.c.workspace_id == workspace_id,
            snapshot_table.c.cluster_id == cluster_id,
            snapshot_table.c.snapshot_id == latest_snapshot_id,
        )
        runs_statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.namespace == namespace,
                table.c.resource_type == WORKLOAD_RESOURCE_TYPE,
                table.c.kind.in_(normalized_run_kinds),
                table.c.deleted_at.is_(None),
                table.c.snapshot_id == latest_snapshot_id,
                table.c.summary["owner_uid"].astext == owner_uid,
                table.c.summary["owner_kind"].astext == owner_kind,
                table.c.summary["owner_name"].astext == owner_name,
            )
            .order_by(
                table.c.summary["creation_timestamp"].astext.desc().nullslast(),
                table.c.last_seen_at.desc(),
                table.c.inventory_key.desc(),
            )
            .limit(effective_limit + 1)
        )
        with self.connection() as conn:
            snapshot_summary = conn.execute(snapshot_summary_statement).scalar_one_or_none()
            run_rows = [dict(row) for row in conn.execute(runs_statement).mappings().all()]
            selected_runs = run_rows[:effective_limit]
            run_owners = [
                and_(
                    table.c.summary["owner_uid"].astext == str(row["uid"]),
                    table.c.summary["owner_kind"].astext == str(row["kind"]),
                    table.c.summary["owner_name"].astext == str(row["name"]),
                )
                for row in selected_runs
                if row.get("uid")
            ]
            pod_rows: list[JsonObject] = []
            if run_owners:
                pods_statement = (
                    select(table)
                    .where(
                        table.c.workspace_id == workspace_id,
                        table.c.cluster_id == cluster_id,
                        table.c.namespace == namespace,
                        table.c.resource_type == POD_RESOURCE_TYPE,
                        table.c.deleted_at.is_(None),
                        table.c.snapshot_id == latest_snapshot_id,
                        or_(*run_owners),
                    )
                    .order_by(table.c.last_seen_at.desc(), table.c.inventory_key.desc())
                    .limit(effective_pod_limit + 1)
                )
                pod_rows = [dict(row) for row in conn.execute(pods_statement).mappings().all()]
        return {
            "runs": [self.serialize_inventory_resource(row) for row in selected_runs],
            "pods": [
                self.serialize_inventory_resource(row) for row in pod_rows[:effective_pod_limit]
            ],
            "runs_truncated": len(run_rows) > effective_limit,
            "pods_truncated": len(pod_rows) > effective_pod_limit,
            "partial_reason_codes": inventory_snapshot_partial_reason_codes(snapshot_summary),
        }

    def list_resource_events(
        self,
        *,
        workspace_id: str,
        cluster_id: str,
        resource: JsonObject,
        limit: int = 50,
    ) -> list[JsonObject]:
        """Kubernetes Event 의 involvedObject 기준으로 단일 리소스 이벤트만 반환."""
        table = ClusterInventoryResourceRecord.__table__
        kind = str(resource.get("kind") or "")
        name = str(resource.get("name") or "")
        uid = resource.get("uid")
        summary_filters = [table.c.summary.contains({"involved_kind": kind, "involved_name": name})]
        if uid:
            summary_filters.append(table.c.summary.contains({"involved_uid": str(uid)}))
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.resource_type == EVENT_RESOURCE_TYPE,
                table.c.deleted_at.is_(None),
                or_(*summary_filters),
            )
            .order_by(table.c.observed_at.desc())
            .limit(max(1, min(limit, 200)))
        )
        namespace = resource.get("namespace")
        if namespace is not None:
            statement = statement.where(table.c.namespace == namespace)
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        matched = [
            self.serialize_inventory_resource(dict(row))
            for row in rows
            if event_involves_resource(dict(row), resource)
        ]
        return matched[: max(1, min(limit, 200))]

    def get_actual_resource_image(
        self,
        workspace_id: str,
        cluster_id: str,
        namespace: str | None,
        resource: str,
    ) -> str | None:
        """diff-worker actual-state 조회 — 최신 inventory 리소스에서 컨테이너 이미지 추출.

        resource 는 gitops resource_ref 형식("kind/name", kind 는 소문자)이다.
        스냅샷이 없거나 이미지가 없으면 None — 호출부(diff-worker)가 "unknown" 처리.
        """
        kind, _, name = str(resource).partition("/")
        if not kind or not name:
            return None
        table = ClusterInventoryResourceRecord.__table__
        statement = (
            select(table.c.summary, table.c.raw)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                func.lower(table.c.kind) == kind.strip().lower(),
                table.c.name == name,
                table.c.deleted_at.is_(None),
            )
            .order_by(table.c.last_seen_at.desc())
            .limit(1)
        )
        if namespace:
            statement = statement.where(table.c.namespace == namespace)
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        if row is None:
            return None
        return first_container_image(
            dict(row["raw"] or {}),
            dict(row["summary"] or {}),
        )

    def get_actual_resource_manifest(
        self,
        workspace_id: str,
        cluster_id: str,
        namespace: str | None,
        resource: str,
    ) -> JsonObject | None:
        """연결 프리뷰용 live 관측 조회 — 최신 inventory 리소스의 관측 요약(raw)과
        메타를 반환한다. resource 는 gitops resource_ref("kind/name", kind 소문자).

        재구성(live 매니페스트 조립)은 호출부(gitops 프리뷰 서비스)가 담당해 도메인
        경계를 지킨다. 관측이 없으면 None.
        """
        kind, _, name = str(resource).partition("/")
        if not kind or not name:
            return None
        table = ClusterInventoryResourceRecord.__table__
        statement = (
            select(
                table.c.kind,
                table.c.name,
                table.c.namespace,
                table.c.api_version,
                table.c.resource_version,
                table.c.raw,
                table.c.summary,
                table.c.observed_at,
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                func.lower(table.c.kind) == kind.strip().lower(),
                table.c.name == name,
                table.c.deleted_at.is_(None),
            )
            .order_by(table.c.last_seen_at.desc())
            .limit(1)
        )
        if namespace:
            statement = statement.where(table.c.namespace == namespace)
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        if row is None:
            return None
        return {
            "kind": row["kind"],
            "name": row["name"],
            "namespace": row["namespace"],
            "api_version": row["api_version"],
            "resource_version": row["resource_version"],
            "raw": dict(row["raw"] or {}),
            "summary": dict(row["summary"] or {}),
        }

    def list_cluster_usage_samples(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        limit: int = 288,
    ) -> list[JsonObject]:
        """실측 usage 롤업 시계열 — 최신 limit 개를 시간 오름차순으로 반환(차트용)."""
        table = ClusterUsageSampleRecord.__table__
        newest_first = (
            select(table.c.sampled_at, table.c.usage)
            .where(table.c.workspace_id == workspace_id, table.c.cluster_id == cluster_id)
            .order_by(table.c.sampled_at.desc())
            .limit(max(1, min(limit, 2000)))
            .subquery()
        )
        statement = select(newest_first).order_by(newest_first.c.sampled_at.asc())
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [
            {"sampled_at": iso_or_none(row["sampled_at"]), "usage": dict(row["usage"] or {})}
            for row in rows
        ]

    def _snapshot_cluster_ids(self, workspace_id: str) -> set[str]:
        """허용 집합이 None(전체)일 때 LATERAL 대상이 될 snapshot 보유 클러스터 집합."""
        snapshots = ClusterInventorySnapshotRecord.__table__
        statement = (
            select(snapshots.c.cluster_id)
            .where(snapshots.c.workspace_id == workspace_id)
            .distinct()
        )
        with self.connection() as conn:
            return {str(row[0]) for row in conn.execute(statement)}

    def fleet_inventory_rollup(
        self,
        workspace_id: str,
        cluster_ids: set[str] | None = None,
    ) -> dict[str, JsonObject]:
        """fleet 화면용 클러스터별 pod/node/workload 상태 롤업(1 쿼리, GROUP BY).

        cluster_ids 는 None(전체 허용) 또는 허용 집합 — 빈 집합이면 즉시 {} (권한 0).
        반환: {cluster_id: {pods_running, pods_total, nodes_ready, nodes_total,
        workloads_degraded, workloads_total, last_seen_at}}.
        """
        resolved_cluster_ids = (
            cluster_ids if cluster_ids is not None else self._snapshot_cluster_ids(workspace_id)
        )
        if not resolved_cluster_ids:
            return {}
        table = ClusterInventoryResourceRecord.__table__
        # 상관 서브쿼리(리소스 행마다 최신 snapshot 재조회)는 snapshot 이력이 쌓이면
        # statement_timeout(30s)을 초과해 fleet 화면 전체가 500 으로 떨어졌다.
        # /clusters 목록과 같은 VALUES+LATERAL 최신-snapshot 1회 조회로 조인한다.
        latest = _latest_inventory_snapshots_statement(
            workspace_id,
            resolved_cluster_ids,
        ).subquery("fleet_rollup_latest_snapshots")
        statement = (
            select(
                table.c.cluster_id,
                table.c.resource_type,
                table.c.status,
                table.c.health,
                func.count().label("count"),
                func.max(table.c.last_seen_at).label("last_seen_at"),
            )
            .select_from(
                table.join(
                    latest,
                    and_(
                        latest.c.workspace_id == table.c.workspace_id,
                        latest.c.cluster_id == table.c.cluster_id,
                        latest.c.snapshot_id == table.c.snapshot_id,
                    ),
                )
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.resource_type.in_(FLEET_ROLLUP_RESOURCE_TYPES),
                table.c.deleted_at.is_(None),
            )
            .group_by(table.c.cluster_id, table.c.resource_type, table.c.status, table.c.health)
        )
        statement = statement.where(table.c.cluster_id.in_(sorted(resolved_cluster_ids)))
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        rollup: dict[str, JsonObject] = {}
        for row in rows:
            entry = rollup.setdefault(
                str(row["cluster_id"]),
                {
                    "pods_running": 0,
                    "pods_total": 0,
                    "nodes_ready": 0,
                    "nodes_total": 0,
                    "workloads_degraded": 0,
                    "workloads_total": 0,
                    "last_seen_at": None,
                },
            )
            count = int(row["count"])
            resource_type = row["resource_type"]
            if resource_type == POD_RESOURCE_TYPE:
                entry["pods_total"] += count
                if row["status"] == POD_RUNNING_STATUS:
                    entry["pods_running"] += count
            elif resource_type == NODE_RESOURCE_TYPE:
                entry["nodes_total"] += count
                if row["status"] == NODE_READY_STATUS:
                    entry["nodes_ready"] += count
            elif resource_type == WORKLOAD_RESOURCE_TYPE:
                entry["workloads_total"] += count
                if row["health"] == DEGRADED_HEALTH:
                    entry["workloads_degraded"] += count
            seen = iso_or_none(row["last_seen_at"])
            if seen is not None and (entry["last_seen_at"] is None or seen > entry["last_seen_at"]):
                entry["last_seen_at"] = seen
        return rollup

    def fleet_inventory_nodes(
        self,
        workspace_id: str,
        cluster_ids: set[str] | None = None,
    ) -> dict[str, list[JsonObject]]:
        """Return node summaries from each cluster's latest committed inventory cut."""

        resolved_cluster_ids = (
            cluster_ids if cluster_ids is not None else self._snapshot_cluster_ids(workspace_id)
        )
        if not resolved_cluster_ids:
            return {}
        table = ClusterInventoryResourceRecord.__table__
        # 상관 서브쿼리 → VALUES+LATERAL 조인(fleet_inventory_rollup 과 동일한 이유·
        # 동일한 "최신 snapshot" 정의). /clusters·counts 경로와 한 기준을 공유한다.
        latest = _latest_inventory_snapshots_statement(
            workspace_id,
            resolved_cluster_ids,
        ).subquery("fleet_nodes_latest_snapshots")
        statement = (
            select(table.c.cluster_id, table.c.summary)
            .select_from(
                table.join(
                    latest,
                    and_(
                        latest.c.workspace_id == table.c.workspace_id,
                        latest.c.cluster_id == table.c.cluster_id,
                        latest.c.snapshot_id == table.c.snapshot_id,
                    ),
                )
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.resource_type == NODE_RESOURCE_TYPE,
                table.c.deleted_at.is_(None),
                table.c.cluster_id.in_(sorted(resolved_cluster_ids)),
            )
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        result: dict[str, list[JsonObject]] = {}
        for row in rows:
            summary = row.get("summary")
            if not isinstance(summary, dict):
                continue
            # Keep the metric observation proof adjacent to the fallback value. Consumers
            # must fail closed when this timestamp is absent or outside their freshness
            # policy; the inventory resource's generic observed_at is not metric evidence.
            result.setdefault(str(row["cluster_id"]), []).append(
                {
                    "summary": dict(summary),
                    "metrics_observed_at": summary.get("metrics_observed_at"),
                }
            )
        return result

    def latest_cluster_usage_rollups(
        self,
        workspace_id: str,
        cluster_ids: set[str] | None = None,
        *,
        samples_per_cluster: int = 2,
    ) -> dict[str, list[JsonObject]]:
        """클러스터별 최신 usage 샘플 N개(시간 오름차순) — restarts_recent 델타 계산용.

        빈 허용 집합이면 즉시 {}. window function(row_number)으로 클러스터당 최신 N개만 취함.
        """
        resolved_cluster_ids = (
            cluster_ids if cluster_ids is not None else self._snapshot_cluster_ids(workspace_id)
        )
        if not resolved_cluster_ids:
            return {}
        table = ClusterUsageSampleRecord.__table__
        # window rank 는 워크스페이스의 usage 샘플 전체(8~30초 간격 append-only)를
        # 훑은 뒤에야 rank 1..N 을 남긴다 — 하루만 지나도 statement_timeout 위험.
        # 클러스터별 VALUES+LATERAL 역방향 프로브(ix_cluster_usage_samples_scope)로
        # 최신 N개만 정확히 읽는다.
        bounded_samples = max(1, min(samples_per_cluster, 10))
        requested = (
            values(column("cluster_id", Text), name="requested_usage_clusters")
            .data([(cluster_id,) for cluster_id in sorted(set(resolved_cluster_ids))])
            .alias("requested_usage_clusters")
        )
        latest = (
            select(table.c.cluster_id, table.c.sampled_at, table.c.usage)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == requested.c.cluster_id,
            )
            .order_by(table.c.sampled_at.desc())
            .limit(bounded_samples)
            .lateral("latest_usage_samples")
        )
        statement = (
            select(latest.c.cluster_id, latest.c.sampled_at, latest.c.usage)
            .select_from(requested.join(latest, true()))
            .order_by(latest.c.cluster_id, latest.c.sampled_at.asc())
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        samples: dict[str, list[JsonObject]] = {}
        for row in rows:
            samples.setdefault(str(row["cluster_id"]), []).append(
                {"sampled_at": iso_or_none(row["sampled_at"]), "usage": dict(row["usage"] or {})}
            )
        return samples

    def list_recent_warning_events(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        limit: int = 10,
    ) -> list[JsonObject]:
        """드릴다운용 현재 경고 이벤트 — 최신 snapshot의 event-time 최신순."""
        table = ClusterInventoryResourceRecord.__table__
        snapshots = ClusterInventorySnapshotRecord.__table__
        latest_snapshot_id = (
            select(snapshots.c.snapshot_id)
            .where(
                snapshots.c.workspace_id == workspace_id,
                snapshots.c.cluster_id == cluster_id,
                snapshots.c.status != "ignored_stale",
                live_inventory_snapshot_clause(snapshots),
            )
            .order_by(snapshots.c.collected_at.desc(), snapshots.c.created_at.desc())
            .limit(1)
            .scalar_subquery()
        )
        statement = (
            select(table)
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.resource_type == EVENT_RESOURCE_TYPE,
                table.c.health == DEGRADED_HEALTH,
                table.c.deleted_at.is_(None),
                table.c.snapshot_id == latest_snapshot_id,
            )
            .order_by(table.c.observed_at.desc())
            .limit(max(1, min(limit, 100)))
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [self.serialize_inventory_resource(dict(row)) for row in rows]

    def latest_inventory_snapshot(self, workspace_id: str, cluster_id: str) -> JsonObject | None:
        table = ClusterInventorySnapshotRecord.__table__
        statement = (
            select(
                table.c.snapshot_id,
                table.c.workspace_id,
                table.c.cluster_id,
                table.c.agent_id,
                table.c.source,
                table.c.status,
                table.c.collected_at,
                table.c.resource_count,
                table.c.summary,
                table.c.created_at,
            )
            .where(
                table.c.workspace_id == workspace_id,
                table.c.cluster_id == cluster_id,
                table.c.status != "ignored_stale",
                live_inventory_snapshot_clause(table),
            )
            .order_by(table.c.created_at.desc(), table.c.snapshot_id.desc())
            .limit(1)
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        return self.serialize_inventory_snapshot(dict(row)) if row else None

    def node_summary_read_model(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        limit: int = 1000,
    ) -> JsonObject | None:
        """Return the compact first-paint node projection in one database round trip.

        ``latest_inventory_snapshot`` contains every per-Pod metric and can exceed
        hundreds of KiB.  The node rail only needs the accepted snapshot's small
        ``summary.nodes``/``usage.nodes`` fragments plus the Node rows.  Projecting
        those JSONB paths in PostgreSQL avoids decoding the full snapshot or any raw
        Pod manifests while preserving the exact observation and freshness evidence.
        """

        resources = ClusterInventoryResourceRecord.__table__
        snapshots = ClusterInventorySnapshotRecord.__table__
        latest = (
            select(
                snapshots.c.snapshot_id,
                snapshots.c.collected_at,
                func.jsonb_build_object(
                    "live_inventory",
                    snapshots.c.summary["summary"]["live_inventory"],
                    "nodes",
                    snapshots.c.summary["summary"]["nodes"],
                ).label("inventory_summary"),
                snapshots.c.summary["usage"]["nodes"].label("node_usage"),
            )
            .where(
                snapshots.c.workspace_id == workspace_id,
                snapshots.c.cluster_id == cluster_id,
                snapshots.c.status != "ignored_stale",
                live_inventory_snapshot_clause(snapshots),
            )
            .order_by(snapshots.c.created_at.desc(), snapshots.c.snapshot_id.desc())
            .limit(1)
            .subquery("latest_node_summary_snapshot")
        )
        # 최신 snapshot 조회를 두 번 렌더하지 않도록(계약: snapshots FROM 1회),
        # Node 행은 latest 에 상관된 LATERAL 로 같은 snapshot_id 를 재사용한다.
        node_rows = (
            select(
                resources.c.name,
                resources.c.status,
                resources.c.health,
                resources.c.summary,
            )
            .where(
                resources.c.workspace_id == workspace_id,
                resources.c.cluster_id == cluster_id,
                resources.c.resource_type == NODE_RESOURCE_TYPE,
                resources.c.deleted_at.is_(None),
                resources.c.snapshot_id == latest.c.snapshot_id,
            )
            .order_by(resources.c.name)
            .limit(max(1, min(limit, 1000)))
            .lateral("node_summary_resources")
        )
        node_json = func.jsonb_build_object(
            "name",
            node_rows.c.name,
            "status",
            node_rows.c.status,
            "health",
            node_rows.c.health,
            "summary",
            node_rows.c.summary,
        )
        nodes = func.coalesce(
            func.jsonb_agg(node_json).filter(node_rows.c.name.is_not(None)),
            func.jsonb_build_array(),
        ).label("nodes")
        statement = (
            select(
                latest.c.snapshot_id,
                latest.c.collected_at,
                latest.c.inventory_summary,
                latest.c.node_usage,
                nodes,
            )
            .select_from(latest.outerjoin(node_rows, true()))
            .group_by(
                latest.c.snapshot_id,
                latest.c.collected_at,
                latest.c.inventory_summary,
                latest.c.node_usage,
            )
        )
        with self.connection() as conn:
            row = conn.execute(statement).mappings().first()
        if row is None:
            return None

        inventory_summary = row.get("inventory_summary")
        node_usage = row.get("node_usage")
        raw_nodes = row.get("nodes")
        compact_nodes = [
            dict(node)
            for node in (raw_nodes if isinstance(raw_nodes, list) else [])
            if isinstance(node, dict) and node.get("name")
        ]
        compact_nodes.sort(key=lambda node: str(node.get("name") or ""))
        return {
            "snapshot_id": str(row["snapshot_id"]),
            "collected_at": iso_or_none(row["collected_at"]),
            "nodes": compact_nodes,
            "snapshot": {
                "summary": {
                    "usage": {"nodes": dict(node_usage or {})},
                    "summary": dict(inventory_summary or {}),
                }
            },
        }

    def latest_inventory_snapshots(
        self,
        workspace_id: str,
        cluster_ids: set[str],
    ) -> dict[str, JsonObject]:
        """클러스터별 최신 snapshot을 인덱스 기반 lateral lookup 한 번으로 반환한다."""
        if not cluster_ids:
            return {}
        statement = _latest_inventory_snapshots_statement(workspace_id, cluster_ids)
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return {
            str(row["cluster_id"]): self.serialize_inventory_snapshot(dict(row)) for row in rows
        }

    def inventory_resource_counts(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        namespaces: tuple[str, ...] = (),
    ) -> list[JsonObject]:
        table = ClusterInventoryResourceRecord.__table__
        predicates = [
            table.c.workspace_id == workspace_id,
            table.c.cluster_id == cluster_id,
            table.c.deleted_at.is_(None),
            table.c.snapshot_id
            == _latest_inventory_snapshot_id_scalar(workspace_id, cluster_id),
        ]
        if namespaces:
            predicates.append(or_(table.c.namespace.is_(None), table.c.namespace.in_(namespaces)))
        statement = (
            select(
                table.c.resource_type,
                table.c.health,
                func.count().label("count"),
            )
            .where(*predicates)
            .group_by(table.c.resource_type, table.c.health)
            .order_by(table.c.resource_type, table.c.health)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return [
            {
                "resource_type": row["resource_type"],
                "health": row["health"],
                "count": int(row["count"]),
            }
            for row in rows
        ]

    def inventory_product_resource_counts(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        namespaces: tuple[str, ...] = (),
    ) -> list[JsonObject]:
        """Return product resource counts from one grouped database query."""

        table = ClusterInventoryResourceRecord.__table__
        predicates = [
            table.c.workspace_id == workspace_id,
            table.c.cluster_id == cluster_id,
            table.c.deleted_at.is_(None),
            table.c.snapshot_id
            == _latest_inventory_snapshot_id_scalar(workspace_id, cluster_id),
        ]
        if namespaces:
            predicates.append(or_(table.c.namespace.is_(None), table.c.namespace.in_(namespaces)))
        statement = (
            select(
                table.c.resource_type,
                table.c.kind,
                table.c.health,
                func.count().label("count"),
            )
            .where(*predicates)
            .group_by(table.c.resource_type, table.c.kind, table.c.health)
            .order_by(table.c.resource_type, table.c.kind, table.c.health)
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()
        return project_inventory_product_counts(rows)

    def inventory_namespace_resource_counts(
        self,
        workspace_id: str,
        cluster_id: str,
        *,
        namespaces: tuple[str, ...] = (),
    ) -> list[JsonObject]:
        """Return exact namespaced product counts without a browser-side regroup."""

        table = ClusterInventoryResourceRecord.__table__
        predicates = [
            table.c.workspace_id == workspace_id,
            table.c.cluster_id == cluster_id,
            table.c.namespace.is_not(None),
            table.c.deleted_at.is_(None),
            table.c.snapshot_id
            == _latest_inventory_snapshot_id_scalar(workspace_id, cluster_id),
        ]
        if namespaces:
            predicates.append(table.c.namespace.in_(namespaces))
        statement = (
            select(
                table.c.namespace,
                table.c.resource_type,
                table.c.kind,
                table.c.health,
                func.count().label("count"),
            )
            .where(*predicates)
            .group_by(
                table.c.namespace,
                table.c.resource_type,
                table.c.kind,
                table.c.health,
            )
            .order_by(
                table.c.namespace,
                table.c.resource_type,
                table.c.kind,
                table.c.health,
            )
        )
        with self.connection() as conn:
            rows = conn.execute(statement).mappings().all()

        by_namespace: dict[str, list[JsonObject]] = {}
        for row in rows:
            namespace = str(row["namespace"])
            by_namespace.setdefault(namespace, []).append(dict(row))
        result = []
        for namespace, namespace_rows in sorted(by_namespace.items()):
            counts = project_inventory_product_counts(namespace_rows)
            result.append(
                {
                    "namespace": namespace,
                    "total": sum(int(item["count"]) for item in counts),
                    "counts": counts,
                }
            )
        return result

    def inventory_resource_counts_by_cluster(
        self,
        workspace_id: str,
        cluster_ids: set[str],
    ) -> dict[str, list[JsonObject]]:
        """Return exact live resource counts for every requested cluster in one query."""

        if not cluster_ids:
            return {}
        statement = _inventory_resource_counts_by_cluster_statement(workspace_id, cluster_ids)
        counts: dict[str, list[JsonObject]] = {cluster_id: [] for cluster_id in cluster_ids}
        with self.connection() as conn:
            for row in conn.execute(statement).mappings():
                counts[str(row["cluster_id"])].append(
                    {
                        "resource_type": row["resource_type"],
                        "health": row["health"],
                        "count": int(row["count"]),
                    }
                )
        return counts

    def serialize_inventory_snapshot(self, row: JsonObject) -> JsonObject:
        item = dict(row)
        item["collected_at"] = iso_or_none(item.get("collected_at"))
        item["created_at"] = iso_or_none(item.get("created_at"))
        return item

    def serialize_inventory_resource(self, row: JsonObject) -> JsonObject:
        item = dict(row)
        item["observed_at"] = iso_or_none(item.get("observed_at"))
        item["first_seen_at"] = iso_or_none(item.get("first_seen_at"))
        item["last_seen_at"] = iso_or_none(item.get("last_seen_at"))
        item["deleted_at"] = iso_or_none(item.get("deleted_at"))
        item["created_at"] = iso_or_none(item.get("created_at"))
        item["updated_at"] = iso_or_none(item.get("updated_at"))
        return item


def selector_labels(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    labels = value.get("matchLabels") if isinstance(value.get("matchLabels"), dict) else value
    return {str(key): str(val) for key, val in labels.items() if isinstance(key, str)}


def pod_summary_labels(pod: JsonObject) -> dict[str, str]:
    summary = pod.get("summary") if isinstance(pod.get("summary"), dict) else {}
    labels = summary.get("labels") if isinstance(summary.get("labels"), dict) else {}
    return {str(key): str(val) for key, val in labels.items() if isinstance(key, str)}


def labels_match(selector: dict[str, str], labels: dict[str, str]) -> bool:
    return bool(selector) and all(labels.get(key) == value for key, value in selector.items())


def pod_owner_matches(pod: JsonObject, *, kind: str, name: str) -> bool:
    summary = pod.get("summary") if isinstance(pod.get("summary"), dict) else {}
    owner_kind = str(summary.get("owner_kind") or "")
    owner_name = str(summary.get("owner_name") or "")
    return owner_kind.lower() == kind.lower() and owner_name == name


def inventory_snapshot_partial_reason_codes(value: Any) -> tuple[str, ...]:
    envelope = value if isinstance(value, Mapping) else {}
    summary = envelope.get("summary") if isinstance(envelope.get("summary"), Mapping) else envelope
    if not isinstance(summary, Mapping):
        return ("source_resources_incomplete",)
    limits = (
        summary.get("collection_limits")
        if isinstance(summary.get("collection_limits"), Mapping)
        else {}
    )
    if limits.get("truncated") is True:
        return ("source_resources_truncated",)
    if summary.get("resources_complete") is not True:
        return ("source_resources_incomplete",)
    return ()


def event_involves_resource(event: JsonObject, resource: JsonObject) -> bool:
    summary = event.get("summary") if isinstance(event.get("summary"), dict) else {}
    involved_kind = str(summary.get("involved_kind") or "")
    involved_name = str(summary.get("involved_name") or "")
    involved_uid = summary.get("involved_uid")
    resource_kind = str(resource.get("kind") or "")
    resource_name = str(resource.get("name") or "")
    resource_uid = resource.get("uid")
    if resource_uid and involved_uid and str(involved_uid) == str(resource_uid):
        return True
    return involved_kind.lower() == resource_kind.lower() and involved_name == resource_name
