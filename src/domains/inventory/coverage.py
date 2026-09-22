"""수집 범위 증명 — 부분 관측 스냅샷이 삭제를 추론해도 되는 범위를 고른다.

cluster-agent 는 전체 클러스터를 증명하지 않고 namespace 단위 cut 만 수집할 수 있다.
그래서 "이번 스냅샷에 없다"는 "지워졌다"가 아니다. 삭제 권한은 끝까지 관측된
(`observed`·`complete`·`delete_safe`, 잘리지 않았고 label selector 로 좁히지 않은)
collection 에서만 나온다. Event 는 보존 기간 때문에 절대 삭제 권한을 얻지 못한다.

생산자 주의: 이 모듈은 `collection_coverage` 항목을 **소비**만 한다. 항목을 만드는
코드는 이 저장소에 없고(에이전트가 아직 내보내지 않는다) 없으면 삭제 범위가 빈
튜플이 되어 아무것도 지우지 않는 쪽으로 닫힌다. docs/design.md 의 "알려진 한계" 참고.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import and_, false, or_

COLLECTION_COVERAGE_SUMMARY_KEY = "collection_coverage"

POD_COLLECTION = "pods"
NODE_COLLECTION = "nodes"
WORKLOAD_COLLECTION = "workloads"
WORKLOAD_REVISION_COLLECTION = "workload_revisions"
SERVICE_COLLECTION = "services"
INGRESS_COLLECTION = "ingresses"
RESOURCE_QUOTA_COLLECTION = "resourcequotas"
ENDPOINT_COLLECTION = "endpoints"
EVENT_COLLECTION = "events"
CUSTOM_RESOURCE_COLLECTION = "custom_resources"

RESOURCE_TYPES_BY_COLLECTION: dict[str, tuple[str, ...]] = {
    POD_COLLECTION: ("pod",),
    NODE_COLLECTION: ("node",),
    WORKLOAD_COLLECTION: ("workload",),
    WORKLOAD_REVISION_COLLECTION: ("workload_revision",),
    SERVICE_COLLECTION: ("service",),
    INGRESS_COLLECTION: ("ingress",),
    RESOURCE_QUOTA_COLLECTION: ("resourcequota",),
    ENDPOINT_COLLECTION: ("endpoint",),
    EVENT_COLLECTION: ("event",),
    CUSTOM_RESOURCE_COLLECTION: ("custom_resource",),
}

NAMESPACED_COLLECTIONS = (
    POD_COLLECTION,
    WORKLOAD_COLLECTION,
    WORKLOAD_REVISION_COLLECTION,
    SERVICE_COLLECTION,
    INGRESS_COLLECTION,
    RESOURCE_QUOTA_COLLECTION,
    ENDPOINT_COLLECTION,
    EVENT_COLLECTION,
)
CLUSTER_COLLECTIONS = (NODE_COLLECTION,)

DELETE_SAFE_COLLECTIONS = frozenset(
    (
        POD_COLLECTION,
        NODE_COLLECTION,
        WORKLOAD_COLLECTION,
        WORKLOAD_REVISION_COLLECTION,
        SERVICE_COLLECTION,
        INGRESS_COLLECTION,
        RESOURCE_QUOTA_COLLECTION,
        ENDPOINT_COLLECTION,
    )
)


@dataclass(frozen=True)
class InventoryDeleteScope:
    resource_type: str
    namespace: str | None


def inventory_deletion_scopes(source_summary: Mapping[str, Any]) -> tuple[InventoryDeleteScope, ...]:
    """Return resource scopes where absence in the latest snapshot proves deletion."""

    if source_summary.get("live_inventory") is not True:
        return ()
    raw_entries = source_summary.get(COLLECTION_COVERAGE_SUMMARY_KEY)
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, str | bytes):
        return ()

    scopes: set[InventoryDeleteScope] = set()
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, Mapping):
            continue
        collection = _text(raw_entry.get("collection"))
        resource_types = RESOURCE_TYPES_BY_COLLECTION.get(collection)
        if collection not in DELETE_SAFE_COLLECTIONS or not resource_types:
            continue
        if not _delete_authoritative_entry(raw_entry):
            continue
        scope = _text(raw_entry.get("scope"))
        namespace = _text(raw_entry.get("namespace")) or None
        if collection in NAMESPACED_COLLECTIONS and scope == "namespace" and namespace:
            for resource_type in resource_types:
                scopes.add(InventoryDeleteScope(resource_type, namespace))
        elif collection in CLUSTER_COLLECTIONS and scope == "cluster":
            for resource_type in resource_types:
                scopes.add(InventoryDeleteScope(resource_type, None))
    return tuple(sorted(scopes, key=lambda item: (item.resource_type, item.namespace or "")))


def inventory_row_in_deletion_scopes(
    row: Mapping[str, Any],
    scopes: Sequence[InventoryDeleteScope],
) -> bool:
    resource_type = _text(row.get("resource_type"))
    namespace = row.get("namespace")
    normalized_namespace = str(namespace) if namespace is not None else None
    return any(
        scope.resource_type == resource_type and scope.namespace == normalized_namespace
        for scope in scopes
    )


def inventory_delete_scope_predicate(table: Any, scopes: Sequence[InventoryDeleteScope]) -> Any:
    clauses = []
    for scope in scopes:
        clause = table.c.resource_type == scope.resource_type
        if scope.namespace is None:
            clause = and_(clause, table.c.namespace.is_(None))
        else:
            clause = and_(clause, table.c.namespace == scope.namespace)
        clauses.append(clause)
    return or_(*clauses) if clauses else false()


def _reason_codes(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return ()
    return tuple(reason.strip() for reason in value if isinstance(reason, str) and reason.strip())


def _delete_authoritative_entry(entry: Mapping[str, Any]) -> bool:
    return (
        entry.get("complete") is True
        and entry.get("delete_safe") is True
        and entry.get("observed") is True
        and entry.get("truncated") is not True
        and not _text(entry.get("label_selector"))
        and not _reason_codes(entry.get("reason_codes"))
    )


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""
