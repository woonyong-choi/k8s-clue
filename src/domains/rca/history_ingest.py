"""Reuse an agent evidence Kubernetes payload as an inventory snapshot for the ledger.

The payload is a scoped incident collection, never fleet truth.  The mapping
therefore declares it incomplete (``live_inventory`` false, no replacement, no
``collection_coverage``), so downstream code can record updates and complete
Event facts but can never infer an addition or a deletion from it.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from domains.inventory.kubernetes_events import (
    EVENT_CAPTURE_SUMMARY_KEY,
    EVENT_FACTS_SUMMARY_KEY,
    parse_timestamp,
)
from packages.contracts.event_bus.interfaces import JsonObject

POD_SUMMARY_FIELDS = ("phase", "reason", "restart_total", "owner_kind", "owner_name", "owner_uid")
WORKLOAD_SUMMARY_FIELDS = (
    "generation",
    "desired_replicas",
    "ready_replicas",
    "available_replicas",
    "updated_replicas",
    "unavailable_replicas",
    "owner_kind",
    "owner_name",
)
EVIDENCE_SNAPSHOT_SOURCE = "agent-evidence"


def evidence_inventory_payload(kubernetes: object) -> JsonObject | None:
    """Return an inventory snapshot payload, or ``None`` when nothing safe can be recorded.

    A payload without a valid collection time is skipped instead of being stamped with the
    server clock, because a made-up observation time would corrupt ordering and gaps.
    """
    if not isinstance(kubernetes, Mapping):
        return None
    cluster = kubernetes.get("cluster")
    collected_at = cluster.get("collected_at") if isinstance(cluster, Mapping) else None
    observed = parse_timestamp(collected_at)
    if observed is None:
        return None
    resources = [
        *_pod_resources(kubernetes.get("pods")),
        *_workload_resources(kubernetes.get("workloads")),
    ]
    summary = _summary(kubernetes)
    if not resources and EVENT_CAPTURE_SUMMARY_KEY not in summary:
        return None
    return {
        "collected_at": _iso(observed),
        "source": EVIDENCE_SNAPSHOT_SOURCE,
        "replace": False,
        "resources": resources,
        "summary": summary,
    }


def _summary(kubernetes: Mapping[str, Any]) -> JsonObject:
    summary: JsonObject = {
        "live_inventory": False,
        "resources_complete": False,
        "labels_complete": False,
    }
    scopes = kubernetes.get("collection_scopes")
    if isinstance(scopes, list):
        summary["collection_scopes"] = [dict(item) for item in scopes if isinstance(item, Mapping)]
    status = kubernetes.get("collection_status")
    if isinstance(status, Mapping):
        summary["collection_status"] = dict(status)
    capture = kubernetes.get("event_capture")
    if isinstance(capture, Mapping):
        facts = capture.get("events")
        summary[EVENT_CAPTURE_SUMMARY_KEY] = {
            key: value for key, value in capture.items() if key != "events"
        }
        summary[EVENT_FACTS_SUMMARY_KEY] = list(facts) if isinstance(facts, list) else []
    return summary


def _pod_resources(value: object) -> list[JsonObject]:
    return [
        _resource(
            item,
            resource_type="pod",
            api_version="v1",
            kind="Pod",
            status=item.get("phase"),
            fields=POD_SUMMARY_FIELDS,
        )
        for item in _identified(value)
    ]


def _workload_resources(value: object) -> list[JsonObject]:
    return [
        _resource(
            item,
            resource_type="workload",
            api_version=str(item.get("api_version") or "apps/v1"),
            kind=str(item.get("kind") or "Workload"),
            status=None,
            fields=WORKLOAD_SUMMARY_FIELDS,
        )
        for item in _identified(value)
    ]


def _identified(value: object) -> list[Mapping[str, Any]]:
    """Keep only rows with a real UID and name; an anonymous row cannot be tracked."""
    if not isinstance(value, list):
        return []
    return [
        item
        for item in value
        if isinstance(item, Mapping)
        and isinstance(item.get("uid"), str)
        and item["uid"]
        and isinstance(item.get("name"), str)
        and item["name"]
    ]


def _resource(
    item: Mapping[str, Any],
    *,
    resource_type: str,
    api_version: str,
    kind: str,
    status: object,
    fields: tuple[str, ...],
) -> JsonObject:
    return {
        "resource_type": resource_type,
        "api_version": api_version,
        "kind": kind,
        "namespace": item.get("namespace"),
        "name": item["name"],
        "uid": item["uid"],
        "resource_version": item.get("resource_version"),
        "status": str(status) if status else "unknown",
        "summary": {field: item[field] for field in fields if item.get(field) is not None},
    }


def _iso(value: datetime) -> str:
    return value.isoformat()
