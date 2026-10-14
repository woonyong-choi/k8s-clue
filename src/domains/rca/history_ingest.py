"""Reuse an agent evidence Kubernetes payload as an inventory snapshot for the ledger.

The payload is a scoped incident collection, never fleet truth.  The mapping
therefore declares it incomplete (``live_inventory`` false, no replacement).  A deletion
is only recorded inside a namespace whose Pod list the producer proved complete; every
other namespace, collection, truncated or unproven list, and every older producer that
sends no proof can record observations and updates but never an absence.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from domains.inventory.coverage import COLLECTION_COVERAGE_SUMMARY_KEY, POD_COLLECTION
from domains.inventory.kubernetes_events import (
    EVENT_CAPTURE_SUMMARY_KEY,
    EVENT_FACTS_SUMMARY_KEY,
    parse_timestamp,
)
from domains.inventory.snapshot_evidence import SCOPED_COVERAGE_AUTHORITY_KEY
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
INVALID_PROOF_REASON = "invalid_proof"
PROOF_FLAGS = ("observed", "complete", "delete_safe", "truncated")


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
    # An empty list is still a record when its namespace is proven complete: it is the deletion.
    if (
        not resources
        and EVENT_CAPTURE_SUMMARY_KEY not in summary
        and not summary.get(COLLECTION_COVERAGE_SUMMARY_KEY)
    ):
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
    coverage = _pod_coverage(kubernetes)
    if coverage:
        summary[COLLECTION_COVERAGE_SUMMARY_KEY] = coverage
        summary[SCOPED_COVERAGE_AUTHORITY_KEY] = True
    capture = kubernetes.get("event_capture")
    if isinstance(capture, Mapping):
        facts = capture.get("events")
        summary[EVENT_CAPTURE_SUMMARY_KEY] = {
            key: value for key, value in capture.items() if key != "events"
        }
        summary[EVENT_FACTS_SUMMARY_KEY] = list(facts) if isinstance(facts, list) else []
    return summary


def _pod_coverage(kubernetes: Mapping[str, Any]) -> list[JsonObject]:
    """Forward only the producer's Pod namespace entries, and none if any list was limited.

    Workloads, services and every other collection are never forwarded: their lists are
    filtered or limited and carry no completeness claim.  A payload that reports a Pod list
    limit cannot say which namespace lost rows, so all of its proofs are dropped.
    """
    raw = kubernetes.get(COLLECTION_COVERAGE_SUMMARY_KEY)
    if not isinstance(raw, list) or _pod_list_limited(kubernetes):
        return []
    entries: list[JsonObject] = []
    for entry in raw:
        if not isinstance(entry, Mapping) or entry.get("collection") != POD_COLLECTION:
            continue
        namespace = entry.get("namespace")
        if entry.get("scope") != "namespace" or not isinstance(namespace, str) or not namespace:
            continue
        entries.append(_proof_entry(entry, namespace))
    return entries


def _proof_entry(entry: Mapping[str, Any], namespace: str) -> JsonObject:
    """Copy one producer entry; anything that is not exactly well typed grants no deletion.

    A wrong type is never normalized into "no reason" or "false": the entry is kept as an
    observation record with its authority removed, so the evidence is still stored.
    """
    if _is_well_typed(entry) and _grants_deletion(entry):
        return _entry(namespace, observed=True, complete=True, truncated=False, reasons=[])
    return _unauthorized_entry(entry, namespace)


def _is_well_typed(entry: Mapping[str, Any]) -> bool:
    flags_are_bool = all(isinstance(entry.get(key), bool) for key in PROOF_FLAGS)
    reasons = entry.get("reason_codes")
    reasons_are_text = isinstance(reasons, list) and all(isinstance(r, str) for r in reasons)
    selector = entry.get("label_selector")
    selector_is_text = selector is None or isinstance(selector, str)
    return flags_are_bool and reasons_are_text and selector_is_text


def _grants_deletion(entry: Mapping[str, Any]) -> bool:
    proved = entry["observed"] and entry["complete"] and entry["delete_safe"]
    clean = not entry["truncated"] and not entry["reason_codes"] and not entry["label_selector"]
    return bool(proved and clean)


def _unauthorized_entry(entry: Mapping[str, Any], namespace: str) -> JsonObject:
    reasons = entry.get("reason_codes")
    named = (
        sorted({code for code in reasons if isinstance(code, str)}) if _is_well_typed(entry) else []
    )
    return _entry(
        namespace,
        observed=entry.get("observed") is True,
        complete=False,
        truncated=entry.get("truncated") is not False,
        reasons=named or [INVALID_PROOF_REASON],
        label_selector=entry.get("label_selector"),
    )


def _entry(
    namespace: str,
    *,
    observed: bool,
    complete: bool,
    truncated: bool,
    reasons: list[str],
    label_selector: object = None,
) -> JsonObject:
    selector = label_selector if isinstance(label_selector, str) and label_selector else None
    return {
        "collection": POD_COLLECTION,
        "scope": "namespace",
        "namespace": namespace,
        "observed": observed,
        "complete": complete,
        "delete_safe": complete,
        "truncated": truncated,
        "label_selector": selector,
        "reason_codes": reasons,
    }


def _pod_list_limited(kubernetes: Mapping[str, Any]) -> bool:
    """True when the payload reports, or cannot be trusted to deny, a limited Pod list.

    Only a consistent report that names other lists leaves the Pod proof standing: an
    untyped flag, a flag that contradicts its list, or any unknown shape cannot say that
    no Pod was dropped.
    """
    if "collection_limits" not in kubernetes:
        return False
    limits = kubernetes["collection_limits"]
    if not isinstance(limits, Mapping):
        return True
    truncated = limits.get("truncated")
    lists = limits.get("lists")
    if not isinstance(truncated, bool) or not isinstance(lists, Mapping):
        return True
    if truncated != bool(lists):
        return True
    return POD_COLLECTION in lists


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
    """Keep every named row.  A row without a UID stays trackable by name and is never given one."""
    if not isinstance(value, list):
        return []
    return [
        item
        for item in value
        if isinstance(item, Mapping) and isinstance(item.get("name"), str) and item["name"]
    ]


def _uid(item: Mapping[str, Any]) -> str | None:
    uid = item.get("uid")
    return uid.strip() if isinstance(uid, str) and uid.strip() else None


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
        "uid": _uid(item),
        "resource_version": item.get("resource_version"),
        "status": str(status) if status else "unknown",
        "summary": {field: item[field] for field in fields if item.get(field) is not None},
    }


def _iso(value: datetime) -> str:
    return value.isoformat()
