"""Assess one image-pull recovery window from the agent's Kubernetes summary."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

ImageWindowStatus = Literal["healthy", "failing", "unknown"]
IMAGE_PULL_REASONS = frozenset({"ErrImagePull", "ImagePullBackOff"})
REQUIRED_COLLECTIONS = ("pods", "workloads", "workload_revisions")


@dataclass(frozen=True)
class ImagePullWindow:
    status: ImageWindowStatus
    reason: str
    deployment_uid: str | None = None
    replicaset_uid: str | None = None
    ready_pod_uids: tuple[str, ...] = ()


def assess_image_pull_window(
    kubernetes: Mapping[str, Any],
    *,
    namespace: str,
    deployment_name: str,
    expected_image: str,
) -> ImagePullWindow:
    if not namespace or not deployment_name or not expected_image:
        return ImagePullWindow("unknown", "target_missing")
    if not _collections_complete(kubernetes, namespace):
        return ImagePullWindow("unknown", "collection_incomplete")
    deployments = [
        item
        for item in _rows(kubernetes, "workloads")
        if item.get("kind") == "Deployment"
        and item.get("namespace") == namespace
        and item.get("name") == deployment_name
    ]
    if len(deployments) != 1:
        return ImagePullWindow("unknown", "deployment_ambiguous_or_missing")
    deployment = deployments[0]
    deployment_uid = _text(deployment.get("uid"))
    desired = _nonnegative_int(deployment.get("desired_replicas"))
    generation = _nonnegative_int(deployment.get("generation"))
    observed_generation = _nonnegative_int(deployment.get("observed_generation"))
    if not deployment_uid or desired is None or desired < 1 or generation is None:
        return ImagePullWindow("unknown", "deployment_state_incomplete")
    if _template_images(deployment.get("pod_template")) != {expected_image}:
        return ImagePullWindow("failing", "deployment_image_mismatch", deployment_uid)
    if observed_generation is None or observed_generation < generation:
        return ImagePullWindow("failing", "deployment_generation_pending", deployment_uid)

    replicasets = [
        item
        for item in _rows(kubernetes, "workload_revisions")
        if item.get("kind") == "ReplicaSet"
        and item.get("namespace") == namespace
        and item.get("owner_uid") == deployment_uid
        and _template_images(item.get("template")) == {expected_image}
    ]
    if len(replicasets) != 1 or not _text(replicasets[0].get("uid")):
        return ImagePullWindow("unknown", "replicaset_ambiguous_or_missing", deployment_uid)
    replicaset_uid = str(replicasets[0]["uid"])
    pods = [
        item
        for item in _rows(kubernetes, "pods")
        if item.get("namespace") == namespace and item.get("owner_uid") == replicaset_uid
    ]
    if not pods or any(not _text(item.get("uid")) for item in pods):
        return ImagePullWindow("unknown", "target_pods_missing", deployment_uid, replicaset_uid)
    if any(_has_image_pull_failure(item) for item in pods):
        return ImagePullWindow(
            "failing", "image_pull_failure_persists", deployment_uid, replicaset_uid
        )
    if any(not _pod_image_identity_present(item) for item in pods):
        return ImagePullWindow(
            "unknown", "pod_image_identity_missing", deployment_uid, replicaset_uid
        )
    ready_pods = tuple(
        sorted(str(item["uid"]) for item in pods if _pod_runs_image(item, expected_image))
    )
    status_ready = all(
        _nonnegative_int(deployment.get(key)) == desired
        for key in ("ready_replicas", "updated_replicas", "available_replicas")
    )
    if not status_ready or len(ready_pods) < desired:
        return ImagePullWindow(
            "failing", "target_pods_not_ready", deployment_uid, replicaset_uid, ready_pods
        )
    return ImagePullWindow(
        "healthy", "target_image_ready", deployment_uid, replicaset_uid, ready_pods
    )


def _collections_complete(kubernetes: Mapping[str, Any], namespace: str) -> bool:
    if not all(
        isinstance(kubernetes.get(key), list)
        and all(isinstance(item, Mapping) for item in kubernetes[key])
        for key in REQUIRED_COLLECTIONS
    ):
        return False
    limits = kubernetes.get("collection_limits")
    if isinstance(limits, Mapping):
        limited = limits.get("lists")
        if not isinstance(limited, Mapping) or any(key in limited for key in REQUIRED_COLLECTIONS):
            return False
    statuses = kubernetes.get("collection_status")
    if isinstance(statuses, Mapping) and any(key in statuses for key in REQUIRED_COLLECTIONS):
        return False
    coverage = kubernetes.get("collection_coverage")
    return isinstance(coverage, list) and any(
        isinstance(item, Mapping)
        and item.get("collection") == "pods"
        and item.get("namespace") == namespace
        and item.get("complete") is True
        for item in coverage
    )


def _rows(kubernetes: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    return [item for item in kubernetes[key] if isinstance(item, Mapping)]


def _template_images(value: Any) -> set[str]:
    if not isinstance(value, Mapping):
        return set()
    spec = value.get("spec")
    if not isinstance(spec, Mapping):
        return set()
    containers = spec.get("containers")
    if not isinstance(containers, list):
        return set()
    return {
        image
        for item in containers
        if isinstance(item, Mapping) and (image := _text(item.get("image")))
    }


def _has_image_pull_failure(pod: Mapping[str, Any]) -> bool:
    reasons = pod.get("waiting_reasons")
    if isinstance(reasons, list) and any(reason in IMAGE_PULL_REASONS for reason in reasons):
        return True
    containers = pod.get("containers")
    return isinstance(containers, list) and any(
        isinstance(item, Mapping) and item.get("state_reason") in IMAGE_PULL_REASONS
        for item in containers
    )


def _pod_runs_image(pod: Mapping[str, Any], expected_image: str) -> bool:
    containers = pod.get("containers")
    return (
        isinstance(containers, list)
        and bool(containers)
        and all(
            isinstance(item, Mapping)
            and item.get("declared_image") == expected_image
            and item.get("ready") is True
            and item.get("state") == "running"
            for item in containers
        )
    )


def _pod_image_identity_present(pod: Mapping[str, Any]) -> bool:
    containers = pod.get("containers")
    return (
        isinstance(containers, list)
        and bool(containers)
        and all(
            isinstance(item, Mapping) and bool(_text(item.get("declared_image")))
            for item in containers
        )
    )


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _nonnegative_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None
