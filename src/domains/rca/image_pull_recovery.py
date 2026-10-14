"""ImagePullBackOff 복구를 실제 배포 세대와 소유 Pod에 한정해 판정한다."""

from __future__ import annotations

__all__ = ["owned_pods", "evaluate_image_recovery", "image_pull_failed"]

from datetime import datetime
from typing import Any


def owned_pods(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    deployment_uid = snapshot["deployment"]["metadata"]["uid"]
    replicasets = snapshot["replicasets"]["items"]
    owned = {item["metadata"]["uid"] for item in replicasets if _owned_by(item, deployment_uid)}
    return [
        pod
        for pod in snapshot["pods"]["items"]
        if any(_owned_by(pod, uid) for uid in owned)
        and not pod["metadata"].get("deletionTimestamp")
    ]


def image_pull_failed(snapshot: dict[str, Any], container: str) -> bool:
    return any(
        status.get("name") == container
        and status.get("state", {}).get("waiting", {}).get("reason")
        in {"ImagePullBackOff", "ErrImagePull"}
        for pod in owned_pods(snapshot)
        for status in pod.get("status", {}).get("containerStatuses", [])
    )


def evaluate_image_recovery(
    incident: dict[str, Any], pull: dict[str, Any], snapshot: dict[str, Any]
) -> str:
    if pull.get("merged") is not True:
        return "awaiting_merge"
    if not pull.get("merged_at") or not pull.get("merge_commit_sha"):
        return "missing_merge_evidence"
    observed = datetime.fromisoformat(snapshot["observed_at"])
    merged = datetime.fromisoformat(pull["merged_at"])
    if observed <= merged:
        return "stale_evidence"
    deployment = snapshot["deployment"]
    metadata = deployment["metadata"]
    if metadata["uid"] != incident["deployment_uid"]:
        return "deployment_replaced"
    generation = metadata["generation"]
    if generation <= incident["generation"]:
        return "awaiting_rollout"
    if deployment.get("status", {}).get("observedGeneration", 0) < generation:
        return "awaiting_controller"
    desired = deployment["spec"].get("replicas", 1)
    if desired != incident["replicas"] or desired < 1:
        return "replica_count_changed"
    containers = deployment["spec"]["template"]["spec"]["containers"]
    if not _has_image(containers, incident):
        return "image_not_applied"
    status = deployment.get("status", {})
    if any(
        status.get(key, 0) != desired
        for key in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")
    ):
        return "rollout_incomplete"
    pods = owned_pods(snapshot)
    if len(pods) != desired or not all(_healthy_pod(pod, incident) for pod in pods):
        return "pods_not_ready"
    return "recovered"


def _owned_by(item: dict[str, Any], uid: str) -> bool:
    return any(
        owner.get("uid") == uid and owner.get("controller") is True
        for owner in item["metadata"].get("ownerReferences", [])
    )


def _has_image(containers: list[dict[str, Any]], incident: dict[str, Any]) -> bool:
    matches = [item for item in containers if item.get("name") == incident["container"]]
    return len(matches) == 1 and matches[0].get("image") == incident["image"]


def _healthy_pod(pod: dict[str, Any], incident: dict[str, Any]) -> bool:
    if not _has_image(pod["spec"]["containers"], incident):
        return False
    status = pod.get("status", {})
    if status.get("phase") != "Running":
        return False
    if not any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in status.get("conditions", [])
    ):
        return False
    matches = [
        item
        for item in status.get("containerStatuses", [])
        if item.get("name") == incident["container"]
    ]
    return len(matches) == 1 and matches[0].get("ready") is True
