"""Project independent Git, Argo and Pod evidence for one merged incident."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from domains.rca.history import OBSERVATION_MAX_GAP_MS
from packages.contracts.timeline.history import (
    IncidentDeploymentImageWindow,
    IncidentDeploymentTracking,
)

ARGO_MAX_AGE_MS = 120_000


def project_deployment_tracking(
    *,
    plan: Mapping[str, Any] | None,
    git_source: Mapping[str, Any] | None,
    argo_observations: Sequence[Mapping[str, Any]],
    image_observations: Sequence[Mapping[str, Any]],
    now: datetime,
) -> IncidentDeploymentTracking | None:
    if plan is None:
        return None
    payload = _mapping(plan.get("payload"))
    target = _mapping(payload.get("target"))
    merge = _mapping(_mapping(payload.get("lifecycle")).get("merge"))
    request = _mapping(merge.get("deployment_request"))
    commit_sha = _text(merge.get("merge_commit_sha"))
    merged_at = _time(merge.get("merged_at"))
    if not commit_sha or merged_at is None:
        return None
    source = _mapping(git_source)
    argo = next(
        (item for item in argo_observations if item.get("expected_sha") == commit_sha), None
    )
    image_rows = [item for item in image_observations if item.get("expected_sha") == commit_sha]
    windows = tuple(
        IncidentDeploymentImageWindow(
            evidence_key=str(item["evidence_key"]),
            observed_at=item["observed_at"],
            status=item["status"],
            reason=str(item["reason"]),
            ready_pod_count=int(item["ready_pod_count"]),
            consecutive_healthy_windows=int(item["consecutive_healthy_windows"]),
            confirmed_recovered=item["confirmed_recovered"] is True,
        )
        for item in reversed(image_rows)
    )
    status, reason = _current_state(source, argo, image_rows, commit_sha, now)
    return IncidentDeploymentTracking(
        status=status,
        reason=reason,
        merge_commit_sha=commit_sha,
        merged_at=merged_at,
        repo_ref=_text(source.get("repo_ref")) or _text(request.get("repo_ref")) or None,
        manifest_path=_text(source.get("manifest_path"))
        or _text(request.get("manifest_path"))
        or None,
        namespace=_text(target.get("namespace")) or None,
        deployment_name=_text(target.get("resource_name")) or None,
        git_source_status=_text(source.get("status")) or None,
        git_image=_text(source.get("image")) or None,
        git_observed_at=_time(source.get("observed_at")),
        argocd_status=_text(argo.get("status")) if argo is not None else None,
        argocd_project=_text(argo.get("project")) if argo is not None else None,
        argocd_application=_text(argo.get("application")) if argo is not None else None,
        argocd_revision=_text(argo.get("sync_revision")) if argo is not None else None,
        argocd_health=_text(argo.get("health_status")) if argo is not None else None,
        argocd_checked_at=_time(argo.get("last_checked_at")) if argo is not None else None,
        image_windows=windows,
    )


def _current_state(
    source: Mapping[str, Any],
    argo: Mapping[str, Any] | None,
    image_rows: Sequence[Mapping[str, Any]],
    commit_sha: str,
    now: datetime,
) -> tuple[str, str]:
    if not source:
        return "merged", "git_source_pending"
    if source.get("status") != "observed":
        return "unknown", "git_source_unconfirmed"
    if argo is None:
        return "merged", "argocd_unobserved"
    checked_at = _time(argo.get("last_checked_at"))
    if checked_at is None or not _fresh(checked_at, now, ARGO_MAX_AGE_MS):
        return "unknown", "argocd_observation_stale"
    if argo.get("status") == "pending":
        return "deploy_pending", "argocd_revision_pending"
    if argo.get("status") != "reported_applied" or argo.get("sync_revision") != commit_sha:
        return "unknown", "argocd_application_unconfirmed"
    if not image_rows:
        return "unknown", "kubernetes_unobserved"
    latest = image_rows[0]
    observed_at = _time(latest.get("observed_at"))
    if observed_at is None or not _fresh(observed_at, now, OBSERVATION_MAX_GAP_MS):
        return "unknown", "kubernetes_observation_stale"
    reconciled_at = _time(argo.get("reconciled_at"))
    if reconciled_at is None or observed_at < reconciled_at:
        return "unknown", "kubernetes_observation_before_application"
    if latest.get("expected_image") != source.get("image"):
        return "unknown", "kubernetes_image_mismatch"
    if latest.get("status") == "failing":
        return "deployed_symptom_persists", str(latest.get("reason") or "image_pull_failure")
    if latest.get("status") != "healthy":
        return "unknown", str(latest.get("reason") or "kubernetes_unconfirmed")
    prior = image_rows[1] if len(image_rows) > 1 else None
    prior_observed_at = _time(prior.get("observed_at")) if prior is not None else None
    if (
        latest.get("confirmed_recovered") is True
        and prior is not None
        and prior.get("status") == "healthy"
        and prior.get("expected_image") == source.get("image")
        and prior_observed_at is not None
        and prior_observed_at >= reconciled_at
    ):
        return "recovered", "two_healthy_image_windows"
    return "verification_pending", "second_healthy_window_required"


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else None


def _fresh(observed_at: datetime, now: datetime, maximum_age_ms: int) -> bool:
    age_ms = (now - observed_at).total_seconds() * 1_000
    return 0 <= age_ms <= maximum_age_ms
