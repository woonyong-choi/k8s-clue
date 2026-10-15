"""Attach agent image-pull observations to an exact merged recovery incident."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from domains.gitops.argo_observation import (
    ArgoObservationError,
    observe_argocd_for_git_change,
)
from domains.gitops.events import GitChangedBody
from domains.rca.deployment_history import project_deployment_tracking
from domains.rca.deployment_verdict import assess_image_pull_window
from domains.rca.events import (
    ClusterEvidenceReceivedBody,
    IncidentResolvedBody,
    RecoveryVerificationUpdatedBody,
)
from packages.runtime.dispatch import CorrelatedResult, EventContext


# cost: time O(p * k), heap O(p + k), stack O(1), io O(p)
# vars: p = 최대 100개 활성 복구 계획, k = 수집된 Kubernetes 요약 행 수
# basis: estimate
async def track_image_pull_evidence(
    event: ClusterEvidenceReceivedBody,
    evidence: Mapping[str, Any],
    context: EventContext[Any],
) -> tuple[CorrelatedResult, ...]:
    if not event.evidence_key:
        return ()
    observed_at = _utc_time(evidence.get("window_start"))
    if observed_at is None or observed_at > datetime.now(UTC):
        return ()
    plans = await context.db.list_image_pull_tracking_plans(
        event.workspace_id, event.cluster_id, limit=100
    )
    completed_events: list[CorrelatedResult] = []
    for plan in plans:
        payload = _mapping(plan.get("payload"))
        if payload.get("root_cause") != "wrong_image_tag":
            continue
        target = _mapping(payload.get("target"))
        if target.get("resource_kind") != "Deployment":
            continue
        merge = _mapping(_mapping(payload.get("lifecycle")).get("merge"))
        request = _mapping(merge.get("deployment_request"))
        merged_at = _utc_time(merge.get("merged_at"))
        commit_sha = _text(merge.get("merge_commit_sha"))
        binding_id = _text(merge.get("binding_id"))
        manifest_path = _text(request.get("manifest_path"))
        if (
            merged_at is None
            or observed_at <= merged_at
            or not commit_sha
            or not binding_id
            or not manifest_path
        ):
            continue
        source = await context.db.get_git_source_observation(
            event.workspace_id, binding_id, commit_sha, manifest_path
        )
        if not isinstance(source, Mapping) or source.get("status") != "observed":
            continue
        if (
            source.get("cluster_id") != event.cluster_id
            or source.get("namespace") != target.get("namespace")
            or source.get("deployment_name") != target.get("resource_name")
        ):
            continue
        expected_image = _text(source.get("image"))
        if not expected_image:
            continue
        await _refresh_argo_observation(
            context,
            event,
            request,
            str(plan["correlation_id"]),
            commit_sha,
            expected_image,
            binding_id,
        )
        kubernetes = _mapping(evidence.get("kubernetes"))
        verdict = assess_image_pull_window(
            kubernetes,
            namespace=_text(target.get("namespace")),
            deployment_name=_text(target.get("resource_name")),
            expected_image=expected_image,
        )
        await context.db.record_image_pull_observation(
            {
                "workspace_id": event.workspace_id,
                "correlation_id": str(plan["correlation_id"]),
                "binding_id": binding_id,
                "expected_sha": commit_sha,
                "expected_image": expected_image,
                "evidence_key": event.evidence_key,
                "status": verdict.status,
                "reason": verdict.reason,
                "deployment_uid": verdict.deployment_uid,
                "replicaset_uid": verdict.replicaset_uid,
                "ready_pod_count": len(verdict.ready_pod_uids),
                "observed_at": observed_at,
            }
        )
        if plan.get("status") in {"deploy_pending", "verification_pending"}:
            completed_events.extend(
                await _complete_recovered_plan(
                    context, plan, source, event.workspace_id, binding_id, event.evidence_key
                )
            )
    return tuple(completed_events)


# cost: time O(1), heap O(1), stack O(1), io 2-3
# basis: estimate
async def _complete_recovered_plan(
    context: EventContext[Any],
    plan: Mapping[str, Any],
    source: Mapping[str, Any],
    workspace_id: str,
    binding_id: str,
    evidence_key: str,
) -> tuple[CorrelatedResult, ...]:
    correlation_id = str(plan["correlation_id"])
    argo, image = (
        await context.db.list_argo_application_observations(
            workspace_id, correlation_id, binding_id, limit=1
        ),
        await context.db.list_image_pull_observations(
            workspace_id, correlation_id, binding_id, limit=2
        ),
    )
    tracking = project_deployment_tracking(
        plan=plan,
        git_source=source,
        argo_observations=argo,
        image_observations=image,
        now=datetime.now(UTC),
    )
    if tracking is None or tracking.status != "recovered":
        return ()
    payload = _mapping(plan.get("payload"))
    lifecycle = dict(_mapping(payload.get("lifecycle")))
    verification = dict(_mapping(lifecycle.get("verification")))
    after = {
        "merge_commit_sha": tracking.merge_commit_sha,
        "git_image": tracking.git_image,
        "argocd_revision": tracking.argocd_revision,
        "argocd_checked_at": (
            tracking.argocd_checked_at.isoformat() if tracking.argocd_checked_at else None
        ),
        "evidence_keys": [window.evidence_key for window in tracking.image_windows[-2:]],
        "ready_pod_count": tracking.image_windows[-1].ready_pod_count,
    }
    verification.update(
        status="completed",
        last_evidence_key=evidence_key,
        last_reason_code="image_pull_recovered",
        last_reason="대상 이미지와 Pod 상태가 연속 두 증거 창에서 정상입니다.",
        after=after,
    )
    lifecycle.update(phase="completed", verification=verification)
    saved = await context.db.update_recovery_plan_lifecycle_if_status(
        str(plan["plan_id"]),
        workspace_id,
        expected_statuses=("deploy_pending", "verification_pending"),
        status="completed",
        lifecycle=lifecycle,
    )
    if saved is None:
        return ()
    before = dict(_mapping(verification.get("before")))
    reason = str(verification["last_reason"])
    return (
        CorrelatedResult(
            RecoveryVerificationUpdatedBody(
                plan_id=str(plan["plan_id"]),
                incident_id=str(plan["incident_id"]),
                status="completed",
                reason_code="image_pull_recovered",
                reason=reason,
                evidence_ref=evidence_key,
                before=before,
                after=after,
                workspace_id=workspace_id,
            ),
            correlation_id,
        ),
        CorrelatedResult(
            IncidentResolvedBody(
                incident_id=str(plan["incident_id"]),
                cluster_id=str(_mapping(payload.get("target")).get("cluster_id")),
                reason=reason,
                evidence_ref=evidence_key,
                recovery_plan_id=str(plan["plan_id"]),
                before=before,
                after=after,
                workspace_id=workspace_id,
            ),
            correlation_id,
        ),
    )


# cost: time O(b), heap O(b), stack O(1), io 2-3
# vars: b = 제한된 Argo Application 응답 바이트 수
# basis: estimate
async def _refresh_argo_observation(
    context: EventContext[Any],
    event: ClusterEvidenceReceivedBody,
    request: Mapping[str, Any],
    correlation_id: str,
    commit_sha: str,
    image: str,
    binding_id: str,
) -> None:
    rows = await context.db.list_argo_application_observations(
        event.workspace_id, correlation_id, binding_id, limit=1
    )
    now = datetime.now(UTC)
    if (
        rows
        and rows[0].get("expected_sha") == commit_sha
        and isinstance(rows[0].get("last_checked_at"), datetime)
    ):
        checked_at = rows[0]["last_checked_at"]
        if checked_at.tzinfo is not None:
            age = now - checked_at
            if timedelta(0) <= age < timedelta(seconds=30):
                return
    try:
        git_event = GitChangedBody(
            commit_sha=commit_sha,
            image=image,
            replicas=int(request.get("replicas") or 1),
            workspace_id=event.workspace_id,
            repository_id=_text(request.get("repository_id")),
            repo_ref=_text(request.get("repo_ref")),
            branch=_text(request.get("branch")),
            watch_target_id=_text(request.get("watch_target_id")),
            binding_id=binding_id,
            application_id=_text(request.get("application_id")),
            workflow_run_id=_text(request.get("workflow_run_id")),
            environment=_text(request.get("environment")),
            cluster_id=event.cluster_id,
            manifest_path=_text(request.get("manifest_path")),
            source_type=_text(request.get("source_type")),
        )
        await observe_argocd_for_git_change(git_event, context, correlation_id=correlation_id)
    except (ArgoObservationError, TypeError, ValueError) as exc:
        reason = exc.reason if isinstance(exc, ArgoObservationError) else "invalid_merge_request"
        await context.db.record_argo_application_observation(
            {
                "workspace_id": event.workspace_id,
                "binding_id": binding_id,
                "correlation_id": correlation_id,
                "expected_sha": commit_sha,
                "project": "",
                "application": "",
                "status": "unavailable",
                "reason": reason,
                "fingerprint": hashlib.sha256(f"unavailable:{reason}".encode()).hexdigest(),
                "checked_at": now,
                "request_count": getattr(exc, "request_count", 0),
                "response_bytes": getattr(exc, "response_bytes", 0),
            }
        )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _utc_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else None
