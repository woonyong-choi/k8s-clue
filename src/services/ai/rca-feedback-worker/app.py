"""rca-feedback-worker — RCA blocked/action/fallback을 후속 조치 이벤트로 정규화."""

from __future__ import annotations

import hashlib
import re
from collections.abc import AsyncIterator, Mapping
from dataclasses import replace

from domains.gitops.argo_observation import observe_argocd_for_git_change
from domains.gitops.events import GitChangedBody, WorkflowRunCompletedBody, WorkflowRunFailedBody
from domains.rca.events import (
    ClusterEvidenceReceivedBody,
    IncidentResolvedBody,
    RcaActionRequiredBody,
    RcaAiFallbackRequestedBody,
    RcaAnalysisBlockedBody,
    RcaCompletedBody,
    RcaFollowupRequiredBody,
    RecoveryPlannedBody,
    RecoveryPrMergedBody,
    RecoveryPrTrackedBody,
    RecoveryVerificationFailedBody,
    RecoveryVerificationStartedBody,
    RecoveryVerificationUpdatedBody,
)
from domains.rca.image_pull_tracking import track_image_pull_evidence
from domains.rca.recovery_verification import (
    DEFAULT_MAXIMUM_SECONDS,
    DEFAULT_MINIMUM_SECONDS,
    STANDARD_SLI_ALERT_NAME,
    VerificationDecision,
    before_alert_snapshot,
    evaluate_recovery_evidence,
    finite_float,
    mapping,
    metric_identity,
    metric_sample_with_identity,
    normalized_utc,
    parse_datetime,
    protected_workloads,
    recovery_target,
    verification_deadline,
)
from domains.scm.events import SafePrCreatedBody, SafePrFailedBody
from packages.config.settings import env
from packages.contracts.event_bus.bodies import EventBody, JsonObject
from packages.contracts.event_bus.bodies.platform import PipelineContractFailedBody
from packages.contracts.stores import RecoveryPlanStore
from packages.runtime.app import App, EventContext
from packages.runtime.dispatch import CorrelatedResult
from services.ai.agent.defaults import ActionRoutes
from services.ai.agent.recovery.engine import RecoveryPlanner

app = App("rca-feedback-worker")

SEVERITY_WARNING = "warning"
NON_ACTIONABLE_ROOT_CAUSES = {
    "",
    "unknown",
    "insufficient_evidence",
    "none",
    "분석 가능한 원인 후보 없음",
}
FOLLOWUP_SUMMARIES = {
    "rule_missing": "대표 증상에 맞는 RCA rule이 없어 자동 원인 확정을 중단했습니다.",
    "insufficient_evidence": "원인 후보는 있지만 필요한 근거가 부족해 자동 RCA 확정을 중단했습니다.",
    "evidence_missing": "원인 후보는 있지만 필요한 근거가 부족해 자동 RCA 확정을 중단했습니다.",
    "no_evaluation": "평가 가능한 원인 후보가 없어 root cause를 선택하지 못했습니다.",
    "context_missing": "worker 간 이벤트 payload에 필수 RCA context가 없어 분석을 진행하지 못했습니다.",
    "action_required": "자동 진행이 차단되어 운영자 조치가 필요합니다.",
    "ai_fallback_required": "대표 증상에 맞는 RCA rule이 없어 AI fallback 검토가 필요합니다.",
    "gitops_authority_unavailable": "Safe PR 생성을 위한 GitOps 권위 context가 없어 운영자 조치가 필요합니다.",
    "gitops_authority_mismatch": "Safe PR 대상과 GitOps 권위 context가 일치하지 않아 운영자 확인이 필요합니다.",
    "safe_pr_patch_missing": "Safe PR에 적용할 구체적인 manifest patch가 없어 운영자 조치가 필요합니다.",
    "safe_pr_patch_unsupported": "선택한 복구 조치를 현재 Safe PR patch로 변환할 수 없습니다.",
    "recovery_verification_prerequisites_missing": (
        "복구 완료 판정에 필요한 사전 근거가 없어 Safe PR 추적을 중단했습니다."
    ),
}
MISSING_EVIDENCE_LABELS = {
    "kubernetes": "Kubernetes 상태 근거",
    "kubernetes:cluster_resource_state": "Kubernetes 상태 근거",
    "metrics": "메트릭 근거",
    "metrics:telemetry_metrics": "메트릭 근거",
    "logs": "관련 Pod 로그",
    "logs:related_logs": "관련 Pod 로그",
    "traces": "trace 근거",
    "traces:related_traces": "trace 근거",
    "metadata": "metadata 근거",
    "metadata:current_workload_snapshot": "대상 Workload 상세 snapshot",
    "metadata:current_workload_snapshots": "Workload snapshot 목록",
    "metadata:referenced_config_objects": "ConfigMap/Secret reference 상태",
    "metadata:resource_quotas": "Namespace ResourceQuota 상태",
    "metadata:change_context": "최근 변경 이력",
    "matching_cause_rule": "대표 증상에 맞는 RCA rule",
    "gitops_authority_context": "GitOps 권위 context",
    "matching_gitops_authority_context": "대상과 일치하는 GitOps 권위 context",
    "patchable_authority_snapshot": "patch 생성 가능한 GitOps snapshot",
    "supported_patch_action": "지원 가능한 Safe PR patch action",
    "manifest_patch": "구체적인 manifest patch",
}
RETRYABLE_RECOVERY_REASON_CODES = frozenset(
    {
        "gitops_authority_unavailable",
        "gitops_authority_mismatch",
        "safe_pr_patch_missing",
        "safe_pr_patch_unsupported",
        "safe_pr_preflight_failed",
        "safe_pr_preflight_unavailable",
        "pre_recovery_continuity_baseline_missing",
        "pre_recovery_sli_baseline_missing",
        "recovery_verification_prerequisites_missing",
    }
)
RECOVERY_VERIFICATION_MIN_SECONDS_ENV = "RECOVERY_VERIFICATION_MIN_SECONDS"
RECOVERY_VERIFICATION_MAX_SECONDS_ENV = "RECOVERY_VERIFICATION_MAX_SECONDS"
RECOVERY_LOAD_TOLERANCE_RATIO_ENV = "RECOVERY_LOAD_TOLERANCE_RATIO"
RECOVERY_STATUS_SELECTED = "selected"
RECOVERY_STATUS_PR_OPEN = "pr_open"
RECOVERY_STATUS_DEPLOY_PENDING = "deploy_pending"
RECOVERY_STATUS_VERIFICATION_PENDING = "verification_pending"
RECOVERY_STATUS_COMPLETED = "completed"
RECOVERY_STATUS_FAILED = "failed"
SAFE_PR_ROUTE = ActionRoutes().safe_pr
GITHUB_SHA_RE = re.compile(r"^[0-9a-f]{40,64}$")

planner = RecoveryPlanner()


@app.on(GitChangedBody)
async def on_git_changed(evt: GitChangedBody, ctx: EventContext[RecoveryPlanStore]) -> None:
    await observe_argocd_for_git_change(evt, ctx)


def verification_window_seconds() -> tuple[int, int]:
    minimum = _bounded_env_seconds(
        RECOVERY_VERIFICATION_MIN_SECONDS_ENV,
        DEFAULT_MINIMUM_SECONDS,
        lower=DEFAULT_MINIMUM_SECONDS,
        upper=DEFAULT_MAXIMUM_SECONDS,
    )
    maximum = _bounded_env_seconds(
        RECOVERY_VERIFICATION_MAX_SECONDS_ENV,
        DEFAULT_MAXIMUM_SECONDS,
        lower=minimum,
        upper=DEFAULT_MAXIMUM_SECONDS,
    )
    return minimum, maximum


def _bounded_env_seconds(
    name: str,
    default: int,
    *,
    lower: int,
    upper: int,
) -> int:
    try:
        value = int(env(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(lower, min(value, upper))


def followup_summary(reason_code: str, fallback: str) -> str:
    return FOLLOWUP_SUMMARIES.get(reason_code, fallback)


def missing_evidence_label(source: str) -> str:
    if source in MISSING_EVIDENCE_LABELS:
        return MISSING_EVIDENCE_LABELS[source]
    if source.startswith("signal:"):
        signal_id = source.removeprefix("signal:").replace("_", " ")
        return f"판별 신호({signal_id})"
    if ":" in source:
        evidence_source, evidence_name = source.split(":", 1)
        return f"{evidence_source} {evidence_name.replace('_', ' ')} 근거"
    return source


def collect_actions_for_missing(missing_evidence: list[str]) -> list[JsonObject]:
    return [
        {
            "action_type": "collect_evidence",
            "source": source,
            "query_id": f"collect_{source}",
            "description": (
                f"{missing_evidence_label(source)}를 수집한 뒤 RCA 평가를 재실행합니다."
            ),
        }
        for source in missing_evidence
    ]


def normalize_next_actions(
    next_actions: list[JsonObject], missing_evidence: list[str]
) -> list[JsonObject]:
    if next_actions:
        return next_actions
    if missing_evidence:
        return collect_actions_for_missing(missing_evidence)
    return [
        {
            "action_type": "manual_review",
            "description": "자동 후속 조치를 결정할 정보가 부족해 운영자 검토가 필요합니다.",
        }
    ]


@app.on(RcaAnalysisBlockedBody)
async def on_rca_analysis_blocked(evt: RcaAnalysisBlockedBody) -> AsyncIterator[EventBody]:
    yield RcaFollowupRequiredBody(
        reason_code=evt.reason_code,
        summary=followup_summary(evt.reason_code, evt.reason),
        evidence_ref=evt.evidence_ref,
        workspace_id=evt.workspace_id,
        severity=evt.severity,
        incident=evt.incident,
        missing_evidence=evt.missing_evidence,
        next_actions=normalize_next_actions(evt.next_actions, evt.missing_evidence),
        diagnostics={
            **evt.diagnostics,
            "source_event": evt.__subject__,
            "agent_safe": True,
        },
    )
    planned = blocked_recovery_plan(evt)
    if planned is not None:
        yield planned


def blocked_recovery_plan(evt: RcaAnalysisBlockedBody) -> RecoveryPlannedBody | None:
    """근거가 일부 부족해도 원인 후보가 식별되면 승인형 복구 후보를 노출한다.

    analysis_blocked 경로에서는 자동 실행을 절대 하지 않고 selection_required 계획만 만든다.
    """

    if evt.incident is None or evt.rca_detail is None or evt.rule_missing is not None:
        return None
    if evt.rca_detail.root_cause.strip().lower() in NON_ACTIONABLE_ROOT_CAUSES:
        return None
    report = RcaCompletedBody(
        root_cause=evt.rca_detail.root_cause,
        action="plan_recovery",
        evidence_ref=evt.evidence_ref,
        workspace_id=evt.workspace_id,
        evidence=evt.evidence,
        incident=evt.incident,
        evidence_bundle=evt.evidence_bundle,
        candidates=evt.candidates,
        evaluations=evt.evaluations,
        rca_detail=evt.rca_detail,
        rule_missing=evt.rule_missing,
    )
    plan_event = planner.plan_body(report)
    if not isinstance(plan_event, RecoveryPlannedBody) or plan_event.plan is None:
        return None
    candidates = [
        replace(
            candidate,
            approval_required=True,
            draft=replace(
                candidate.draft,
                params={
                    **candidate.draft.params,
                    "analysis_blocked_fallback": True,
                    "analysis_blocked_reason": evt.reason_code,
                    "missing_evidence": evt.missing_evidence,
                },
            ),
        )
        for candidate in plan_event.plan.candidates
    ]
    if not candidates:
        return None
    plan = replace(
        plan_event.plan,
        summary=f"{plan_event.plan.summary} 추가 근거 수집이 필요해 운영자 선택 후 진행합니다.",
        selection_required=True,
        candidates=candidates,
        recommended_action_id=candidates[0].action_id,
        execution_route=candidates[0].route,
    )
    return replace(plan_event, draft=candidates[0].draft, plan=plan)


@app.on(SafePrCreatedBody)
async def on_recovery_safe_pr_created(
    evt: SafePrCreatedBody,
    ctx: EventContext[RecoveryPlanStore],
) -> AsyncIterator[EventBody]:
    """Bind only a complete GitHub PR identity to the selected recovery."""

    record = await ctx.db.get_recovery_plan_by_correlation(
        ctx.correlation_id,
        evt.workspace_id,
    )
    if not isinstance(record, dict) or record.get("status") != RECOVERY_STATUS_SELECTED:
        return
    payload = mapping(record.get("payload"))
    selected = selected_recovery_candidate(record)
    if not selected or text_value(selected.get("route")) != SAFE_PR_ROUTE:
        return
    params = selected_candidate_params(
        await recovery_approval_record(ctx, record, evt.workspace_id)
    )
    authorized_changes = approved_change_contract(params.get("authorized_changes"))
    identity_error = safe_pr_identity_error(evt, params)
    if identity_error is not None:
        reopened = await ctx.db.reopen_recovery_plan_action(
            str(record["plan_id"]),
            evt.workspace_id,
            str(record.get("selected_action_id") or ""),
        )
        if not reopened:
            return
        yield RcaFollowupRequiredBody(
            reason_code="safe_pr_identity_incomplete",
            summary=identity_error,
            evidence_ref=str(record.get("evidence_ref") or "unknown"),
            workspace_id=evt.workspace_id,
            severity=SEVERITY_WARNING,
            next_actions=[
                {
                    "action_type": "recreate_safe_pr",
                    "description": "GitHub PR immutable identity를 다시 확보한 뒤 복구 PR을 재생성합니다.",
                }
            ],
            diagnostics={
                "plan_id": record.get("plan_id"),
                "action_id": record.get("selected_action_id"),
                "pr_url": evt.pr_url,
                "retryable": True,
            },
        )
        return

    now = normalized_utc(await ctx.db.current_database_time())
    target = selected_recovery_target(payload, selected)
    original_evidence = await ctx.db.get_evidence_payload(
        evt.workspace_id,
        ctx.correlation_id,
        "rca_bundle",
    )
    original_evidence = original_evidence if isinstance(original_evidence, dict) else {}
    collected_failure_ratio, collected_failure_ratio_identity = metric_sample_with_identity(
        original_evidence,
        "kyro_sli_failure_ratio",
        target,
    )
    collected_request_rate, collected_request_rate_identity = metric_sample_with_identity(
        original_evidence,
        "kyro_sli_request_rate",
        target,
    )
    failure_ratio_before = finite_float(params.get("verification_failure_ratio_before"))
    if failure_ratio_before is None:
        failure_ratio_before = collected_failure_ratio
    approved_failure_ratio_identity = mapping(
        params.get("verification_failure_ratio_metric_identity")
    )
    failure_ratio_identity = (
        metric_identity(approved_failure_ratio_identity)
        if approved_failure_ratio_identity
        else collected_failure_ratio_identity
    )
    request_rate_baseline = finite_float(params.get("verification_request_rate_baseline"))
    if request_rate_baseline is None:
        request_rate_baseline = collected_request_rate
    approved_request_rate_identity = mapping(
        params.get("verification_request_rate_metric_identity")
    )
    request_rate_identity = (
        metric_identity(approved_request_rate_identity)
        if approved_request_rate_identity
        else collected_request_rate_identity
    )
    load_tolerance_ratio = bounded_ratio(
        env(RECOVERY_LOAD_TOLERANCE_RATIO_ENV, "0.2"),
        default=0.2,
    )
    protected_before = protected_workloads(original_evidence, target) or []
    approved_protected_baseline = params.get("protected_baseline")
    protected_baseline = (
        [dict(item) for item in approved_protected_baseline if isinstance(item, Mapping)]
        if isinstance(approved_protected_baseline, list)
        else []
    )
    approved_session_baseline = params.get("protected_session_baseline")
    protected_session_baseline = (
        [dict(item) for item in approved_session_baseline if isinstance(item, Mapping)]
        if isinstance(approved_session_baseline, list)
        else []
    )
    requires_protected_continuity = (
        text_value(params.get("verification_contract")) == "protected_workload_continuity"
    )
    # Preflight blockers are advisory. Re-evaluate from the latest persisted
    # evidence when the PR is created so a newly collected baseline can unlock
    # the lifecycle instead of leaving a stale blocker attached forever.
    missing_prerequisites: list[str] = []
    if requires_protected_continuity and (not protected_baseline or not protected_session_baseline):
        missing_prerequisites.extend(
            [
                "metadata:current_workload_snapshots",
                "metrics:kyro_continuity_active_sessions",
            ]
        )
    approved_alert_before = mapping(params.get("verification_alert_before"))
    if approved_alert_before:
        before = dict(approved_alert_before)
    else:
        alerts = await ctx.db.list_alert_events(
            evt.workspace_id,
            rule_name=STANDARD_SLI_ALERT_NAME,
            source="alertmanager",
            incident_ids=tuple(
                sorted(
                    {
                        value
                        for value in (
                            ctx.correlation_id,
                            str(record.get("incident_id") or ""),
                        )
                        if value
                    }
                )
            ),
            limit=10,
        )
        before = before_alert_snapshot(
            alerts,
            target=target,
            correlation_id=ctx.correlation_id,
            incident_id=str(record.get("incident_id") or ""),
            expected_series_identity=failure_ratio_identity,
        )
    before.update(
        {
            "captured_at": now.isoformat(),
            "failure_ratio": failure_ratio_before,
            "request_rate": request_rate_baseline,
            "metrics_evidence_ref": original_evidence.get("object_ref"),
            "protected_workloads": protected_before,
            "protected_active_sessions": protected_session_baseline,
        }
    )
    minimum_seconds, maximum_seconds = verification_window_seconds()
    expected_replicas = nonnegative_int(params.get("expected_replicas"))
    evidence_cadence_seconds = nonnegative_int(params.get("verification_evidence_cadence_seconds"))
    if evidence_cadence_seconds is None:
        registration = await ctx.db.get_cluster_registration(
            evt.workspace_id,
            target.get("cluster_id", ""),
        )
        registration_settings = mapping(
            registration.get("settings") if isinstance(registration, Mapping) else None
        )
        evidence_cadence_seconds = nonnegative_int(
            registration_settings.get("evidence_interval_seconds")
        )
    if failure_ratio_before is None:
        missing_prerequisites.append("metrics:kyro_sli_failure_ratio")
    if not failure_ratio_identity:
        missing_prerequisites.append("metrics:kyro_sli_failure_ratio_identity")
    if request_rate_baseline is None or request_rate_baseline <= 0:
        missing_prerequisites.append("metrics:kyro_sli_request_rate")
    if not request_rate_identity:
        missing_prerequisites.append("metrics:kyro_sli_request_rate_identity")
    threshold = finite_float(before.get("threshold"))
    if before.get("available") is not True or threshold is None:
        missing_prerequisites.append("alertmanager:original_exact_alert")
    if expected_replicas is None:
        missing_prerequisites.append("gitops:approved_replica_baseline")
    if not authorized_changes:
        missing_prerequisites.append("gitops:approved_change_contract")
    if evidence_cadence_seconds is None or evidence_cadence_seconds <= 0:
        missing_prerequisites.append("cluster:evidence_cadence")
    missing_prerequisites = sorted(set(missing_prerequisites))
    current_lifecycle = mapping(payload.get("lifecycle"))
    attempt = dict(mapping(current_lifecycle.get("attempt")))
    attempt_id = text_value(attempt.get("id"))
    pr_lifecycle: JsonObject = {
        "url": evt.pr_url,
        "number": evt.pr_number,
        "node_id": evt.pr_node_id,
        "head_ref": evt.head_ref,
        "head_sha": evt.head_sha,
        "repo_ref": evt.repo_ref,
        "repository_id": evt.repository_id,
        "binding_id": evt.binding_id,
        "application_id": evt.application_id,
        "base_branch": evt.base_branch,
        "manifest_path": evt.manifest_path,
        "environment": evt.environment,
        "cluster_id": target.get("cluster_id"),
        "patch_sha256": evt.patch_sha256,
        "tracked_at": now.isoformat(),
    }
    if attempt_id:
        pr_lifecycle["attempt_id"] = attempt_id
    lifecycle: JsonObject = {
        "phase": RECOVERY_STATUS_PR_OPEN,
        "attempt": attempt,
        "pr": pr_lifecycle,
        "verification": {
            "minimum_seconds": minimum_seconds,
            "maximum_seconds": maximum_seconds,
            "expected": {
                "failure_ratio_max": threshold,
                "request_rate_baseline": request_rate_baseline,
                "request_rate_tolerance_ratio": load_tolerance_ratio,
                "replicas": expected_replicas,
                "failure_ratio_metric_identity": failure_ratio_identity,
                "request_rate_metric_identity": request_rate_identity,
                "evidence_cadence_seconds": evidence_cadence_seconds,
                "protected_workloads": (
                    len(protected_baseline) if requires_protected_continuity else 0
                ),
            },
            "before": before,
            "protected_baseline": protected_baseline,
            "protected_session_baseline": protected_session_baseline,
            "target": target,
            "status": ("merge_blocked" if missing_prerequisites else "waiting_for_merge"),
            "blockers": missing_prerequisites,
        },
        "authorization": {
            "target": target,
            "changes": authorized_changes,
        },
    }
    saved = await ctx.db.update_recovery_plan_lifecycle_if_status(
        str(record["plan_id"]),
        evt.workspace_id,
        expected_statuses=(RECOVERY_STATUS_SELECTED,),
        status=RECOVERY_STATUS_PR_OPEN,
        lifecycle=lifecycle,
    )
    if saved is None:
        return
    yield RecoveryPrTrackedBody(
        plan_id=str(record["plan_id"]),
        incident_id=str(record["incident_id"]),
        pr_url=evt.pr_url,
        repository_id=evt.repository_id,
        repo_ref=evt.repo_ref,
        binding_id=evt.binding_id,
        application_id=evt.application_id,
        base_branch=evt.base_branch,
        workspace_id=evt.workspace_id,
    )


@app.on(WorkflowRunCompletedBody)
async def on_recovery_deploy_completed(
    evt: WorkflowRunCompletedBody,
    ctx: EventContext[RecoveryPlanStore],
) -> AsyncIterator[EventBody]:
    started = await start_recovery_verification(
        ctx=ctx,
        workspace_id=evt.workspace_id,
        workflow_run_id=evt.workflow_run_id,
        binding_id=evt.binding_id,
        application_id=evt.application_id,
    )
    if started is not None:
        yield started


@app.on(RecoveryPrMergedBody)
async def on_recovery_pr_merged(
    evt: RecoveryPrMergedBody,
    ctx: EventContext[RecoveryPlanStore],
) -> AsyncIterator[EventBody]:
    """Reconcile a workflow that may have succeeded before the PR webhook.

    GitHub can deliver the base-branch push before ``pull_request.closed``.
    The deterministic workflow may therefore already be terminal when the
    recovery enters ``deploy_pending``.  Reading the persisted run here makes
    both webhook orders converge without relying on a duplicate completion
    event.
    """

    started = await start_recovery_verification(
        ctx=ctx,
        workspace_id=evt.workspace_id,
        workflow_run_id=evt.workflow_run_id,
        binding_id=evt.binding_id,
        application_id=evt.application_id,
    )
    if started is not None:
        yield started


async def start_recovery_verification(
    *,
    ctx: EventContext[RecoveryPlanStore],
    workspace_id: str,
    workflow_run_id: str,
    binding_id: str,
    application_id: str,
) -> RecoveryVerificationStartedBody | None:
    record = await ctx.db.get_recovery_plan_for_workflow(
        workspace_id,
        workflow_run_id,
        binding_id,
        application_id,
    )
    if not isinstance(record, dict) or record.get("status") != RECOVERY_STATUS_DEPLOY_PENDING:
        return None
    payload = mapping(record.get("payload"))
    lifecycle = dict(mapping(payload.get("lifecycle")))
    merge = mapping(lifecycle.get("merge"))
    workflow = await ctx.db.get_workflow_run(workflow_run_id)
    if (
        not isinstance(workflow, dict)
        or text_value(merge.get("workflow_run_id")) != workflow_run_id
        or text_value(merge.get("binding_id")) != binding_id
        or text_value(merge.get("application_id")) != application_id
        or text_value(merge.get("merge_commit_sha")) != text_value(workflow.get("commit_sha"))
        or text_value(merge.get("cluster_id")) != text_value(workflow.get("cluster_id"))
        or text_value(workflow.get("workspace_id")) != workspace_id
        or text_value(workflow.get("binding_id")) != binding_id
        or text_value(workflow.get("application_id")) != application_id
        or text_value(workflow.get("status")) != "succeeded"
    ):
        return None
    now = normalized_utc(await ctx.db.current_database_time())
    verification = dict(mapping(lifecycle.get("verification")))
    maximum_seconds = int(verification.get("maximum_seconds") or DEFAULT_MAXIMUM_SECONDS)
    verification.update(
        {
            "status": RECOVERY_STATUS_VERIFICATION_PENDING,
            "started_at": now.isoformat(),
            "deadline_at": verification_deadline(now, maximum_seconds).isoformat(),
            "healthy_since": None,
            "last_reason_code": "waiting_for_post_deploy_evidence",
            "last_reason": "배포 성공 후 첫 정상화 evidence 창을 기다립니다.",
        }
    )
    lifecycle.update(
        {
            "phase": RECOVERY_STATUS_VERIFICATION_PENDING,
            "verification": verification,
        }
    )
    saved = await ctx.db.update_recovery_plan_lifecycle_if_status(
        str(record["plan_id"]),
        workspace_id,
        expected_statuses=(RECOVERY_STATUS_DEPLOY_PENDING,),
        status=RECOVERY_STATUS_VERIFICATION_PENDING,
        lifecycle=lifecycle,
    )
    if saved is None:
        return None
    return RecoveryVerificationStartedBody(
        plan_id=str(record["plan_id"]),
        incident_id=str(record["incident_id"]),
        workflow_run_id=workflow_run_id,
        started_at=verification["started_at"],
        deadline_at=verification["deadline_at"],
        expected=dict(mapping(verification.get("expected"))),
        before=dict(mapping(verification.get("before"))),
        workspace_id=workspace_id,
    )


@app.on(WorkflowRunFailedBody)
async def on_recovery_deploy_failed(
    evt: WorkflowRunFailedBody,
    ctx: EventContext[RecoveryPlanStore],
) -> AsyncIterator[EventBody]:
    record = await ctx.db.get_recovery_plan_for_workflow(
        evt.workspace_id,
        evt.workflow_run_id,
        evt.binding_id,
        evt.application_id,
    )
    if not isinstance(record, dict) or record.get("status") != RECOVERY_STATUS_DEPLOY_PENDING:
        return
    payload = mapping(record.get("payload"))
    lifecycle = dict(mapping(payload.get("lifecycle")))
    lifecycle["phase"] = RECOVERY_STATUS_FAILED
    lifecycle["failure"] = {
        "reason_code": "recovery_deploy_failed",
        "reason": evt.reason,
        "workflow_run_id": evt.workflow_run_id,
    }
    saved = await ctx.db.update_recovery_plan_lifecycle_if_status(
        str(record["plan_id"]),
        evt.workspace_id,
        expected_statuses=(RECOVERY_STATUS_DEPLOY_PENDING,),
        status=RECOVERY_STATUS_FAILED,
        lifecycle=lifecycle,
    )
    if saved is None:
        return
    verification = mapping(lifecycle.get("verification"))
    yield RecoveryVerificationFailedBody(
        plan_id=str(record["plan_id"]),
        incident_id=str(record["incident_id"]),
        reason_code="recovery_deploy_failed",
        reason=evt.reason,
        evidence_ref=str(record.get("evidence_ref") or "unknown"),
        before=dict(mapping(verification.get("before"))),
        workspace_id=evt.workspace_id,
    )


def apply_verification_terminal_state(
    lifecycle: JsonObject,
    decision: VerificationDecision,
    *,
    evidence_ref: str,
) -> None:
    """Persist the retry identity whenever evidence ends verification."""

    if decision.status == "failed":
        lifecycle["failure"] = {
            "reason_code": decision.reason_code,
            "reason": decision.reason,
            "evidence_ref": evidence_ref,
        }
    elif decision.status == "completed":
        lifecycle.pop("failure", None)


@app.on(ClusterEvidenceReceivedBody)
async def on_recovery_verification_evidence(
    evt: ClusterEvidenceReceivedBody,
    ctx: EventContext[RecoveryPlanStore],
) -> AsyncIterator[EventBody | CorrelatedResult]:
    if not evt.evidence_key:
        return
    evidence = await ctx.db.get_evidence_window_payload_for_workspace(
        evt.workspace_id,
        evt.evidence_key,
    )
    if not isinstance(evidence, dict):
        return
    for result in await track_image_pull_evidence(evt, evidence, ctx):
        yield result
    records = await ctx.db.list_recovery_verification_plans(
        evt.workspace_id,
        evt.cluster_id,
        limit=100,
    )
    records = [
        record
        for record in records
        if mapping(record.get("payload")).get("root_cause") != "wrong_image_tag"
    ]
    if not records:
        return
    now = normalized_utc(await ctx.db.current_database_time())
    for record in records:
        payload = mapping(record.get("payload"))
        lifecycle = dict(mapping(payload.get("lifecycle")))
        verification = dict(mapping(lifecycle.get("verification")))
        before_snapshot = mapping(verification.get("before"))
        original_event_id = str(before_snapshot.get("alert_event_id") or "")
        subject_key = str(before_snapshot.get("subject_key") or "")
        verification_started_at = parse_datetime(verification.get("started_at"))
        alerts: list[JsonObject] = []
        if original_event_id and subject_key and verification_started_at is not None:
            original_alerts = await ctx.db.list_alert_events(
                evt.workspace_id,
                event_ids=(original_event_id,),
                limit=1,
            )
            refire_alerts = await ctx.db.list_alert_events(
                evt.workspace_id,
                from_time=verification_started_at,
                rule_name=STANDARD_SLI_ALERT_NAME,
                source="alertmanager",
                subject_key=subject_key,
                limit=500,
            )
            alerts_by_id = {
                str(alert.get("event_id") or ""): dict(alert)
                for alert in (*original_alerts, *refire_alerts)
                if str(alert.get("event_id") or "")
            }
            alerts = list(alerts_by_id.values())
        decision = evaluate_recovery_evidence(
            plan_payload=payload,
            lifecycle=lifecycle,
            evidence=evidence,
            alerts=alerts,
            now=now,
        )
        verification.update(
            {
                "status": decision.status,
                "healthy_since": decision.healthy_since,
                "last_healthy_observed_at": decision.last_healthy_observed_at,
                "distinct_evidence_count": decision.distinct_evidence_count,
                "last_evidence_key": decision.last_evidence_key,
                "last_reason_code": decision.reason_code,
                "last_reason": decision.reason,
                "after": decision.after,
            }
        )
        current_session_samples = decision.after.get("protected_active_sessions")
        if isinstance(current_session_samples, list):
            verification["last_session_samples"] = [
                dict(item) for item in current_session_samples if isinstance(item, Mapping)
            ]
        lifecycle["verification"] = verification
        target_status = (
            RECOVERY_STATUS_COMPLETED
            if decision.status == "completed"
            else RECOVERY_STATUS_FAILED
            if decision.status == "failed"
            else RECOVERY_STATUS_VERIFICATION_PENDING
        )
        lifecycle["phase"] = target_status
        apply_verification_terminal_state(
            lifecycle,
            decision,
            evidence_ref=evt.evidence_key,
        )
        saved = await ctx.db.update_recovery_plan_lifecycle_if_status(
            str(record["plan_id"]),
            evt.workspace_id,
            expected_statuses=(RECOVERY_STATUS_VERIFICATION_PENDING,),
            status=target_status,
            lifecycle=lifecycle,
        )
        if saved is None:
            continue
        before = dict(mapping(verification.get("before")))
        yield RecoveryVerificationUpdatedBody(
            plan_id=str(record["plan_id"]),
            incident_id=str(record["incident_id"]),
            status=decision.status,
            reason_code=decision.reason_code,
            reason=decision.reason,
            evidence_ref=evt.evidence_key,
            before=before,
            after=decision.after,
            workspace_id=evt.workspace_id,
        )
        if decision.status == "failed":
            yield RecoveryVerificationFailedBody(
                plan_id=str(record["plan_id"]),
                incident_id=str(record["incident_id"]),
                reason_code=decision.reason_code,
                reason=decision.reason,
                evidence_ref=evt.evidence_key,
                before=before,
                after=decision.after,
                workspace_id=evt.workspace_id,
            )
        elif decision.status == "completed":
            target = dict(mapping(verification.get("target"))) or recovery_target(payload)
            yield IncidentResolvedBody(
                incident_id=str(record["incident_id"]),
                cluster_id=target["cluster_id"],
                reason=decision.reason,
                evidence_ref=evt.evidence_key,
                recovery_plan_id=str(record["plan_id"]),
                before=before,
                after=decision.after,
                workspace_id=evt.workspace_id,
            )


def selected_recovery_candidate(record: JsonObject) -> JsonObject:
    payload = mapping(record.get("payload"))
    candidates = payload.get("candidates")
    selected_action_id = text_value(record.get("selected_action_id"))
    if not isinstance(candidates, list) or not selected_action_id:
        return {}
    matches = [
        dict(candidate)
        for candidate in candidates
        if isinstance(candidate, dict)
        and text_value(candidate.get("action_id")) == selected_action_id
    ]
    return matches[0] if len(matches) == 1 else {}


def recovery_approval_identity(plan_id: str, action_id: str) -> str:
    digest = hashlib.sha256(f"{plan_id}|{action_id}|recovery-action".encode()).hexdigest()[:32]
    return f"approval-{digest}"


async def recovery_approval_record(
    ctx: EventContext[RecoveryPlanStore],
    record: JsonObject,
    workspace_id: str,
) -> JsonObject:
    plan_id = text_value(record.get("plan_id"))
    action_id = text_value(record.get("selected_action_id"))
    if not plan_id or not action_id:
        return {}
    approval = await ctx.db.get_workflow_approval(
        recovery_approval_identity(plan_id, action_id),
        workspace_id,
    )
    return approval if isinstance(approval, dict) else {}


def selected_candidate_params(approval: Mapping[str, object]) -> JsonObject:
    details = mapping(approval.get("details"))
    selected = mapping(details.get("selected_candidate"))
    draft = mapping(selected.get("draft"))
    return dict(mapping(draft.get("params")))


def approved_change_contract(value: object) -> list[JsonObject]:
    if not isinstance(value, list) or not value:
        return []
    changes: list[JsonObject] = []
    paths: set[str] = set()
    for item in value:
        if not isinstance(item, Mapping):
            return []
        field_path = text_value(item.get("field_path"))
        if (
            not field_path
            or field_path in paths
            or "current_value" not in item
            or "desired_value" not in item
        ):
            return []
        paths.add(field_path)
        changes.append(
            {
                "field_path": field_path,
                "current_value": item.get("current_value"),
                "desired_value": item.get("desired_value"),
            }
        )
    return changes


def selected_recovery_target(
    payload: Mapping[str, object],
    selected: Mapping[str, object],
) -> dict[str, str]:
    """Resolve the workload from the action the operator actually selected."""

    target = mapping(payload.get("target"))
    draft = mapping(selected.get("draft"))
    params = mapping(draft.get("params"))
    return {
        "cluster_id": text_value(
            params.get("cluster_id") or draft.get("cluster_id") or target.get("cluster_id")
        ),
        "namespace": text_value(
            params.get("namespace") or draft.get("namespace") or target.get("namespace")
        ),
        "resource_kind": text_value(
            params.get("resource_kind") or draft.get("resource_kind") or target.get("resource_kind")
        ),
        "resource_name": text_value(
            params.get("resource_name") or draft.get("resource_name") or target.get("resource_name")
        ),
    }


def safe_pr_identity_error(
    evt: SafePrCreatedBody,
    params: Mapping[str, object],
) -> str | None:
    if (
        evt.mode != "github_rest"
        or not evt.pr_url.startswith("https://")
        or evt.pr_number is None
        or evt.pr_number <= 0
        or not evt.pr_node_id
        or not evt.head_ref
        or not GITHUB_SHA_RE.fullmatch(evt.head_sha)
    ):
        return "생성된 PR의 number/node/head SHA가 없어 merge를 안전하게 추적할 수 없습니다."
    expected = {
        "repository_id": evt.repository_id,
        "binding_id": evt.binding_id,
        "application_id": evt.application_id,
        "repo_ref": evt.repo_ref,
        "base_branch": evt.base_branch,
        "commit_sha": evt.commit_sha,
    }
    if any(text_value(params.get(key)) != value for key, value in expected.items()):
        return "생성된 PR identity가 승인된 GitOps 권위 context와 일치하지 않습니다."
    return None


def nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def bounded_ratio(value: object, *, default: float) -> float:
    parsed = finite_float(value)
    if parsed is None:
        return default
    return max(0.0, min(parsed, 0.5))


@app.on(RcaActionRequiredBody)
async def on_rca_action_required(
    evt: RcaActionRequiredBody,
    ctx: EventContext[RecoveryPlanStore],
) -> AsyncIterator[EventBody]:
    if evt.reason_code in RETRYABLE_RECOVERY_REASON_CODES:
        allowed = await reopen_recovery_selection(
            ctx,
            evt.workspace_id,
            evt.diagnostics,
        )
        if not allowed:
            return
    yield RcaFollowupRequiredBody(
        reason_code=evt.reason_code,
        summary=followup_summary(evt.reason_code, evt.reason),
        evidence_ref=evt.evidence_ref,
        workspace_id=evt.workspace_id,
        severity=evt.severity,
        missing_evidence=evt.missing_evidence,
        next_actions=normalize_next_actions(evt.next_actions, evt.missing_evidence),
        diagnostics={
            **evt.diagnostics,
            "source_event": evt.__subject__,
            "agent_safe": True,
        },
    )


@app.on(SafePrFailedBody)
async def on_recovery_safe_pr_failed(
    evt: SafePrFailedBody,
    ctx: EventContext[RecoveryPlanStore],
) -> AsyncIterator[EventBody]:
    identity = await failed_safe_pr_recovery_identity(evt, ctx)
    if identity is None:
        return
    plan_id, action_id = identity
    record = await ctx.db.get_recovery_plan_by_correlation(
        ctx.correlation_id,
        evt.workspace_id,
    )
    if (
        not isinstance(record, dict)
        or text_value(record.get("plan_id")) != plan_id
        or record.get("status") != RECOVERY_STATUS_SELECTED
        or text_value(record.get("selected_action_id")) != action_id
    ):
        return
    payload = mapping(record.get("payload"))
    lifecycle = dict(mapping(payload.get("lifecycle")))
    lifecycle["phase"] = "failed"
    lifecycle["failure"] = {
        "stage": "safe_pr",
        "reason_code": evt.reason_code,
        "reason": evt.reason,
        "provider_stage": evt.stage,
        "approval_ref": text_value(evt.details.get("approval_ref")),
        "workflow_run_id": evt.workflow_run_id,
        "commit_sha": evt.commit_sha,
        "evidence_ref": evt.evidence_ref,
    }
    failed = await ctx.db.update_recovery_plan_lifecycle_if_status(
        plan_id,
        evt.workspace_id,
        expected_statuses=(RECOVERY_STATUS_SELECTED,),
        status="failed",
        lifecycle=lifecycle,
    )
    if failed is None:
        return
    yield RcaFollowupRequiredBody(
        reason_code=evt.reason_code,
        summary=followup_summary(evt.reason_code, evt.reason),
        evidence_ref=evt.evidence_ref or "unknown",
        workspace_id=evt.workspace_id,
        severity=SEVERITY_WARNING,
        next_actions=[
            {
                "action_type": "retry_recovery_action",
                "description": "차단 원인을 해결한 뒤 저장된 복구 조치를 최신 권위 context로 다시 시도합니다.",
            }
        ],
        diagnostics={
            **evt.details,
            "plan_id": plan_id,
            "action_id": action_id,
            "source_event": evt.__subject__,
            "agent_safe": True,
            "retryable": True,
        },
    )


async def failed_safe_pr_recovery_identity(
    evt: SafePrFailedBody,
    ctx: EventContext[RecoveryPlanStore],
) -> tuple[str, str] | None:
    approval_ref = evt.details.get("approval_ref")
    if isinstance(approval_ref, str) and approval_ref.strip():
        approval = await ctx.db.get_workflow_approval(
            approval_ref.strip(),
            evt.workspace_id,
        )
        if isinstance(approval, dict):
            approval_details = approval.get("details")
            if isinstance(approval_details, dict):
                plan_id = text_value(approval_details.get("recovery_plan_id"))
                action_id = text_value(approval_details.get("recovery_action_id"))
                if plan_id and action_id:
                    return plan_id, action_id
    record = await ctx.db.get_recovery_plan_by_correlation(
        ctx.correlation_id,
        evt.workspace_id,
    )
    if not isinstance(record, dict) or record.get("status") != "selected":
        return None
    plan_id = text_value(record.get("plan_id"))
    action_id = text_value(record.get("selected_action_id"))
    if not plan_id or not action_id:
        return None
    return plan_id, action_id


async def reopen_recovery_selection(
    ctx: EventContext[RecoveryPlanStore],
    workspace_id: str,
    diagnostics: JsonObject,
) -> bool:
    plan_id = text_value(diagnostics.get("plan_id"))
    action_id = text_value(diagnostics.get("action_id"))
    if not plan_id or not action_id:
        return True
    reopened = await ctx.db.reopen_recovery_plan_action(
        plan_id,
        workspace_id,
        action_id,
    )
    if reopened:
        return True
    record = await ctx.db.get_recovery_plan_by_correlation(
        ctx.correlation_id,
        workspace_id,
    )
    return bool(
        isinstance(record, dict)
        and text_value(record.get("plan_id")) == plan_id
        and record.get("status") == "selection_requested"
        and record.get("selected_action_id") is None
    )


def text_value(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


@app.on(PipelineContractFailedBody)
async def on_pipeline_contract_failed(evt: PipelineContractFailedBody) -> AsyncIterator[EventBody]:
    diagnostics = evt.diagnostics or {}
    reason_code = str(diagnostics.get("reason_code") or "context_missing")
    yield RcaFollowupRequiredBody(
        reason_code=reason_code,
        summary=followup_summary(reason_code, evt.reason),
        evidence_ref=evt.evidence_ref or "unknown",
        workspace_id=evt.workspace_id,
        severity=evt.severity,
        next_actions=[
            {
                "action_type": "fix_pipeline_contract",
                "contract": evt.contract,
                "consumer": evt.consumer,
                "description": "upstream 이벤트 payload가 consumer 계약을 만족하도록 수정합니다.",
            }
        ],
        diagnostics={
            **diagnostics,
            "contract": evt.contract,
            "consumer": evt.consumer,
            "source_event": evt.__subject__,
            "agent_safe": True,
        },
    )


@app.on(RcaAiFallbackRequestedBody)
async def on_ai_fallback_requested(evt: RcaAiFallbackRequestedBody) -> AsyncIterator[EventBody]:
    yield RcaFollowupRequiredBody(
        reason_code="ai_fallback_required",
        summary=followup_summary("ai_fallback_required", evt.reason),
        evidence_ref=evt.evidence_ref,
        workspace_id=evt.workspace_id,
        severity=SEVERITY_WARNING,
        incident=evt.incident,
        missing_evidence=evt.missing_evidence,
        next_actions=[
            {
                "action_type": "run_llm_rca_agent",
                "description": "LLM RCA agent가 evidence bundle과 missing evidence를 검토합니다.",
            }
        ],
        diagnostics={
            "source_event": evt.__subject__,
            "evidence_bundle": evt.evidence_bundle.to_body(),
            "agent_safe": True,
        },
    )


if __name__ == "__main__":
    app.run()
