from domains.rca.events import (
    HealingActionDraft,
    RcaActionRequiredBody,
    RecoveryActionCandidate,
    RecoveryPlan,
)
from domains.scm.events import SafePrFilePatch, SafePrRequestedBody
from packages.contracts.gitops_authority import GitOpsAuthorityContext
from services.ai.agent.recovery.dispatch import (
    build_safe_pr_request_body,
    recovery_safe_pr_body,
    recovery_safe_pr_title,
)


def recovery_candidate(**overrides: object) -> RecoveryActionCandidate:
    values: dict[str, object] = {
        "action_id": "action-1",
        "title": "이전 Secret 버전으로 복원",
        "description": "정상 동작했던 데이터베이스 접속 정보로 Secret을 복원합니다.",
        "draft": HealingActionDraft(
            action_type="config_fix",
            namespace="payments",
            resource_kind="Deployment",
            resource_name="payment-api",
            reason="인증 실패 로그와 Secret 변경 시점이 일치합니다.",
            risk_level="low",
            dry_run=True,
            source_evidence=["evidence-1"],
            params={},
        ),
        "route": "safe_pr",
        "rank": 1,
        "score": 0.92,
        "risk_level": "low",
        "blast_radius": "payment-api Deployment",
        "approval_required": False,
        "prerequisites": ["이전 Secret 버전이 존재하는지 확인"],
        "validation_checks": [
            "새 Pod가 Ready 상태인지 확인",
            "데이터베이스 인증 오류가 더 이상 발생하지 않는지 확인",
        ],
        "rollback_plan": "문제가 지속되면 변경 전 Secret을 다시 적용합니다.",
        "evidence_refs": ["evidence-1"],
        "recommendation_reason": "가장 작은 영향 범위로 원인을 되돌릴 수 있습니다.",
        "expected_outcome": "인증 오류가 사라지고 Pod가 정상 상태로 전환됩니다.",
    }
    values.update(overrides)
    return RecoveryActionCandidate(**values)  # type: ignore[arg-type]


def recovery_plan(candidate: RecoveryActionCandidate) -> RecoveryPlan:
    return RecoveryPlan(
        plan_id="plan-1",
        incident_id="incident-1",
        evidence_ref="evidence-1",
        summary="데이터베이스 인증 실패로 Pod가 반복 재시작되었습니다.",
        target={
            "namespace": "payments",
            "resource_kind": "Deployment",
            "resource_name": "payment-api",
        },
        recommended_action_id=candidate.action_id,
        execution_route="safe_pr",
        selection_required=False,
        candidates=[candidate],
    )


def recovery_authority() -> GitOpsAuthorityContext:
    return GitOpsAuthorityContext(
        workspace_id="default",
        repository_id="repo-1",
        binding_id="binding-1",
        application_id="app-1",
        workflow_run_id="run-1",
        environment="production",
        cluster_id="cluster-1",
        manifest_path="deploy/k8s/payment-api.yaml",
        repo_ref="example/payments",
        base_branch="main",
        commit_sha="a" * 40,
        source_type="raw-yaml",
        source_manifest_sha256="sha256:" + "b" * 64,
        resource="Deployment/payment-api",
        desired_manifest={
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "payment-api", "namespace": "payments"},
        },
        changes=(),
        evidence={},
    )


def test_recovery_safe_pr_body_contains_operator_review_sections() -> None:
    candidate = recovery_candidate()

    body = recovery_safe_pr_body(recovery_plan(candidate), candidate)

    assert "## 복구 개요" in body
    assert "**대상:** `payments / Deployment / payment-api`" in body
    assert "**조치 위험도:** 낮음" in body
    assert "## 변경 내용" in body
    assert "## 사전 확인" in body
    assert "- [ ] 이전 Secret 버전이 존재하는지 확인" in body
    assert "## 적용 후 검증" in body
    assert "- [ ] 새 Pod가 Ready 상태인지 확인" in body
    assert "## 실패 시 복원" in body
    assert "<summary>추적 정보</summary>" in body
    assert "Kubernetes 장애 복구 파이프라인에서 생성된 PR입니다." in body


def test_recovery_safe_pr_title_collapses_whitespace_and_limits_length() -> None:
    candidate = recovery_candidate(
        title="  매우 긴 복구 조치  " * 20,
    )

    title = recovery_safe_pr_title(candidate)

    assert "\n" not in title
    assert len(title) <= 120
    assert title.endswith("…")


def test_recovery_safe_pr_always_uses_pull_request_delivery() -> None:
    patch = SafePrFilePatch(
        path="deploy/k8s/payment-api.yaml",
        content="apiVersion: apps/v1\nkind: Deployment\n",
    )

    for risk_level in ("low", "medium", "high"):
        candidate = recovery_candidate(risk_level=risk_level)

        request = build_safe_pr_request_body(
            recovery_plan(candidate),
            candidate,
            "default",
            [patch],
            recovery_authority(),
        )

        assert isinstance(request, SafePrRequestedBody)
        assert request.delivery == "pull_request"


def test_recovery_safe_pr_requires_gitops_authority() -> None:
    candidate = recovery_candidate()
    patch = SafePrFilePatch(
        path="deploy/k8s/payment-api.yaml",
        content="apiVersion: apps/v1\nkind: Deployment\n",
    )

    request = build_safe_pr_request_body(
        recovery_plan(candidate),
        candidate,
        "default",
        [patch],
    )

    assert isinstance(request, RcaActionRequiredBody)
    assert request.reason_code == "gitops_authority_unavailable"
