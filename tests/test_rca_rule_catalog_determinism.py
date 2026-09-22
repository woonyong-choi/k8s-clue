"""실제 룰 카탈로그(YAML)로 도는 RCA 판정의 결정성과 정지 조건.

이 저장소의 핵심 주장은 "원인 판정이 versioned rule 밖으로 나가지 않는다"이다.
기존 테스트는 파이프라인 배선과 이벤트 모양을 검사했을 뿐,
`causes/catalog/*.yaml` 을 실제로 적재해 판정까지 가는 경로는 검사하지 않았다.
"""

from __future__ import annotations

from domains.rca.events import EvidenceBundle, EvidenceItem, IncidentRecord
from services.ai.agent.causes.engine import (
    NO_MATCHING_RULE_MESSAGE,
    analyze_root_cause,
    evaluate_causes,
    plan_causes,
)

EVIDENCE_REF = "object://evidence/correlation-1.json"


def incident(symptom: str) -> IncidentRecord:
    return IncidentRecord(
        incident_id="incident-1",
        cluster_id="cluster-1",
        resource_kind="Deployment",
        resource_name="payment-api",
        namespace="payments",
        symptom=symptom,
        severity="critical",
        first_seen_at="2026-09-01T00:00:00Z",
        summary="Pod가 이미지를 받지 못해 기동하지 못한다.",
    )


def bundle(*event_messages: str) -> EvidenceBundle:
    return EvidenceBundle(
        incident_id="incident-1",
        items=[
            EvidenceItem(
                source="kubernetes",
                name="cluster_resource_state",
                value={
                    "pods": [
                        {
                            "name": "payment-api-7d9f",
                            "waiting_reasons": ["ImagePullBackOff"],
                            "containers": [{"name": "api"}],
                        }
                    ],
                    "events": [
                        {"reason": "Failed", "message": message} for message in event_messages
                    ],
                },
                summary="ImagePullBackOff 상태의 Pod 1개",
                evidence_ref=EVIDENCE_REF,
                check_id="kubernetes:cluster_resource_state",
            )
        ],
        missing_evidence=[],
        complete=True,
    )


def verdict(symptom: str, *event_messages: str):
    plan = plan_causes(incident(symptom), bundle(*event_messages), EVIDENCE_REF)
    evaluations = evaluate_causes(plan.candidates, bundle(*event_messages), plan.rule_missing)
    return analyze_root_cause(evaluations)


def test_catalog_rule_selects_the_same_cause_for_the_same_evidence() -> None:
    message = 'Failed to pull image "registry/payment-api:v9": manifest unknown'

    first = verdict("ImagePullBackOff", message)
    second = verdict("ImagePullBackOff", message)

    assert first.selected_candidate_id == "wrong_image_tag"
    assert first.confidence == 1.0
    assert first == second, "같은 증거에 두 번 다른 판정이 나오면 결정론적 룰이 아니다"


def test_catalog_rule_discriminates_auth_failure_from_missing_tag() -> None:
    denied = verdict(
        "ImagePullBackOff",
        'Failed to pull image "registry/payment-api:v9": pull access denied',
    )

    assert denied.selected_candidate_id == "missing_image_pull_secret"


def test_no_discriminating_event_cannot_finalize_a_cause() -> None:
    """증거 소스만 있고 판별 신호가 없으면 원인을 확정하지 않는다.

    카탈로그 주석이 적어 둔 회귀 방어선 — registry_unavailable 은 metadata 없이도
    완결 가능한 유일한 후보였고, 신호가 없으면 항상 1.0 으로 뽑히던 적이 있다.
    """
    blocked = verdict("ImagePullBackOff", "Back-off pulling image")

    assert blocked.selected_candidate_id == "none"
    assert blocked.root_cause == "insufficient_evidence"
    assert blocked.confidence == 0.0


def test_symptom_outside_the_catalog_stops_instead_of_guessing() -> None:
    unknown = verdict("CosmicRayBitFlip", "everything is on fire")

    assert unknown.root_cause == "unknown"
    assert unknown.confidence == 0.0
    assert unknown.reason == NO_MATCHING_RULE_MESSAGE
    assert "matching_cause_rule" in unknown.missing_evidence
