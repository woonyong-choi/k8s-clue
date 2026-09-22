"""README 에 실린 RCA 엔진 수치가 지금 코드에서 그대로 나오는지 확인한다.

측정 결과를 파일로 커밋해 두면 코드가 바뀌어도 숫자는 그대로 남는다.
그래서 `evals/results.json` 은 문서가 아니라 **회귀 기준**으로 다룬다.
룰을 하나 추가하거나 신호를 바꾸면 이 테스트가 먼저 깨지고, `make rca-eval` 로
다시 측정해 커밋해야 한다.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from domains.rca.events import EvidenceBundle, EvidenceItem, IncidentRecord
from services.ai.agent.causes.engine import (
    analyze_root_cause,
    evaluate_causes,
    plan_causes,
)

EVALS = Path(__file__).resolve().parents[1] / "evals"
NON_SPECIFIC = {"unknown", "insufficient_evidence", "분석 가능한 원인 후보 없음"}


def measure() -> dict[str, object]:
    golden = json.loads((EVALS / "golden_set.json").read_text(encoding="utf-8"))
    rows = []
    for scenario in golden["scenarios"]:
        incident = IncidentRecord(**scenario["incident"])
        bundle = EvidenceBundle(
            incident_id=incident.incident_id,
            items=[
                EvidenceItem(
                    source=item["source"],
                    name=item["name"],
                    value=item["value"],
                    summary=item["summary"],
                    evidence_ref=item["evidence_ref"],
                    check_id=item["check_id"],
                )
                for item in scenario["evidence_items"]
            ],
            missing_evidence=[],
            complete=True,
        )
        plan = plan_causes(incident, bundle, evidence_ref=f"eval://{scenario['scenario_id']}")
        detail = analyze_root_cause(
            evaluate_causes(plan.candidates, bundle, plan.rule_missing)
        )
        rows.append(
            {
                "kind": scenario["kind"],
                "golden": scenario["golden_candidate_id"],
                "predicted": detail.root_cause,
                "planned": plan.candidate_count,
                "rule_missing": plan.rule_missing is not None,
            }
        )
    positives = [row for row in rows if row["kind"] == "positive"]
    healthy = [row for row in rows if row["kind"] == "healthy"]
    return {
        "correct": sum(row["predicted"] == row["golden"] for row in positives),
        "positives": len(positives),
        "covered": sum(not row["rule_missing"] and row["planned"] > 0 for row in rows),
        "scenarios": len(rows),
        "false_positives": sum(row["predicted"] not in NON_SPECIFIC for row in healthy),
        "healthy": len(healthy),
        "confusions": Counter(
            (row["golden"], row["predicted"]) for row in positives if row["predicted"] != row["golden"]
        ),
    }


def test_published_rca_metrics_still_hold() -> None:
    published = json.loads((EVALS / "results.json").read_text(encoding="utf-8"))
    measured = measure()

    assert measured["positives"] == published["accuracy"]["total"]
    assert measured["correct"] == published["accuracy"]["correct"]
    assert measured["covered"] == published["coverage"]["covered"]
    assert measured["scenarios"] == published["coverage"]["total"]
    assert measured["false_positives"] == published["false_positive"]["count"]
    assert measured["healthy"] == published["false_positive"]["total"]


def test_no_catalog_candidate_is_shadowed_by_another() -> None:
    """골든셋의 각 candidate 는 자기 신호를 받으면 자기 자신으로 판정돼야 한다.

    이것이 이 평가가 실제로 증명하는 것이다 — 룰이 정확하다는 뜻이 아니라,
    어떤 후보도 다른 후보에 가려져 도달 불가능해지지 않았다는 뜻이다.
    """
    measured = measure()

    assert measured["confusions"] == Counter(), f"가려진 후보: {measured['confusions']}"


def test_healthy_evidence_never_produces_a_specific_cause() -> None:
    measured = measure()

    assert measured["false_positives"] == 0
    assert measured["healthy"] > 0
