"""골든셋 시나리오를 실제 엔진(plan_causes → evaluate_causes → analyze_root_cause)에
통과시켜 accuracy / coverage / false positive 를 실측한다.

실행: make rca-eval (또는 uv run python evals/run_eval.py)
입력: evals/golden_set.json (build_golden_set.py 산출물)
출력: evals/results.json, evals/results.md
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from domains.rca.events import EvidenceBundle, EvidenceItem, IncidentRecord  # noqa: E402
from services.ai.agent.causes.engine import (  # noqa: E402
    analyze_root_cause,
    evaluate_causes,
    plan_causes,
)

EVALS_DIR = Path(__file__).resolve().parent
GOLDEN_PATH = EVALS_DIR / "golden_set.json"
NON_SPECIFIC_ROOT_CAUSES = {"unknown", "insufficient_evidence", "분석 가능한 원인 후보 없음"}


def to_incident(payload: dict) -> IncidentRecord:
    return IncidentRecord(**payload)


def to_bundle(incident_id: str, items_payload: list[dict]) -> EvidenceBundle:
    items = [
        EvidenceItem(
            source=item["source"],
            name=item["name"],
            value=item["value"],
            summary=item["summary"],
            evidence_ref=item["evidence_ref"],
            check_id=item["check_id"],
        )
        for item in items_payload
    ]
    return EvidenceBundle(
        incident_id=incident_id,
        items=items,
        missing_evidence=[],
        complete=True,
    )


def run_scenario(scenario: dict) -> dict:
    incident = to_incident(scenario["incident"])
    bundle = to_bundle(incident.incident_id, scenario["evidence_items"])
    plan = plan_causes(incident, bundle, evidence_ref=f"eval://{scenario['scenario_id']}")
    evaluations = evaluate_causes(plan.candidates, bundle, plan.rule_missing)
    detail = analyze_root_cause(evaluations)
    return {
        "scenario_id": scenario["scenario_id"],
        "kind": scenario["kind"],
        "rule_id": scenario["rule_id"],
        "golden": scenario["golden_candidate_id"],
        "planned_candidates": plan.candidate_count,
        "rule_missing": plan.rule_missing is not None,
        "predicted_root_cause": detail.root_cause,
        "confidence": detail.confidence,
        "missing_evidence_count": len(detail.missing_evidence),
    }


def main() -> None:
    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    rows = [run_scenario(scenario) for scenario in golden["scenarios"]]

    positives = [row for row in rows if row["kind"] == "positive"]
    healthy = [row for row in rows if row["kind"] == "healthy"]

    correct = [row for row in positives if row["predicted_root_cause"] == row["golden"]]
    covered = [row for row in rows if not row["rule_missing"] and row["planned_candidates"] > 0]
    false_positives = [
        row for row in healthy if row["predicted_root_cause"] not in NON_SPECIFIC_ROOT_CAUSES
    ]
    confusions = [
        row for row in positives if row["predicted_root_cause"] != row["golden"]
    ]
    confusion_pairs = Counter(
        (row["golden"], row["predicted_root_cause"]) for row in confusions
    )

    metrics = {
        "totals": {
            "rules": golden["rule_count"],
            "candidates": golden["candidate_count"],
            "positive_scenarios": len(positives),
            "healthy_scenarios": len(healthy),
        },
        "accuracy": {
            "correct": len(correct),
            "total": len(positives),
            "pct": round(100.0 * len(correct) / len(positives), 1) if positives else None,
        },
        "coverage": {
            "covered": len(covered),
            "total": len(rows),
            "pct": round(100.0 * len(covered) / len(rows), 1) if rows else None,
        },
        "false_positive": {
            "count": len(false_positives),
            "total": len(healthy),
            "pct": round(100.0 * len(false_positives) / len(healthy), 1) if healthy else None,
        },
        "confusion_pairs": [
            {"golden": g, "predicted": p, "count": c}
            for (g, p), c in confusion_pairs.most_common()
        ],
        "false_positive_cases": [
            {
                "scenario_id": row["scenario_id"],
                "predicted": row["predicted_root_cause"],
                "confidence": row["confidence"],
            }
            for row in false_positives
        ],
        "confusion_cases": [
            {
                "scenario_id": row["scenario_id"],
                "golden": row["golden"],
                "predicted": row["predicted_root_cause"],
                "confidence": row["confidence"],
            }
            for row in confusions
        ],
        "rows": rows,
    }

    (EVALS_DIR / "results.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (EVALS_DIR / "results.md").write_text(render_markdown(metrics), encoding="utf-8")

    acc = metrics["accuracy"]
    cov = metrics["coverage"]
    fpr = metrics["false_positive"]
    print(
        f"scenarios: positive={len(positives)} healthy={len(healthy)}\n"
        f"accuracy:       {acc['correct']}/{acc['total']} = {acc['pct']}%\n"
        f"coverage:       {cov['covered']}/{cov['total']} = {cov['pct']}%\n"
        f"false positive: {fpr['count']}/{fpr['total']} = {fpr['pct']}%\n"
        f"confusion pairs: {len(confusion_pairs)}"
    )
    for (g, p), c in confusion_pairs.most_common(10):
        print(f"  golden={g} -> predicted={p} (x{c})")


def render_markdown(metrics: dict) -> str:
    acc = metrics["accuracy"]
    cov = metrics["coverage"]
    fpr = metrics["false_positive"]
    totals = metrics["totals"]
    lines = [
        "# RCA 규칙 엔진 평가 결과",
        "",
        "합성 골든셋(카탈로그 YAML 역산)으로 plan_causes → evaluate_causes → "
        "analyze_root_cause 전체 경로를 실측한 결과. `make rca-eval` 로 재생성한다.",
        "",
        "## 이 수치가 증명하는 것과 증명하지 않는 것",
        "",
        "골든셋은 카탈로그 YAML 을 역산해 만든다. 즉 라벨의 출처가 채점 대상과 같은 "
        "파일이므로 **accuracy 100% 는 \"RCA 가 정확하다\"는 뜻이 아니다**. "
        "이 평가가 실제로 증명하는 것은 두 가지다.",
        "",
        "- **도달 가능성** — 모든 candidate 가 자기 신호를 받으면 자기 자신으로 판정된다. "
        "어떤 후보도 다른 후보의 부분 문자열 신호에 가려져 영원히 선택되지 않는 상태가 아니다. "
        "혼동 사례가 0 이라는 것이 이 뜻이다.",
        "- **오탐 없음** — 신호가 없는 정상 증거에서 특정 원인을 확정하지 않는다. "
        "규칙 밖에서는 unknown / insufficient_evidence 로 멈춘다.",
        "",
        "실제 장애 증거에 대한 정확도는 측정하지 않았다. 그러려면 라벨이 붙은 실제 사건 "
        "기록이 필요하고, 이 저장소에는 없다.",
        "",
        "## 골든셋",
        f"- rule: {totals['rules']}개 / candidate: {totals['candidates']}개",
        f"- positive 시나리오: {totals['positive_scenarios']}개 "
        f"(candidate 당 1개, 모든 signal 그룹의 any_of 첫 매처 충족)",
        f"- healthy 시나리오: {totals['healthy_scenarios']}개 (rule 당 1개, 신호 없음)",
        "",
        "## 지표",
        "| 지표 | 값 | 정의 |",
        "|---|---|---|",
        f"| accuracy | {acc['pct']}% ({acc['correct']}/{acc['total']}) "
        "| positive에서 root cause == 골든 라벨 |",
        f"| coverage | {cov['pct']}% ({cov['covered']}/{cov['total']}) "
        "| plan_causes 가 rule_missing 없이 후보 생성 |",
        f"| false positive | {fpr['pct']}% ({fpr['count']}/{fpr['total']}) "
        "| healthy에서 특정 원인 확정 (unknown/insufficient 는 정상) |",
        "",
        "## 혼동 사례 (골든 → 판정)",
    ]
    if metrics["confusion_pairs"]:
        lines.append("| 골든 | 판정 | 건수 |")
        lines.append("|---|---|---|")
        for pair in metrics["confusion_pairs"]:
            lines.append(f"| {pair['golden']} | {pair['predicted']} | {pair['count']} |")
    else:
        lines.append("- 없음")
    lines.append("")
    lines.append("## healthy 오탐 사례")
    if metrics["false_positive_cases"]:
        for case in metrics["false_positive_cases"]:
            lines.append(
                f"- {case['scenario_id']}: {case['predicted']} "
                f"(confidence={case['confidence']:.2f})"
            )
    else:
        lines.append("- 없음")
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    main()
