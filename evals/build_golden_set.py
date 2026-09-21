"""카탈로그 YAML 로부터 골든셋을 기계적으로 생성한다.

- 후보(candidate)마다 positive 시나리오 1개: 모든 signal 그룹의 any_of[0] 매처를
  충족하는 evidence bundle + rule.symptoms[0] incident. 골든 라벨 = candidate_id.
- rule 마다 healthy 시나리오 1개: symptom 은 갖되 판별 신호가 전혀 없는 정상 bundle.

fact → evidence JSON 역산은 services/ai/agent/causes/signals.py 의 collect_* 함수 기준:
- terminated_reason=R  → kubernetes.pods[].terminated_reasons
- waiting_reason=R     → kubernetes.pods[].waiting_reasons
- exit_code=N          → kubernetes.pods[].containers[].exit_code (non_oom 은 1)
- event_reason=R       → kubernetes.events[].reason
- pod_label:k=v        → kubernetes.pods[].labels
- alert_name=A         → metrics.alertmanager.alerts[](status=firing).labels.alertname
- log_pattern:P        → logs.entries[].line 에 P 포함
- event_pattern:P      → kubernetes.events[].message 에 P 포함

실행: cd /tmp/k8s-clue && PYTHONPATH=src python3 evals/build_golden_set.py
출력: evals/golden_set.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from services.ai.agent.causes.loader import load_catalog_profiles  # noqa: E402

OUT_PATH = Path(__file__).resolve().parent / "golden_set.json"

# 판별 신호와 절대 겹치지 않아야 하는 정상(benign) 텍스트 — 생성 시 검증한다.
BENIGN_LOG_LINE = "app heartbeat: request served in 12ms, status=200"
BENIGN_SOURCE_VALUES = {
    "kubernetes": lambda: {
        "pods": [
            {
                "name": "demo-app-7f9c",
                "namespace": "demo",
                "waiting_reasons": [],
                "terminated_reasons": [],
                "containers": [{"name": "app", "exit_code": 0}],
            }
        ],
        "events": [],
    },
    "metrics": lambda: {"results": {"cpu_usage": {"status": "success"}}},
    "logs": lambda: {"entries": [{"line": BENIGN_LOG_LINE}]},
    "traces": lambda: {"results": {"http_server": {"status": "success"}}},
    "metadata": lambda: {"items": [{"status": "ok"}]},
}


def benign_value(source: str) -> dict:
    factory = BENIGN_SOURCE_VALUES.get(source, lambda: {"items": [{"status": "ok"}]})
    return factory()


def split_evidence_key(key: str) -> tuple[str, str]:
    if ":" in key:
        source, name = key.split(":", 1)
        return source, name
    return key, key


def ensure_item(items: dict[tuple[str, str], dict], source: str, name: str) -> dict:
    entry = items.get((source, name))
    if entry is None:
        entry = benign_value(source)
        items[(source, name)] = entry
    return entry


def first_item_of_source(
    items: dict[tuple[str, str], dict], source: str, default_name: str
) -> dict:
    for (item_source, _name), value in items.items():
        if item_source == source:
            return value
    return ensure_item(items, source, default_name)


def inject_fact(items: dict[tuple[str, str], dict], fact: str) -> None:
    k8s = first_item_of_source(items, "kubernetes", "cluster_resource_state")
    pod = k8s["pods"][0]
    if fact.startswith("terminated_reason="):
        pod.setdefault("terminated_reasons", []).append(fact.split("=", 1)[1])
    elif fact.startswith("waiting_reason="):
        pod.setdefault("waiting_reasons", []).append(fact.split("=", 1)[1])
    elif fact.startswith("event_reason="):
        k8s.setdefault("events", []).append(
            {"reason": fact.split("=", 1)[1], "message": ""}
        )
    elif fact == "exit_code=non_oom":
        pod.setdefault("containers", [{}])
        pod["containers"].append({"name": "synthetic", "exit_code": 1})
    elif fact.startswith("exit_code="):
        pod.setdefault("containers", [{}])
        pod["containers"].append({"name": "synthetic", "exit_code": int(fact.split("=", 1)[1])})
    elif fact.startswith("pod_label:"):
        key, value = fact[len("pod_label:"):].split("=", 1)
        pod.setdefault("labels", {})[key] = value
    elif fact.startswith("alert_name="):
        metrics = first_item_of_source(items, "metrics", "telemetry_metrics")
        alertmanager = metrics.setdefault("alertmanager", {"alerts": []})
        alertmanager.setdefault("alerts", []).append(
            {"status": "firing", "labels": {"alertname": fact.split("=", 1)[1]}}
        )
    else:
        raise ValueError(f"지원하지 않는 fact matcher: {fact}")


def inject_matcher(items: dict[tuple[str, str], dict], matcher: dict) -> None:
    fact = matcher.get("fact")
    if isinstance(fact, str):
        inject_fact(items, fact)
        return
    log_pattern = matcher.get("log_pattern")
    if isinstance(log_pattern, str):
        logs = first_item_of_source(items, "logs", "related_logs")
        logs.setdefault("entries", []).append({"line": f"synthetic evidence: {log_pattern}"})
        return
    event_pattern = matcher.get("event_pattern")
    if isinstance(event_pattern, str):
        k8s = first_item_of_source(items, "kubernetes", "cluster_resource_state")
        k8s.setdefault("events", []).append({"reason": "", "message": event_pattern})
        return
    raise ValueError(f"지원하지 않는 matcher: {matcher}")


def incident_payload(rule_id: str, symptom: str, scenario_id: str) -> dict:
    return {
        "incident_id": f"inc-{scenario_id}",
        "cluster_id": "cluster-eval",
        "resource_kind": "Deployment",
        "resource_name": "demo-app",
        "namespace": "demo",
        "symptom": symptom,
        "severity": "high",
        "first_seen_at": "2026-08-01T00:00:00Z",
        "summary": f"synthetic eval incident for {rule_id}",
    }


def serialize_items(items: dict[tuple[str, str], dict]) -> list[dict]:
    return [
        {
            "source": source,
            "name": name,
            "value": value,
            "summary": f"synthetic {source}:{name} evidence",
            "evidence_ref": f"eval://{source}/{name}",
            "check_id": f"evidence:{source}:{name}",
        }
        for (source, name), value in items.items()
    ]


def build() -> dict:
    profiles = load_catalog_profiles()
    scenarios: list[dict] = []
    all_log_patterns: set[str] = set()
    all_event_patterns: set[str] = set()

    for profile in profiles:
        for candidate in profile.candidate_specs:
            for group in candidate.signals:
                for matcher in group.get("any_of") or []:
                    if isinstance(matcher.get("log_pattern"), str):
                        all_log_patterns.add(matcher["log_pattern"].casefold())
                    if isinstance(matcher.get("event_pattern"), str):
                        all_event_patterns.add(matcher["event_pattern"].casefold())

    # benign 텍스트가 어떤 판별 패턴과도 겹치지 않는지 생성 시점에 검증
    benign = BENIGN_LOG_LINE.casefold()
    clashes = [p for p in all_log_patterns if p in benign]
    if clashes:
        raise SystemExit(f"benign 로그 라인이 판별 패턴과 충돌: {clashes}")

    for profile in profiles:
        symptom = profile.symptoms[0]
        # positive: candidate 별 1개
        for candidate in profile.candidate_specs:
            scenario_id = f"pos-{profile.rule_id}-{candidate.candidate_id}"
            items: dict[tuple[str, str], dict] = {}
            for key in candidate.expected_evidence:
                source, name = split_evidence_key(key)
                ensure_item(items, source, name)
            for group in candidate.signals:
                matchers = group.get("any_of") or []
                if not matchers:
                    continue
                inject_matcher(items, matchers[0])
            scenarios.append(
                {
                    "scenario_id": scenario_id,
                    "kind": "positive",
                    "rule_id": profile.rule_id,
                    "golden_candidate_id": candidate.candidate_id,
                    "incident": incident_payload(profile.rule_id, symptom, scenario_id),
                    "evidence_items": serialize_items(items),
                }
            )
        # healthy: rule 별 1개 — 해당 rule 후보들의 expected evidence 는 모두 있으나 신호 없음
        scenario_id = f"healthy-{profile.rule_id}"
        items = {}
        for candidate in profile.candidate_specs:
            for key in candidate.expected_evidence:
                source, name = split_evidence_key(key)
                ensure_item(items, source, name)
        scenarios.append(
            {
                "scenario_id": scenario_id,
                "kind": "healthy",
                "rule_id": profile.rule_id,
                "golden_candidate_id": None,
                "incident": incident_payload(profile.rule_id, symptom, scenario_id),
                "evidence_items": serialize_items(items),
            }
        )

    return {
        "rule_count": len(profiles),
        "candidate_count": sum(len(p.candidate_specs) for p in profiles),
        "positive_count": sum(1 for s in scenarios if s["kind"] == "positive"),
        "healthy_count": sum(1 for s in scenarios if s["kind"] == "healthy"),
        "scenarios": scenarios,
    }


def main() -> None:
    golden = build()
    OUT_PATH.write_text(json.dumps(golden, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"golden set written: {OUT_PATH}\n"
        f"rules={golden['rule_count']} candidates={golden['candidate_count']} "
        f"positive={golden['positive_count']} healthy={golden['healthy_count']}"
    )


if __name__ == "__main__":
    main()
