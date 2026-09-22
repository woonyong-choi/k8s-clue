# RCA 규칙 엔진 평가 결과

합성 골든셋(카탈로그 YAML 역산)으로 plan_causes → evaluate_causes → analyze_root_cause 전체 경로를 실측한 결과. `make rca-eval` 로 재생성한다.

## 이 수치가 증명하는 것과 증명하지 않는 것

골든셋은 카탈로그 YAML 을 역산해 만든다. 즉 라벨의 출처가 채점 대상과 같은 파일이므로 **accuracy 100% 는 "RCA 가 정확하다"는 뜻이 아니다**. 이 평가가 실제로 증명하는 것은 두 가지다.

- **도달 가능성** — 모든 candidate 가 자기 신호를 받으면 자기 자신으로 판정된다. 어떤 후보도 다른 후보의 부분 문자열 신호에 가려져 영원히 선택되지 않는 상태가 아니다. 혼동 사례가 0 이라는 것이 이 뜻이다.
- **오탐 없음** — 신호가 없는 정상 증거에서 특정 원인을 확정하지 않는다. 규칙 밖에서는 unknown / insufficient_evidence 로 멈춘다.

실제 장애 증거에 대한 정확도는 측정하지 않았다. 그러려면 라벨이 붙은 실제 사건 기록이 필요하고, 이 저장소에는 없다.

## 골든셋
- rule: 29개 / candidate: 87개
- positive 시나리오: 87개 (candidate 당 1개, 모든 signal 그룹의 any_of 첫 매처 충족)
- healthy 시나리오: 29개 (rule 당 1개, 신호 없음)

## 지표
| 지표 | 값 | 정의 |
|---|---|---|
| accuracy | 100.0% (87/87) | positive에서 root cause == 골든 라벨 |
| coverage | 100.0% (116/116) | plan_causes 가 rule_missing 없이 후보 생성 |
| false positive | 0.0% (0/29) | healthy에서 특정 원인 확정 (unknown/insufficient 는 정상) |

## 혼동 사례 (골든 → 판정)
- 없음

## healthy 오탐 사례
- 없음
