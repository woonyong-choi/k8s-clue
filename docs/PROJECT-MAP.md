# Project Map

## Runtime

기본 제어면은 15개 서비스입니다.

| 구간 | 서비스 |
|---|---|
| 진입·전달 | `api-gateway`, `dispatch-worker`, `outbox-relay`, `dead-letter-monitor` |
| 증거·사건 | `evidence-worker`, `incident-worker` |
| RCA | `plan-worker`, `analyze-worker`, `rca-worker`, `rca-feedback-worker` |
| 안전한 변경 | `select-worker`, `safe-pr-worker`, `scm-worker` |
| 사후 검증 | `recovery-worker` |
| 보조 | `ai-diff-worker` |

대상 클러스터에는 `cluster-agent` 하나만 배포합니다. agent는 Kubernetes snapshot 수집 capability만 등록하며 command, terminal, port-forward, node collector, traffic provider를 포함하지 않습니다.

## HTTP와 UI

`api-gateway`는 identity/session, GitHub App·repository discovery, webhook, RCA query/bundle, agent evidence lease/result, dead letter, health/metrics, 정적 frontend proxy만 제공합니다.

Frontend route는 세 개입니다.

- `/`: 사건 목록으로 이동
- `/incidents`: 사건 목록
- `/incidents/:correlationId`: evidence, RCA, Draft PR, verification 상세

WebSocket gateway와 대형 dashboard route는 없습니다.

## 디렉터리 책임

| 경로 | 책임 |
|---|---|
| `src/services/target/cluster-agent` | read-only Kubernetes evidence 수집 |
| `src/services/ai/agent` | incident, deterministic RCA, bounded recovery 계획 |
| `src/services/ai/*-worker` | Golden Path 이벤트 단계 |
| `src/services/gitops/scm-worker` | pinned-base GitHub Draft PR 생성 |
| `src/domains/rca`, `src/domains/scm`, `src/domains/gitops` | evidence/RCA/PR/verification 영속 계약 |
| `frontend/src` | 3-route incident UI |
| `charts/clue` | 최소 runtime 설치와 read-only RBAC |
| `deploy/kind` | 선택적인 로컬 Kubernetes fixture |
| `tests` | 결정론적 RCA, GitOps authority, PR lifecycle, 재검증 계약 |

## 의도적으로 격리한 스키마

`domains.command`의 models/events/repository/lifecycle는 기존 migration과 과거 workflow 참조를 읽기 위해 남아 있습니다. router, handler, action catalog, worker, agent executor는 제거됐고 저장소의 cancel/retry 해석도 항상 거부하도록 고정했습니다.

`domains.dashboard`에는 RCA와 change correlation이 함께 읽는 `RcaTimeline` 영속 모델만 남아 있습니다. dashboard repository, ready stream, HTTP route, projection worker, frontend는 제거됐습니다.
