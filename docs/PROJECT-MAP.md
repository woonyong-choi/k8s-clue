# Project Map

## Runtime

기본 제어면은 11개 서비스를 단일 Controller 프로세스로 조합한다. 대상 에이전트를 포함하면 12개 서비스다. 이 수는 앱 항목 수이며 Pod 수가 아니다.

| 구간 | 서비스 |
|---|---|
| 진입·전달 | `api-gateway`, `outbox-relay`, `dead-letter-monitor` |
| 증거·사건 | `evidence-worker`, `incident-worker` |
| RCA | `rca-worker`, `rca-feedback-worker` |
| 안전한 변경 | `safe-pr-worker`, `scm-worker` |
| 복구 계획·선택·전달, 사후 검증 | `recovery-worker` |
| 보조 | `ai-diff-worker` |

대상 클러스터에는 `cluster-agent` 하나만 배포한다. agent는 Kubernetes snapshot 수집 capability만 등록하며 command, terminal, port-forward, node collector, traffic provider를 포함하지 않는다.

## HTTP와 UI

`api-gateway`는 identity/session, GitHub App·repository discovery, webhook, RCA query/bundle/history, agent evidence lease/result, dead letter, health/metrics, 정적 frontend proxy만 제공한다.

사건 이력은 `GET /incidents/{correlation_id}/history`가 조회한다. 인증된 agent evidence 수신이 snapshot과 이력 ledger를 한 트랜잭션으로 기록하며, 이력은 UID가 있는 객체와 UID 없이 locator(종류·namespace·이름)로만 식별한 객체를 구분한다. 삭제는 증명된 namespace의 Pod 수집에서 빠진 경우에만 인정하고, 증명 없는 부재는 기록하지 않는다. 수집이 범위 한정이라 이력 전체는 항상 `partial`이다. 호출자에게 `inventory.read`·`evidence.read` 권한이 없는 출처는 결과에서 제외한다. 관측하지 못한 삭제 시각은 정확한 값이 아니라 마지막 관측과 다음 수집 사이의 상한으로만 표시한다.

Frontend route는 세 개다.

- `/`: 제품 소개와 Golden Path 안내
- `/incidents`: 사건 목록
- `/incidents/:correlationId`: 진단 근거와 선택된 수정안. PR, 배포와 회복의 실제 진행 상태는 아직 표시하지 않음

API gateway는 Redis를 세션 권위로 사용한다. Helm은 같은 Controller Pod에 loopback 전용 Redis를 배치하며 replica 하나만 허용한다. Redis 재시작 시 로그인 세션은 소멸하지만 업무 증거는 PostgreSQL에 남는다.

WebSocket gateway와 대형 dashboard route는 없다.

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

`domains.command`의 models/events/repository/lifecycle는 기존 migration과 과거 workflow 참조를 읽기 위해 남아 있다. router, handler, action catalog, worker, agent executor는 제거됐고 저장소의 cancel/retry 해석도 항상 거부하도록 고정했다.

`domains.dashboard`에는 RCA와 change correlation이 함께 읽는 `RcaTimeline` 영속 모델만 남아 있다. dashboard repository, ready stream, HTTP route, projection worker, frontend는 제거됐다.
