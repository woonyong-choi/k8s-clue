# k8s-clue Python 정리 계획

## 결정

- `k8s-clue`를 먼저 고친다.
- `k8s-clue`는 Python 행동 참조 구현으로 마무리한다.
- Java 구현은 Python Handoff Gate 통과 후 시작한다.
- Python에서 Hub, Fleet, 사용자 권한, 장기 운영 기능을 새로 완성하지 않는다.
- 현재 구현 중 Java에 필요 없는 runtime은 계약 추출 후 삭제한다.
- Python 코드를 Java로 줄 단위 번역하지 않는다.
- 저장소 이름과 GitHub URL은 `k8s-clue`로 유지한다.
- 제품 표시명과 목표 CLI는 Clue로 통일한다. 현재 package·chart와 저장·통신 식별자의 호환 경계는 [README](../README.md#명칭과-호환-범위)를 따른다.
- 아래 새 CLI·module·prefix는 앞으로의 구현 목표다. 기존 암호화·서명·identity·metric·환경변수 계약을 명칭 치환만으로 이전하지 않는다.

## Python 저장소의 최종 결과

```text
clue diagnose
→ kubeconfig에서 context 확인
→ Kubernetes API read-only 조회
→ Evidence Bundle 생성
→ deterministic Analyzer 실행
→ Rule ID와 Evidence 출력
→ 필요하면 Remediation Plan 생성
→ 허용된 변경만 Draft PR로 제안
→ 새 Evidence로 Recovery Check
```

필수 외부 구성요소:

```text
없음
```

필수 로컬 입력:

```text
kubeconfig
Kubernetes API 접근 권한
```

Python 최종 범위에서 제외:

```text
Hub
Fleet
사용자 가입
RBAC 관리 UI
장기 PostgreSQL 저장
Agent enrollment
Prometheus 필수 연동
NATS
Redis
웹 Console
외부 AI 필수 사용
```

## 현재 기준선

현재 구조:

- Python 3.13
- FastAPI
- SQLAlchemy
- PostgreSQL
- Alembic
- NATS
- Redis dependency
- 15개 service/worker
- React frontend
- Helm control-plane chart
- cluster-agent
- GitHub Draft PR
- ImagePullBackOff Golden Path

현재 검증 기준:

```text
backend 203 tests
frontend typecheck/lint/test/build
Helm lint/template
in-process/NATS equivalence
Alembic single head
ImagePullBackOff demo
```

P0에서 이 결과를 다시 기록한다. 이후 삭제 단계마다 기준선을 줄인다.

## 남길 기능

### Kubernetes Evidence

- kubeconfig context 선택
- namespace 범위 선택
- Pod 상태
- Deployment/ReplicaSet owner 관계
- Kubernetes Event
- container current state
- container last termination state
- restart count
- rollout condition
- Service와 EndpointSlice 관계
- PVC 상태
- HPA condition
- observed-at
- resourceVersion
- resource UID

### Analyzer

- versioned Rule ID
- deterministic result
- Evidence reference
- confidence class
- `insufficient_evidence`
- unsupported result
- false-positive regression fixture

### Incident

- cluster UID
- namespace
- workload UID
- Analyzer ID
- symptom identity
- occurrence
- opened/resolved lifecycle

### Remediation

- repository
- branch
- manifest path
- base SHA
- source digest
- 변경 전 값
- 제안 값
- 허용 field
- inverse patch
- dry-run result
- Draft PR only

### Recovery

- 변경 전 Evidence baseline
- Recovery Check 시작 시각
- 변경 후 새 Evidence window
- 동일 resource identity
- 안정 구간
- resolved/failed/insufficient 결과

## Java로 넘길 계약

파일 형식은 JSON Schema와 JSON fixture로 고정한다.

| 계약 | 필수 내용 |
|---|---|
| Evidence Bundle | schema version, resource identity, observed-at, normalized evidence |
| Finding | Analyzer ID/version, cause, confidence, Evidence reference |
| Incident | identity, occurrence, lifecycle |
| Remediation Plan | authority, target, before/after, risk, dry-run |
| Draft PR Result | repository, base SHA, PR identity, failure reason |
| Recovery Check | baseline, new window, stability, result |
| RBAC Contract | resource, verb, forbidden subresource |
| Reason Codes | stable code, meaning, retry class |

Java에 넘기지 않는 항목:

- Python class 이름
- Python module 이름
- FastAPI route 구조
- SQLAlchemy entity 구조
- Alembic revision history
- worker 이름
- NATS subject 이름
- 현재 DB table 이름
- 현재 frontend component 구조

## 목표 디렉터리

```text
src/clue/
├── cli/
│   ├── main.py
│   ├── commands/
│   │   ├── diagnose.py
│   │   ├── rules.py
│   │   ├── evidence.py
│   │   ├── remediate.py
│   │   └── recover.py
│   └── renderers/
│       ├── text.py
│       └── json.py
├── domain/
│   ├── evidence/
│   ├── analysis/
│   ├── incident/
│   ├── remediation/
│   └── recovery/
├── application/
│   ├── diagnose.py
│   ├── plan_remediation.py
│   └── check_recovery.py
├── adapters/
│   ├── kubernetes/
│   ├── git/
│   ├── github/
│   └── filesystem/
└── rules/
    ├── image_pull/
    ├── crash_loop/
    ├── scheduling/
    ├── memory/
    └── rollout/
tests/
├── unit/
├── contract/
├── golden/
├── e2e/
└── fixtures/
handoff/
├── schemas/
├── fixtures/
├── expected-results/
├── rules/
├── rbac/
└── compatibility-matrix.md
```

## 현재 경로 처리표

### 이동 후 유지

| 현재 경로 | 목표 경로 | 처리 |
|---|---|---|
| `src/services/target/cluster-agent/evidence/collector.py` | `src/clue/adapters/kubernetes/collector.py` | Hub lease 제거, local call 유지 |
| `src/services/target/cluster-agent/providers/` | `src/clue/adapters/kubernetes/` | read-only provider만 유지 |
| `src/services/ai/agent/pipeline/causes.py` | `src/clue/rules/` | Rule ID와 fixture 분리 |
| `src/packages/ai/rule_catalog.py` | `src/clue/domain/analysis/` | framework 의존 제거 |
| `src/domains/rca/` | `src/clue/domain/incident/`, `analysis/`, `recovery/` | ORM과 router 분리 |
| `src/domains/gitops/source_patch.py` | `src/clue/domain/remediation/` | allowlist 유지 |
| `src/services/gitops/scm-worker/github_provider.py` | `src/clue/adapters/github/` | worker wrapper 제거 |
| `src/domains/rca/recovery_verification.py` | `src/clue/domain/recovery/` | clock 주입 |

### 계약 추출 후 삭제

| 경로 | 추출할 내용 | 삭제 조건 |
|---|---|---|
| `frontend/` | Incident view model | expected JSON 생성 후 |
| `src/services/gateway/` | 인증·권한 요구사항 | Java security 문서 반영 후 |
| `src/services/projection/` | failure reason과 audit field | schema 반영 후 |
| `src/services/ai/*-worker/` | 단계별 입력·출력 | application service test 대체 후 |
| `src/services/gitops/safe-pr-worker/` | safe PR precondition | contract test 대체 후 |
| `src/services/gitops/scm-worker/app.py` | SCM request/result | adapter test 대체 후 |
| `src/packages/events/` | correlation/causation 최소 필드 | protocol schema 반영 후 |
| `src/packages/contracts/event_bus/` | event payload field | Handoff schema 반영 후 |
| `src/packages/runtime/` | idempotency/retry rule | direct application test 반영 후 |
| `src/packages/storage/` | persistent field | schema 목록 작성 후 |
| `src/domains/identity/` | 역할 요구사항 | Java 계획 반영 후 |
| `src/domains/timeline/` | Incident chronology field | Incident schema 반영 후 |
| `src/domains/alert/` | 외부 alert identity | Analyzer input 문서 반영 후 |
| `src/domains/target/` | cluster identity와 enrollment field | protocol 문서 반영 후 |
| `alembic/`, `alembic.ini` | 최종 schema field 목록 | JSON schema 확정 후 |
| `charts/clue/` | read-only RBAC | `handoff/rbac` 생성 후 |
| `src/entrypoints/app.py` | active service list | CLI entrypoint 전환 후 |
| `src/entrypoints/bootstrap*.py` | bootstrap requirement | Python DB 제거 후 |

### 삭제 대상 script

| 경로 | 삭제 시점 |
|---|---|
| `scripts/test-event-bus-equivalence.sh` | NATS 제거 단계 |
| `scripts/event_bus_equivalence.py` | NATS 제거 단계 |
| `scripts/services.py` | worker/service discovery 제거 단계 |
| full control-plane용 manifest 검사 | Helm runtime 제거 단계 |
| frontend gate | frontend 제거 단계 |

### 유지할 script

- lint
- unit test
- contract test
- golden fixture test
- kind diagnose E2E
- release artifact 검사
- 브랜드 검사
- Secret 비수집 검사

## dependency 처리표

| dependency | 처리 |
|---|---|
| `fastapi` | gateway 삭제 후 제거 |
| `uvicorn` | gateway/demo server 삭제 후 제거 |
| `sqlalchemy` | repository fixture 전환 후 제거 |
| `alembic` | migration 제거 후 제거 |
| `psycopg` | PostgreSQL runtime 제거 후 제거 |
| `nats-py` | direct application flow 전환 후 제거 |
| `redis` | active import 확인 후 제거 |
| `greenlet` | SQLAlchemy 제거 후 제거 |
| `opentelemetry-exporter-*` | CLI 필수 경로에서 제거 |
| `pyjwt` | 웹 인증 제거 후 필요성 재검사 |
| `cryptography` | GitHub/credential contract에 필요한지 재검사 |
| `httpx` | Kubernetes/GitHub adapter 사용 여부에 따라 유지 |
| `pyyaml` | rule과 manifest 처리에 유지 |

Python CLI parser는 표준 `argparse`를 우선 사용한다. CLI framework dependency는 completion과 subcommand 유지 비용을 확인한 뒤 추가한다.

## P0. 기준선 고정

### 작업

- [ ] clean worktree 확인
- [ ] 현재 commit SHA 기록
- [ ] `make gate` 실행
- [ ] `make event-bus-equivalence` 실행
- [ ] `make demo` 실행
- [ ] 전체 test 수와 실행 시간 기록
- [ ] frontend bundle 결과 기록
- [ ] Helm object 수 기록
- [ ] Alembic head 기록
- [ ] service inventory JSON 생성
- [ ] dependency inventory 생성
- [ ] active entrypoint 목록 생성
- [ ] 제거 후보별 보호 test 연결

### 산출물

```text
docs/baseline/current-runtime.md
handoff/baseline/test-results.json
handoff/baseline/services.json
handoff/baseline/dependencies.json
```

### 완료 조건

- [ ] 현재 결과를 한 명령으로 재현
- [ ] 실패한 baseline은 실패 상태와 원인 기록
- [ ] 삭제 전 비교 자료 확보

## P1. Clue 명칭과 CLI entrypoint

### 명칭 변경

- [x] 제품 표시 `Clue`
- [ ] CLI `clue`
- [x] Python project `k8s-clue`
- [x] chart directory `charts/clue`
- [x] 로컬 image 이름 `clue` (원격 게시 아님)
- [ ] 향후 environment prefix `CLUE_`: 현재 참조 구현의 `KYRO_`는 호환 유지
- [ ] 향후 metric prefix `clue_`: 현재 참조 구현의 `kyro_`는 호환 유지
- [ ] label/annotation prefix는 소유한 DNS 기준으로 확정
- [x] PostgreSQL 기본 database/user는 기존 설치와 호환되도록 `kyro` 유지
- [x] frontend package `clue-console`
- [ ] 기존 제품명 평문·파일명 검사

### CLI 생성

`pyproject.toml`:

```toml
[project.scripts]
clue = "clue.cli.main:main"
```

명령:

```bash
clue version
clue diagnose --help
clue rules list
```

### exit code

| code | 의미 |
|---|---|
| 0 | 진단 완료, critical finding 없음 |
| 1 | 진단 완료, warning/critical finding 있음 |
| 2 | CLI 입력 오류 |
| 3 | Kubernetes 인증·권한 오류 |
| 4 | Evidence 수집 실패 |
| 5 | 내부 오류 |

### 완료 조건

- [ ] 저장소명 `k8s-clue` 유지
- [ ] 제품 명칭 Clue 통일
- [ ] CLI help snapshot test
- [ ] text/json output 계약 생성

## P2. 설치 없는 진단

### 조회 순서

1. kubeconfig 로드
2. context 결정
3. API server 연결 확인
4. read permission 확인
5. namespace와 target 결정
6. allowlisted resource 조회
7. Evidence normalization
8. Analyzer 실행
9. Finding 정렬
10. text/json 출력

### target 선택

```bash
clue diagnose
clue diagnose --context dev
clue diagnose --namespace payments
clue diagnose deployment/card-api
clue diagnose pod/card-api-abc
clue diagnose --all-contexts
```

### 보안 조건

- [ ] Secret API 호출 없음
- [ ] create/update/patch/delete 없음
- [ ] exec/attach/port-forward/proxy 없음
- [ ] 출력에 kubeconfig token 없음
- [ ] 오류 출력에 Authorization header 없음

### 첫 Analyzer

```text
CLUE-IMAGE-001 IMAGE_TAG_NOT_FOUND
CLUE-IMAGE-002 IMAGE_PULL_UNAUTHORIZED
CLUE-IMAGE-003 REGISTRY_UNAVAILABLE
CLUE-IMAGE-099 INSUFFICIENT_IMAGE_EVIDENCE
```

### 완료 조건

- [ ] Agent 없이 동작
- [ ] DB 없이 동작
- [ ] NATS 없이 동작
- [ ] Prometheus 없이 동작
- [ ] AI 없이 동작
- [ ] kind ImagePullBackOff E2E

## P3. 순수 domain과 계약 추출

### domain 제약

- [ ] FastAPI import 금지
- [ ] SQLAlchemy import 금지
- [ ] NATS import 금지
- [ ] Kubernetes client import 금지
- [ ] 현재 시각 직접 호출 금지
- [ ] UUID 직접 생성 금지
- [ ] environment 직접 조회 금지

외부 값은 port로 주입한다.

```text
Clock
IdGenerator
EvidenceSource
IncidentRepository
ScmProvider
RuleCatalog
```

### canonical JSON

- key 정렬 고정
- UTC RFC3339 timestamp
- enum은 대문자 stable code
- 정수와 quantity normalization
- optional field 누락 규칙 고정
- unknown optional field 허용 정책 기록
- schema version 필수

### 동등성 검사

```text
기존 pipeline input
→ 기존 result

같은 input
→ 새 application service
→ 새 result

semantic diff
```

비교 필드:

- resource identity
- Analyzer ID/version
- cause
- confidence class
- Evidence reference set
- reason code
- remediation level
- recovery outcome

### 완료 조건

- [ ] domain-only test command 존재
- [ ] Golden Path fixture 동등성 통과
- [ ] schema validation 통과

## P4. runtime 제거

### 삭제 순서

```text
1. frontend
2. gateway와 웹 인증
3. projection
4. worker wrapper
5. service discovery
6. NATS/event bus
7. outbox/dead-letter runtime
8. Redis
9. PostgreSQL/SQLAlchemy
10. Alembic
11. Agent enrollment/runtime
12. control-plane Helm chart
13. bootstrap entrypoint
14. 미사용 dependency/test/script
```

### 각 삭제 commit 절차

1. 대체 fixture 추가
2. 대체 contract test 추가
3. 새 direct path로 entrypoint 전환
4. 기존 path import 차단 test 추가
5. 대상 코드 삭제
6. dependency 삭제
7. 문서 링크 삭제
8. 전체 gate 실행

### 삭제 금지

- 계약 추출 전 migration 삭제 금지
- expected result 생성 전 worker 삭제 금지
- RBAC fixture 생성 전 chart 삭제 금지
- view model 추출 전 frontend 삭제 금지
- recovery fixture 생성 전 persistence model 삭제 금지

### 완료 조건

- [ ] active worker process 0
- [ ] NATS import 0
- [ ] Redis import 0
- [ ] FastAPI import 0
- [ ] SQLAlchemy import 0
- [ ] Alembic file 0
- [ ] frontend build dependency 0
- [ ] Python control-plane Helm resource 0
- [ ] CLI test는 외부 service 없이 실행

## P5. P0 Analyzer와 수정 계약

### Analyzer

| Rule group | 필수 case |
|---|---|
| Image pull | tag 없음, 인증 실패, registry 장애 |
| Crash loop | non-zero exit, probe failure, config error |
| Scheduling | resource 부족, selector 불일치, taint |
| Memory | OOMKilled, memory limit 근접 |
| Rollout | progress deadline, unavailable replica |

### fixture 수

각 rule마다 최소:

- positive 2개
- negative 2개
- insufficient 1개
- malformed input 1개
- Kubernetes version variation 1개

### Remediation

- [ ] Explain
- [ ] Suggest
- [ ] Draft PR
- [ ] Apply 없음
- [ ] Secret 생성 없음
- [ ] RBAC 확대 없음
- [ ] NetworkPolicy 완화 없음
- [ ] base SHA 재확인
- [ ] target scalar 재확인
- [ ] inverse patch

### Recovery

- [ ] stale window 거부
- [ ] duplicate window 무시
- [ ] 다른 UID 거부
- [ ] baseline 없음 실패
- [ ] stability window 충족
- [ ] 재발 시 실패

## P6. Handoff Pack

### 디렉터리

```text
handoff/python-handoff-v1/
├── manifest.json
├── schemas/
├── fixtures/
├── expected-results/
├── rules/
├── rbac/
├── reason-codes.json
├── compatibility-matrix.md
├── provenance.md
└── checksums.txt
```

### manifest

```json
{
  "handoffVersion": "1",
  "pythonReferenceVersion": "...",
  "gitCommit": "...",
  "kubernetesVersions": ["..."],
  "schemaVersions": {},
  "ruleVersions": {},
  "generatedAt": "..."
}
```

### 생성 명령 목표

```bash
make handoff
make handoff-verify
```

### release

```text
tag: python-handoff-v1
artifact: clue-python-handoff-v1.tar.gz
checksum: SHA-256
```

## Handoff Gate

- [ ] `clue diagnose`가 외부 service 없이 실행된다.
- [ ] P0 Analyzer fixture가 모두 통과한다.
- [ ] schema가 versioned 상태다.
- [ ] expected result가 canonical JSON이다.
- [ ] Secret 비수집 검사가 통과한다.
- [ ] read-only RBAC가 fixture로 남아 있다.
- [ ] Draft PR safety fixture가 통과한다.
- [ ] Recovery Check fixture가 통과한다.
- [ ] Python 결과를 한 명령으로 생성한다.
- [ ] 삭제 대상 runtime import가 0이다.
- [ ] 문서, Rule ID, command 이름이 일치한다.
- [ ] license와 NOTICE가 정리됐다.
- [ ] handoff artifact checksum이 검증됐다.
- [ ] `python-handoff-v1` tag가 있다.

## test 재분류

### contract/golden으로 유지

- `test_golden_path_safety_contracts.py`
- `test_incident_signal_identity.py`
- `test_recovery_gitops_authority.py`
- `test_recovery_kustomize_edit_source.py`
- `test_recovery_merge_scope.py`
- `test_recovery_pr_lifecycle.py`
- `test_recovery_safe_pr_copy.py`
- `test_recovery_selection_preflight.py`
- `test_recovery_verification.py`
- `test_safe_pr_structured_base_advance.py`

파일명은 새 domain 기준으로 변경한다. DB·worker fixture는 JSON fixture로 바꾼다.

### 요구사항만 추출하고 삭제

- API gateway route test
- session/admin test
- GitHub App uninstall UI flow test
- management guard runtime test
- Alertmanager webhook server test
- timeline projection query test
- DB repository filter test
- event bus equivalence test
- migration head test
- frontend component test

삭제 전에 필요한 reason code, identity와 payload field가 contract fixture에 포함됐는지 확인한다.

## 최종 gate

```bash
make lint
make test
make contract-test
make golden-test
make diagnose-e2e
make security-contract-test
make handoff-verify
make brand-check
```

최종 gate에서 제외:

```text
frontend build
NATS equivalence
Alembic head
full control-plane Helm lint
PostgreSQL integration
gateway API E2E
```

## commit 순서

```text
1. baseline
2. naming
3. CLI shell
4. ImagePullBackOff direct diagnose
5. schema/fixture
6. pure domain
7. frontend 제거
8. gateway 제거
9. worker/event bus 제거
10. DB/migration 제거
11. Agent/control-plane 제거
12. P0 Analyzer
13. remediation/recovery contract
14. Handoff Pack
15. python-handoff-v1
```

한 commit에서 하지 않는 조합:

- 이동과 로직 변경
- 대규모 삭제와 포맷 변경
- schema 변경과 expected result 무근거 변경
- dependency 삭제와 unrelated refactor
- naming 변경과 behavior 변경

## 완료

완료 상태:

```text
k8s-clue = 실행 가능한 Python Clue 명세
Clue = Java 포팅 대기
```

Java 작업 시작 조건:

```text
P6 완료
Handoff Gate 전체 통과
python-handoff-v1 발행
```
