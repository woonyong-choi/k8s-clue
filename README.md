# K8s Clue — Kubernetes 장애 진단과 Draft PR 제안

Kubernetes 장애 증거를 읽기 전용으로 수집하고, 버전이 붙은 규칙으로 원인을 판정한 뒤, 허용된 GitOps 변경만 GitHub Draft PR로 제안하는 Python 팀 프로젝트입니다.

- 5인 팀의 팀장으로 전체 아키텍처, 이벤트 런타임, 서비스 간 계약, CI와 배포를 맡았습니다. 룰 카탈로그, 에이전트 수집, PR 생성, 게이트웨이 인증은 팀원이 작성했습니다.
- 클러스터를 직접 변경하거나 PR을 자동 merge하지 않습니다. 증거와 허용 필드를 확인하고, 사람이 검토할 Draft PR까지만 생성합니다.
- 공개 정리본은 ImagePullBackOff 한 경로를 kind와 계약 테스트 86개로 확인했습니다. 전체 pytest는 294개이며, 외부 클러스터·GitHub App 연동 E2E는 아직 검증하지 않았습니다.

RCA 룰은 합성 입력 116개(장애 87 + 정상 29)에서 가려진 후보 0건·오탐 0건을 기록했습니다. 이 수치는 2026-09-23 로컬 재실행 결과이며 실사용 트래픽 지표가 아닙니다.

[![CI](https://github.com/woonyong-choi/k8s-clue/actions/workflows/ci.yml/badge.svg)](https://github.com/woonyong-choi/k8s-clue/actions/workflows/ci.yml)
![Python 3.13](https://img.shields.io/badge/python-3.13-blue)

크래프톤 정글 팀 과제(원본 [minmings111/Kyro-jungle-final](https://github.com/minmings111/Kyro-jungle-final))에서 시작해, 과제 종료 후 개인 포크에서 Golden Path와 안전 계약을 정리했습니다. 커밋 단위 경계는 [팀 과제와 개인 확장의 경계](docs/personal-extension.md)에 있습니다.

## 데모

`make demo`는 kind 클러스터에서 ImagePullBackOff를 실제로 재현한 뒤, 증거·RCA → base SHA 고정 Draft PR → 배포 후 검증 계약을 순서대로 검사합니다. 실제 GitHub에 PR을 발행하는 E2E는 아닙니다.

![make demo 실행 기록 — kind에서 ErrImagePull 관측 후 계약 테스트 24 + 29 + 27 + 6건 통과](https://raw.githubusercontent.com/woonyong-choi/k8s-clue/main/docs/demo.gif)

위는 `DEMO_SKIP_PR=1 DEMO_KIND_CONTEXT=kind-clue-demo bash scripts/demo_terminal.sh`의 실제 실행 기록입니다. kind에 없는 image tag를 쓰는 Deployment를 띄워 `ErrImagePull`을 관측하고, 네 장면의 계약 테스트 86건을 통과한 뒤, Draft PR 수명주기 계약 테스트 이름 일부를 보여 줍니다.

## 빠르게 실행하기

Python 3.13과 [uv](https://docs.astral.sh/uv/getting-started/installation/)가 필요합니다. 카탈로그 계층 테스트에는 Docker(PostgreSQL 하나)가 추가로 필요합니다.

```bash
git clone https://github.com/woonyong-choi/k8s-clue.git && cd k8s-clue
uv sync --all-groups
make demo          # Golden Path 계약 86개
make catalog-up && make catalog-schema   # 카탈로그 검사용 PostgreSQL
make test          # ruff lint + pytest 294개
```

`make demo`는 `DEMO_KIND_CONTEXT`가 없으면 kind 장면을 건너뛰고 계약 테스트만 실행합니다. 전체 타깃은 `make help`, 로컬 도구 확인은 `make doctor`로 봅니다.

## 구조

```
src/services/target/cluster-agent/   읽기 전용 증거 수집 (get·list·watch만)
src/services/ai/                     RCA·계획·복구 워커 10개
src/services/gitops/                 safe-pr-worker(패치 구성) · scm-worker(Draft PR)
src/services/gateway/                api-gateway(증거 수신) · outbox-relay(발행)
src/domains/                         gitops·rca·inventory·datacatalog 도메인 규칙
src/packages/                        event bus · outbox · 처리 원장 · 보안
charts/clue/                         Helm chart (agent RBAC은 read-only만)
evals/                               RCA 룰 골든셋과 실측 결과
tests/                               계약·속성·카탈로그 테스트 294개
```

`uv run python scripts/services.py`가 16개 runtime 서비스를 출력합니다.

```mermaid
flowchart LR
  subgraph T["대상 클러스터"]
    K["Kubernetes API"]
    A["cluster-agent<br/>get·list·watch만<br/>명령 채널 없음"]
  end
  subgraph M["관리 클러스터"]
    GW["api-gateway<br/>증거 수신·에이전트 인증"]
    DB[("PostgreSQL<br/>증거·outbox·처리 원장")]
    RCA["rca-worker<br/>versioned rule 판정"]
    SPR["safe-pr-worker<br/>allowlist scalar 패치 구성"]
    SCM["scm-worker<br/>base SHA 재확인 → Draft PR"]
    VER["rca-feedback-worker<br/>기준선 대비 회복 검증"]
  end
  GH["GitHub App<br/>GitOps 저장소"]
  P["사람<br/>리뷰·merge"]

  K -->|"Pod·Event 읽기"| A
  A -->|"증거 bundle<br/>(에이전트가 연결 개시)"| GW
  GW --> DB
  DB --> RCA
  RCA -->|"원인 후보 + 근거"| SPR
  SPR -->|"scalar 1개 patch<br/>+ 기대 base SHA"| SCM
  SCM -->|"draft: true 강제<br/>merge API 미호출"| GH
  GH --> P
  P -->|"merge 결정"| GH
  GH -->|"서명된 merge webhook"| VER
  A -.->|"다음 주기 evidence window"| VER
  VER -->|"resolved / verification failed"| DB
```

설계 문서: [설계 근거와 알려진 한계](docs/design.md) · [Golden Path 안전 계약 9개 조항](docs/GOLDEN-PATH.md) · [Project Map](docs/PROJECT-MAP.md) · [문서 목차](docs/README.md)

## 안전 규칙

- **YAML 패치** — 원문을 다시 덤프하지 않고 허용한 `ScalarNode`의 byte span만 바꿉니다. 속성 테스트가 한 줄만 바뀌는지, 주석 수가 유지되는지, rollback이 원문을 복원하는지 확인합니다. → [`domains/gitops/source_patch.py`](src/domains/gitops/source_patch.py)
- **원인 판정** — 버전이 붙은 YAML 룰 카탈로그(29 rule / 87 candidate)로 판단합니다. 규칙 밖의 증거는 reason code와 원문 참조를 남기고 멈춥니다. → [`services/ai/agent/causes/engine.py`](src/services/ai/agent/causes/engine.py), [`evals/results.md`](evals/results.md)
- **Draft PR** — PR 생성 직전에 base SHA를 다시 읽고, 처음 확인한 SHA와 다르면 중단합니다. `draft: true`가 아닌 응답을 거부하고 merge API는 호출하지 않습니다. → [`github_provider.py`](src/services/gitops/scm-worker/github_provider.py)
- **인벤토리 삭제** — 수집 범위가 완전하고 잘리지 않았으며 selector와 reason code가 없을 때만 `(resource_type, namespace)` 삭제 범위를 발급합니다. Event는 삭제 범위를 얻지 못합니다. → [`coverage.py`](src/domains/inventory/coverage.py)
- **권한 검사** — Helm이 렌더링한 manifest에서 read-only 밖의 verb, exec·attach·proxy, wildcard가 보이면 CI가 실패합니다. → [`manifest-check.sh`](scripts/manifest-check.sh)

## 검증

| 스위트 | 무엇을 증명하나 | 개수 | 실행 명령 |
|---|---|---:|---|
| 전체 pytest | lint + 전 계층 회귀 (PostgreSQL 기동 시) | **294 passed** | `make catalog-up && make test` |
| 전체 pytest (DB 없이) | 카탈로그 36개는 skip | 258 passed, 36 skipped | `make test` |
| demo — 증거·RCA | ImagePullBackOff 증거에서 결정론적 원인이 나오는가 | 24 | `make demo` |
| demo — Draft PR | base SHA 고정, draft 강제, merge 경로 부재 | 29 | `make demo` |
| demo — 배포 후 검증 | 기준선 대비 실제 회복 판정, stale window 거부 | 27 | `make demo` |
| demo — Golden Path 안전 계약 | 9개 조항(read-only·correlation·멱등성·allowlist 등) | 6 | `make demo` |
| 속성 테스트 | byte span 치환과 삭제 범위의 불변식 | 6 | `uv run pytest tests/test_source_patch_splice_properties.py tests/test_inventory_delete_scope_properties.py` |
| RCA 룰 실측 | 가려진 후보 / 정상 증거 오탐 | **0 / 0** (116 시나리오) | `make rca-eval` |
| 운영 데이터 카탈로그 | 자산·스키마 계약·리니지·품질 SQL·조회 API | **79 passed** | `make catalog-test` |
| Helm·manifest 검사 | 렌더된 object 11개에 mutation verb·wildcard 없음 | 통과 | `make manifest-check` |
| event bus 모드 동등성 | in-process와 NATS 결과가 같은가 | `equivalent: true` | `make event-bus-equivalence` |
| requirements 재현성 | `uv.lock` ↔ `requirements.txt` | 일치 | `make test` |

CI는 4개 job입니다 — `backend`(`make gate-backend`), `frontend`(`npm run check`), `actionlint`, `demo`. `demo` job은 kind 클러스터를 띄우고 `DEMO_KIND_CONTEXT`를 넘겨, 없는 image tag를 쓰는 Deployment가 실제로 `ImagePullBackOff`에 들어가는지 관측한 뒤 같은 계약 테스트를 실행합니다(`DEMO_SKIP_PR=1`이므로 PR은 발행하지 않습니다).

## 범위와 한계

- 완결 검증한 시나리오는 **ImagePullBackOff 하나**입니다. 룰 카탈로그에는 다른 시나리오도 있지만, 지표·로그 임계치가 필요한 경로는 패치 allowlist와 회복 검증 쪽이 막혀 있습니다.
- **실사용 트래픽·복구 시간·처리량·비용은 측정하지 않았습니다.** 외부 클러스터와 GitHub App의 종단 검증도 미완입니다. 현재 보장 범위는 계약 테스트와 kind 재현까지입니다.
- RCA 골든셋은 카탈로그 YAML을 역산한 **합성 데이터**입니다. accuracy 100%는 실제 장애 정확도가 아니라 "어떤 후보도 가려져 있지 않다"는 뜻입니다.
- **범위 단위 삭제 기능은 사실상 꺼져 있습니다.** 에이전트가 내보내는 `collection_scopes`를 `collection_coverage`로 투영하는 단계가 연결돼 있지 않아 런타임에서 `inventory_deletion_scopes()`는 항상 빈 튜플을 돌려줍니다. 안전한 쪽(아무것도 지우지 않음)으로 닫히지만 기능은 아직 없습니다 — [design.md의 알려진 한계](docs/design.md#알려진-한계-지금-열려-있는-구멍).
- `clue diagnose`는 **계획된 CLI**이고 현재 실행 진입점은 Make 타깃입니다. 저장·통신 호환 때문에 남아 있는 `kyro`/`KYRO` 식별자의 마이그레이션 경로도 아직 정하지 않았습니다.

## 관련 링크

- [설계 근거](docs/design.md) — 왜 이 자료구조인가, 버린 대안, 알려진 한계
- [Golden Path 안전 계약](docs/GOLDEN-PATH.md) — 9개 조항의 코드 강제 지점과 회귀 테스트
- [팀 과제와 개인 확장의 경계](docs/personal-extension.md) — 커밋 범위, 정리 전후, 기술 스펙
- 운영 데이터 카탈로그 — [수집 완전성 계약](docs/collection-contract.md) · [메타데이터 카탈로그](docs/metadata-catalog.md) · [품질 검사 SQL](docs/sql-quality-checks.md) · [조회 API](docs/catalog-api.md)
- [Python 선행 정리 계획](docs/PYTHON-FIRST-PLAN.md) — Java 포팅 인수 조건
- 원본 팀 저장소 — [minmings111/Kyro-jungle-final](https://github.com/minmings111/Kyro-jungle-final) · [팀 발표 영상](https://www.youtube.com/watch?v=Ar4rNJZX7lU)
- [포트폴리오](https://docs.woonyong.com/projects/) · [CI 실행 기록](https://github.com/woonyong-choi/k8s-clue/actions)
- [`NOTICE`](NOTICE) — upstream [skyhook-io/radar](https://github.com/skyhook-io/radar) 출처와 재작성 범위 (Apache-2.0)
- [Kubernetes — Pod lifecycle·Events](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/) · [GitHub REST API — Pulls](https://docs.github.com/en/rest/pulls/pulls)
