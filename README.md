# Clue Python Reference

Kubernetes 장애 증거 기반 GitOps 변경 제안 도구

## 한눈에

| 구분 | 내용 |
|---|---|
| 무엇 | Kubernetes 장애 증거를 보존하고, 규칙으로 원인을 판정한 뒤, 허용된 변경만 GitHub Draft PR 로 제안하는 GitOps 도구 |
| 왜 | 자동화가 클러스터를 직접 바꾸지 못하게 막으면서 관측, 판정, 변경, 사후 검증을 하나의 사건 ID 로 잇기 위해 |
| 내 몫 | 5인 팀의 팀장. 전체 아키텍처, 장애 파이프라인, 서비스 간 인터페이스 설계. 종료 후 Golden Path 축소와 안전 계약 감사 |
| 스택 | Python 3.13 · NATS · PostgreSQL(Alembic) · Kubernetes/Helm · TypeScript 프론트엔드 |
| 검증된 사실 | backend 203 passed, frontend typecheck/lint/vitest/build, Helm lint, 이벤트 의미 동등성. `make demo` 로 재현 (아래 "검증 결과") |
| 한계 | 실사용 트래픽 없음. 외부 클러스터 E2E 미수행. 완결 시나리오는 ImagePullBackOff 1개 |

**같은 사람의 다른 저장소** · 이력서 허브: <https://woonyong-kr.github.io>
[Clue 제품](https://github.com/woonyong-kr/clue) · [Python Reference](https://github.com/woonyong-kr/k8s-clue-python-reference) · [MiniDB](https://github.com/woonyong-kr/minidb) · [PintOS](https://github.com/woonyong-kr/pintos) · [dx_framework](https://github.com/woonyong-kr/dx_framework)


Kubernetes 장애 당시의 증거를 보존하고, 규칙으로 원인을 판정한 뒤 허용된 manifest 변경만 GitHub Draft PR로 제안하고 배포 이후 상태를 다시 검증한다.

## 해결하는 문제

Kubernetes 장애 대응에서는 관측 시점의 상태, 원인 판단, 실제 변경과 배포 결과가 서로 다른 도구에 흩어지기 쉽다. 이 프로젝트는 네 단계를 하나의 Correlation ID로 연결하고, 자동화가 클러스터를 직접 변경하지 못하도록 권한을 분리한다.

현재 완결한 범위는 다음 한 경로다.

```text
ImagePullBackOff
→ Pod·Event 증거 수집
→ wrong_image_tag 규칙 판정
→ image tag scalar 변경안
→ base SHA가 고정된 GitHub Draft PR
→ 배포 후 새 증거와 변경 전 기준선 비교
```

## 주요 기능

- 읽기 전용 Kubernetes agent의 Pod·Event 증거 수집
- 동일 사건의 중복 처리와 중복 PR 생성을 막는 멱등성 계약
- versioned YAML rule 기반의 결정론적 원인 판정
- Deployment와 허용된 scalar field만 수정하는 patch allowlist
- PR 생성 직전 base SHA 재확인
- Draft PR 강제와 자동 merge·클러스터 직접 변경 차단
- 배포 이후 새 evidence window와 변경 전 기준선 비교
- 실패 단계, reason code, 원본 evidence reference 보존

## 설계에서 제외한 범위

- 모든 Kubernetes 장애 자동 복구
- LLM의 자유로운 YAML 수정
- 클러스터 명령 직접 실행
- 자동 merge와 자동 rollback
- 범용 채팅, 웹 터미널, 비용·트래픽 대시보드
- node collector와 광범위한 CD orchestration

기능 수보다 한 경로의 권한·실패·검증 조건을 끝까지 설명하는 것을 우선했다.

## 설치

요구 사항:

- Python 3.13
- [uv](https://docs.astral.sh/uv/)
- Node.js 22
- Helm

```bash
git clone https://github.com/woonyong-kr/k8s-clue-python-reference.git
cd k8s-clue-python-reference
uv sync --all-groups
cd frontend
npm ci
cd ..
```

Docker, kubectl과 kind는 이미지·manifest·로컬 클러스터 검증을 수행할 때만 필요하다.

### 명칭과 호환 범위

- 제품 표시명은 `Clue`, 이 참조 저장소와 Python project는 `k8s-clue-python-reference`, frontend package는 `clue-console`, Helm chart는 `charts/clue`다.
- `clue diagnose` 등 새 CLI는 [Python 선행 정리 계획](docs/PYTHON-FIRST-PLAN.md)의 목표이며, 이번 명칭 변경으로 구현된 명령이 아니다. 현재 실행 방법은 아래 Make 명령을 사용한다.
- 기존 암호문·cursor 서명·사용자 ID, event·metric·annotation·header·환경변수의 `kyro`/`KYRO` 식별자와 DB·기본 bootstrap identity는 호환을 위해 유지한다. 제품 표시명이 아니라 기존 저장·통신 계약이다. Alembic baseline·migration의 과거 바이트도 변경하지 않는다.
- chart 명칭 변경은 실행 중인 Helm release, 기존 리소스·PVC·DB를 자동 이전하지 않는다. 기존 설치에 바로 덮어쓰지 말고 별도로 이전 범위를 확인해야 한다.
- 기본 이미지 이름은 로컬 빌드용 `clue`와 `clue-console`이다. 원격 이미지 게시·서비스 배포는 이 변경에 포함되지 않는다.

## 사용법

외부 Kubernetes 클러스터나 GitHub 저장소를 변경하지 않고 대표 흐름의 계약을 확인한다.

```bash
make demo
```

전체 저장소를 검증한다.

```bash
make test
make gate-frontend
make manifest-check
make event-bus-equivalence
make build-image
```

로컬 도구 상태는 다음 명령으로 확인한다.

```bash
make doctor
```

## 코드로 따라가기

| 단계 | 구현 | 대표 검증 |
|---|---|---|
| Correlation ID 생성·전파 | [`envelope.py`](src/packages/events/envelope.py), [`dispatch.py`](src/packages/runtime/dispatch.py) | [`test_golden_path_safety_contracts.py`](tests/test_golden_path_safety_contracts.py) |
| 증거 수집 | [`collector.py`](src/services/target/cluster-agent/evidence/collector.py), Kubernetes providers | evidence scope·read-only contract tests |
| incident와 중복 억제 | [`models.py`](src/domains/rca/models.py), [`incident-worker`](src/services/ai/incident-worker/app.py) | incident identity tests |
| 결정론적 RCA | [`causes.py`](src/services/ai/agent/pipeline/causes.py), rule catalog | ImagePullBackOff/RCA tests |
| patch allowlist | [`source_patch.py`](src/domains/gitops/source_patch.py) | merge scope·Kustomize source tests |
| Draft PR | [`github_provider.py`](src/services/gitops/scm-worker/github_provider.py) | base advance·Draft enforcement tests |
| 사후 검증 | [`recovery_verification.py`](src/domains/rca/recovery_verification.py), [`rca-feedback-worker`](src/services/ai/rca-feedback-worker/app.py) | stale window·baseline·recovery tests |

앞으로의 정리 순서와 Java 포팅 인수 조건은 [Python 선행 정리 계획](docs/PYTHON-FIRST-PLAN.md)을 기준으로 한다. 현재 이벤트 계약과 안전장치는 [Golden Path](docs/GOLDEN-PATH.md), 정리 전 실행 구조는 [Project Map](docs/PROJECT-MAP.md)에 정리했다.

## 검증 결과

정리 완료 커밋 기준으로 다음 검증을 통과했다.

- Backend: `203 passed`
- Frontend typecheck, ESLint, Vitest와 production build
- Helm lint/template와 read-only RBAC 검사
- in-process/NATS 이벤트 의미 동등성
- Docker image build
- Alembic single head와 baseline 검증

이 수치는 운영 성능이나 실사용 성과가 아니다. 구현한 계약이 로컬 테스트·빌드·manifest 수준에서 일치함을 뜻한다.

## 프로젝트에서 맡은 범위

5인 팀의 팀장으로 전체 아키텍처, 장애 처리 파이프라인과 서비스 간 인터페이스를 설계했다. 프로젝트 종료 후 포트폴리오 정리 단계에서 기능 표면을 한 Golden Path로 축소하고, 직접 변경·자동 merge를 차단하는 안전 계약과 코드·테스트 근거를 다시 감사했다.

팀 프로젝트이므로 저장소 전체 코드를 개인 구현으로 주장하지 않는다. 면접과 포트폴리오에서는 직접 설계하고 코드로 추적할 수 있는 파이프라인·인터페이스·권한 경계를 설명한다.

## 현재 한계

- 실제 사용자와 운영 트래픽이 없는 데모 프로젝트다.
- 실제 Kubernetes 클러스터와 GitHub App을 연결한 외부 E2E는 수행하지 않았다.
- 대표 완료 시나리오는 ImagePullBackOff 하나다.
- GitHub 외 SCM provider는 지원하지 않는다.
- migration 호환을 위한 과거 ORM·event model 일부가 비실행 상태로 남아 있다.
- 성능·장애 복구 시간·비용 절감 수치는 측정하지 않았다.
