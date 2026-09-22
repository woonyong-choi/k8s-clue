# 🔎 Clue Python Reference

Kubernetes 장애 증거를 보존하고 규칙 기반 RCA로 원인을 판정한 뒤, 허용된 GitOps 변경을 GitHub Draft PR로 제안하는 Python 참조 구현입니다. 사건 ID로 증거·판정·변경 제안·배포 후 검증을 연결합니다.

[Project Map](docs/PROJECT-MAP.md) · [문서 목차](docs/README.md) · [Golden Path](docs/GOLDEN-PATH.md)

## 설치와 대표 실행

Python 3.13과 [uv](https://docs.astral.sh/uv/getting-started/installation/)가 필요합니다.

```bash
git clone https://github.com/woonyong-choi/k8s-clue.git
cd k8s-clue
uv sync --all-groups
make demo
```

`make demo`는 ImagePullBackOff 증거·RCA, base SHA가 고정된 Draft PR, 배포 후 증거 비교의 **계약 테스트를 순서대로 실행**합니다. 실제 클러스터에서 장애를 만들거나 GitHub에 PR을 발행하는 E2E 데모는 아닙니다.

```bash
make test                  # Backend lint와 검사
make doctor                # 로컬 도구 확인
# 프론트엔드 확인은 Node.js 22, manifest 확인은 Helm 필요
make gate-frontend
make manifest-check
```

Docker·kubectl·kind는 이미지·클러스터 검증에 사용합니다. 전체 도구를 설치하기 전에는 필요한 실행 범위를 `make doctor`와 [설치 문서](docs/README.md)에서 확인합니다.

## 구현과 설계

ImagePullBackOff → Pod·Event 증거 → wrong_image_tag 규칙 → 허용된 image tag 변경 → Draft PR → 새 증거와 변경 전 기준선 비교가 대표 경로입니다.

| 단계 | 핵심 코드와 경계 |
| --- | --- |
| 증거 수집 | [collector.py](src/services/target/cluster-agent/evidence/collector.py), 읽기 전용 agent |
| 사건·중복 억제 | [models.py](src/domains/rca/models.py), 동일 사건의 중복 처리 방지 |
| 결정론적 RCA | [causes.py](src/services/ai/agent/pipeline/causes.py), versioned rule |
| 제한된 수정 | [source_patch.py](src/domains/gitops/source_patch.py), Deployment·scalar allowlist |
| Draft PR | [github_provider.py](src/services/gitops/scm-worker/github_provider.py), 생성 직전 base SHA 재확인 |
| 회복 검증 | [recovery_verification.py](src/domains/rca/recovery_verification.py), 새 evidence window와 기준선 비교 |

자동 merge·클러스터 직접 변경·자동 rollback은 하지 않습니다. 실패 단계·reason code·원본 evidence reference를 보존합니다. 인터페이스와 Java 이전 조건은 [Python 선행 계획](docs/PYTHON-FIRST-PLAN.md)에 있습니다.

## 현재 상태와 호환 범위

현재 완결 시나리오는 ImagePullBackOff 하나이며 실제 사용자·운영 트래픽과 외부 클러스터·GitHub App E2E는 미검증입니다. `clue diagnose`는 계획된 CLI이고 현재 실행 명령은 Make 진입점입니다.

Python project는 `k8s-clue-python-reference`, frontend는 `clue-console`, Helm chart는 [charts/clue](charts/clue/)입니다. 기존 암호문·cursor·DB·event·환경변수의 `kyro`/`KYRO` 식별자는 저장·통신 호환 때문에 유지합니다. 이름 변경만으로 기존 Helm release·PVC·DB가 이전되지는 않습니다.

## 기여

5인 팀의 팀장으로 전체 아키텍처, 장애 파이프라인과 서비스 간 인터페이스를 설계했습니다. 프로젝트 종료 후 Golden Path로 기능을 좁히고 권한·실패·복구 경계를 감사했습니다. 팀 코드 전체를 개인 구현으로 주장하지 않으며 기여는 해당 코드와 변경 이력으로 구분합니다.
