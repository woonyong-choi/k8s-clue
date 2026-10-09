# Python 제품 완성 계획

| 항목 | 값 |
|---|---|
| 상태 | 결정 |
| 관련 작업 | [초기 실행과 표시 오류 수정](https://github.com/woonyong-choi/k8s-clue/issues/2) |

## 요약

Clue를 Python으로 완성한다. 로컬 Docker와 kind에서 장애 증거 수집, 원인 판정, 실제 GitHub Draft PR, 사람의 검토와 배포, 회복 확인을 한 사건으로 연결한다. EKS와 Java 이전은 완료 조건에 포함하지 않는다.

## 동기

현재 저장소에는 수집, 규칙 엔진, 제한된 패치, Draft PR, 회복 검증과 저장 계층이 있다. 컴포넌트 계약 검증과 실제 외부 연동의 완료 범위는 다르다. 기존 코드를 다른 언어로 넘기기 위해 줄이는 대신, Python 실행 경로를 연결하고 실패 조건을 검증한다.

## 예시

### ImagePullBackOff 진단과 회복

1. 사용자가 kind에 잘못된 이미지 태그를 가진 Deployment를 배포한다.
2. 에이전트가 Pod와 Event를 읽고 출처와 관측 시각이 포함된 증거를 보낸다.
3. Clue가 원인과 부족한 증거를 기록하고 허용된 수정 후보를 만든다.
4. 사용자가 후보를 선택하면 Clue가 고정된 base SHA와 허용 필드에 한정한 GitHub Draft PR을 만든다.
5. 사용자가 PR을 검토하고 병합한 커밋을 로컬 클러스터에 배포한다.
6. Clue가 배포 이후 증거를 다시 수집하고 회복 여부를 기록한다.

증거가 부족하거나 저장소 원문이 승인한 상태와 다르면 PR 생성을 중단한다. PR 병합, 배포, 회복 확인은 서로 다른 결과로 기록한다.

## 상세 설계

### 저장소와 실행 경로

이 저장소가 Python 구현과 설치 산출물의 정본이다. 팀 과제 원본과 개인 확장의 구분은 [기여 범위](personal-extension.md)에 유지한다. 소스와 서비스 책임은 [Project Map](PROJECT-MAP.md)을 따른다.

| 구성 | 현재 경로와 보존 이유 |
|---|---|
| Controller | `src/entrypoints/app.py`가 gateway와 worker를 한 프로세스로 조합 |
| Agent | `src/services/target/cluster-agent/`가 대상 Kubernetes 증거 수집 |
| Gateway | 인증, 증거 접수, RCA와 복구 계획 조회, GitHub webhook 처리 |
| PostgreSQL | 증거, outbox, 처리 원장, 복구 상태와 카탈로그 보존 |
| Redis | gateway의 세션 저장과 요청 한도 처리, 연결 장애 시 인증 거부 |
| Event bus | `inprocess`와 NATS 모드 유지. 영속 처리 검증은 NATS JetStream 경로를 대상으로 수행 |
| Console | 사건 목록과 진단·수정안 조회 |
| Helm과 migration | 설치, 읽기 전용 RBAC, 저장 데이터 호환성 유지 |

`uv run python src/entrypoints/app.py --check`는 서비스 발견과 진입점 구성을 검사한다. DB, Redis, NATS, GitHub와 실제 통신을 완료했다는 의미는 아니다. `clue diagnose`는 아직 구현되지 않았으며 기존 Controller와 별개의 CLI 재구성을 완료 전제로 두지 않는다.

### 로컬 검증 환경

개발 중에는 Docker Compose로 의존 서비스를 실행하고 kind에 대상 API와 에이전트를 배포한다. 최종 설치 검증은 Clue도 컨테이너로 패키징해 kind에 설치한다. 현재 `docker-compose.catalog.yml`은 카탈로그 PostgreSQL만 실행한다. Helm chart는 Controller Pod 안의 Redis에 `redis://127.0.0.1:6379/0`으로 연결한다. Redis 포트는 Pod 밖으로 공개하지 않으며 Controller replica는 하나로 제한한다. Redis가 재시작되면 세션과 요청 한도 기록은 사라져 다시 로그인해야 한다. 업무 증거와 복구 상태는 PostgreSQL에 보존한다. 이 구성의 실제 kind 설치 검증은 별도로 수행한다.

단일 노드 kind에서 시작한다. 스케줄링 장애 실험에 필요한 경우 로컬 노드를 늘린다. GitHub 연동에는 인터넷과 저장소 권한이 필요하다. AWS IAM, EBS, ALB와 여러 물리 서버의 장애 대응은 검증 범위에 포함하지 않는다.

### 코드 정리 기준

`make setup`은 환경 예제와 Python 의존성을 준비한다. 저장소에 설정이 없는 pre-commit과 import-linter 의존성 및 자동 hook 설치는 제거한다. 기존 `commit-msg` hook이 참조하는 스크립트 경로는 유지하며 `type(scope): 한글 설명`을 검사한다. 이전에 별도로 설치한 pre-commit hook은 사용자가 유지 여부를 판단하고 직접 관리한다. 코드 검사는 `make gate`를 기준으로 한다.

호출자, 동적 로딩, 설정, 테스트가 모두 없는 코드만 삭제한다. 단순 이름 검색 결과만으로 SQLAlchemy 모델, 자동 발견되는 Repository, 등록 데코레이터를 삭제하지 않는다. 테스트 fixture와 외부 서비스를 대체하는 테스트용 구현은 유지한다.

인증과 변경 권한을 검사하는 코드, 영속 데이터와 migration, 기존 이벤트와 서명 식별자는 보존한다. `kyro`와 `KYRO` 이름은 저장·통신 호환 계약이므로 일괄 치환하지 않는다. 삭제한 코드는 Git 이력으로 복구한다.

### 작업 순서

| 순서 | 작업 | 완료 증거 |
|---|---|---|
| 1 | Python 제품 기준과 현재 코드 정리 | 실행 기준선, 재현된 오류의 회귀 검증, 삭제 근거 |
| 2 | ImagePullBackOff 전체 연결 | 실제 증거, Draft PR, 배포 커밋, 회복 결과의 연결 |
| 3 | 수집 누락, 중복, 재시작과 자원 한도 보강 | 부분 수집 표시, 중복 PR 방지, 재시작 후 처리 재개, 큐와 메모리 한도 |
| 4 | 여섯 장애군과 독립 평가 데이터 | 장애 주입 조건, 정답 근거, 오진과 판단 유보 기록 |
| 5 | API 부하, 설치 산출물과 사용 문서 | 요청 지표, 장애 전후 비교, 깨끗한 환경에서 재현 가능한 설치 |

여섯 장애군은 이미지 가져오기 실패, 반복 종료, OOM, probe 실패, Pending, Service 연결 실패다. 각 장애군의 진단, PR 생성, 회복 확인 지원 여부를 따로 표시한다. 원인을 알 수 없는 애플리케이션 오류에는 임의의 YAML 수정을 제안하지 않는다.

RCA 카탈로그에서 역산한 합성 데이터는 후보 도달 여부를 확인하는 데 쓴다. 실제 정확도 평가는 별도로 장애를 주입하고 보관한 증거로 수행한다. [기존 평가 결과](../evals/results.md)의 범위를 확대 해석하지 않는다.

### 데모와 회귀 검증

`make demo`는 계약 테스트를 실행한다. `DEMO_KIND_CONTEXT`를 설정하면 별도 kind 재현 장면을 먼저 실행한다. 이 장면의 실제 증거가 뒤의 계약 테스트와 연결된 것은 아니다. `DEMO_SKIP_PR=0`은 구현하지 않은 실제 PR 실행을 요청하므로 장면 실행 전에 오류로 종료한다. 토큰을 추가해도 실제 PR 검증으로 바뀌지 않는다.

Frontend는 API 응답의 표시 필드와 목록 원소를 검증한다. 잘못된 응답은 오류 상태로 표시하며 사건 행을 조용히 제외하지 않는다. 선택되지 않은 첫 후보를 선택된 수정안으로 표시하지 않는다.

### 요구사항

| 요구사항 | 검증 계획 |
|---|---|
| 기존 서비스 구성을 유지한다. | `src/entrypoints/app.py --check`와 `tests/test_controller_composition.py` |
| 허용 범위 밖 변경과 자동 병합을 거부한다. | [Golden Path 안전 계약](GOLDEN-PATH.md)의 회귀 테스트 |
| 잘못된 화면 입력이 렌더링 오류나 사건 누락으로 이어지지 않는다. | `frontend/src/api.test.ts`의 응답 검증과 실제 화면 확인 |
| 미구현 live PR 데모를 성공으로 출력하지 않는다. | `tests/test_demo_mode.py` |
| 실제 ImagePullBackOff부터 회복까지 연결한다. | 실제 수집 증거, GitHub PR, 배포 커밋과 회복 기록 대조 |
| 장애 중 재시작과 중복 전달을 견딘다. | DB 저장, 이벤트 발행, GitHub 응답 사이 프로세스 종료 실험 |
| 새 DB 초기화가 삭제된 기능의 테이블을 요구하지 않는다. | `tests/test_database_bootstrap.py`의 초기화, 재실행과 스키마 확인 |
| 기본 설치가 loopback Redis를 인증 저장소로 사용한다. | `tests/test_chart_sessions.py`와 실제 로그인, 조회, 만료 확인 |
| 클라우드 계정 없이 전체 설치와 부하 실험을 재현한다. | Docker와 kind의 새 설치, API 요청 지표와 자원 제한 기록 |
