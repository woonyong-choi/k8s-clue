# 팀 과제와 개인 확장의 경계

이 저장소는 크래프톤 정글 5인 팀 과제(원본 [minmings111/Kyro-jungle-final](https://github.com/minmings111/Kyro-jungle-final))에서
시작했습니다. 이 문서는 어디까지가 팀 코드이고 어디부터가 과제 종료 후 개인 작업인지,
그리고 개인 작업에서 무엇이 달라졌는지를 커밋 단위로 적습니다.
현재 구조와 검증 수치는 [README](../README.md)에 있습니다.

## 역할

| 항목 | 내용 |
|---|---|
| 원본 팀 저장소 | [minmings111/Kyro-jungle-final](https://github.com/minmings111/Kyro-jungle-final) |
| 팀 구성 | 5인 |
| 본인 담당 | 팀장 · 전체 아키텍처 · 이벤트 런타임 · 서비스 간 계약 · CI · 배포 |
| 팀원 담당 | 룰 카탈로그 · 에이전트와 수집 · PR 생성 · 게이트웨이 인증 |

팀 코드 전체를 개인 구현으로 주장하지 않습니다. 기여는 아래 커밋 범위로 구분합니다.

## 커밋 범위

`git log --reverse --format='%h %ad %s' --date=short` 기준 전체 25개 커밋 중
**팀 기준선 5개, 과제 종료 후 개인 작업 20개**입니다.

```bash
git rev-list --count HEAD            # 25
git rev-list --count e1a9c79..HEAD   # 20
```

팀 기준선은 [`b749f3b`](https://github.com/woonyong-choi/k8s-clue/commit/b749f3b)(2026-08-04, 첫 커밋)부터
[`e1a9c79`](https://github.com/woonyong-choi/k8s-clue/commit/e1a9c79)(2026-08-17, 마지막 확인 커밋)까지입니다.
팀 작업은 이 저장소에 소수의 커밋으로 묶여 들어와 있어 커밋 단위로는 팀 내 기여를 더 쪼갤 수 없습니다.

| 기준선 커밋 | 날짜 | 내용 |
|---|---|---|
| [`b749f3b`](https://github.com/woonyong-choi/k8s-clue/commit/b749f3b) | 2026-08-04 | Kubernetes 장애 복구 / GitOps 워크플로 / 검증 기준 |
| [`c9233d3`](https://github.com/woonyong-choi/k8s-clue/commit/c9233d3) | 2026-08-04 | Python 정리 계획 / Java 인수 기준 / 문서 정리 |
| [`ff2e8c9`](https://github.com/woonyong-choi/k8s-clue/commit/ff2e8c9) | 2026-08-14 | Opsia를 Kyro로 명칭 전환 |
| [`a8219c3`](https://github.com/woonyong-choi/k8s-clue/commit/a8219c3) | 2026-08-15 | 이벤트 버스 모드(in-process / NATS) 검증과 생성을 분리 |
| [`e1a9c79`](https://github.com/woonyong-choi/k8s-clue/commit/e1a9c79) | 2026-08-17 | requirements 재현성 고정 / OpenTelemetry 정렬 / CI 게이트 도입 |

### 과제 종료 후 개인 작업 (2026-09-09 ~ 2026-09-23, 20개 커밋)

| 무엇이 달라졌나 | 커밋 |
|---|---|
| 제품명 전환에 맞춘 식별자 정리(Kyro → Clue). 저장·통신 호환 식별자는 유지 | [`778e1e8`](https://github.com/woonyong-choi/k8s-clue/commit/778e1e8) |
| 참조 구현의 워크플로와 현재 실행 범위를 문서에 명시 | [`2e02605`](https://github.com/woonyong-choi/k8s-clue/commit/2e02605) |
| `make demo` 복구 — 존재하지 않는 `tests/test_incident_alert_event.py`를 가리켜 실행 실패하던 것을 실제 계약 테스트로 교체 | [`6a6fa4b`](https://github.com/woonyong-choi/k8s-clue/commit/6a6fa4b) |
| 운영 데이터 카탈로그 흡수 — `k8s-ops-min`의 수집 완전성 계약·카탈로그·품질 SQL·조회 API를 정본으로 합치고 미검증 MCP는 제외 | [`6ccc94d`](https://github.com/woonyong-choi/k8s-clue/commit/6ccc94d) |
| 죽은 코드·중복 테스트 정리 (284 → 291) | [`8747416`](https://github.com/woonyong-choi/k8s-clue/commit/8747416), [`e274027`](https://github.com/woonyong-choi/k8s-clue/commit/e274027) |
| workflow 파일 actionlint 검사 추가 | [`118800d`](https://github.com/woonyong-choi/k8s-clue/commit/118800d) |
| `make demo`를 kind 클러스터에서 실행하는 CI job 추가 | [`4365a9f`](https://github.com/woonyong-choi/k8s-clue/commit/4365a9f), [`9780fe9`](https://github.com/woonyong-choi/k8s-clue/commit/9780fe9) |

## 정리 전후

기능을 넓히는 대신 Golden Path 하나(ImagePullBackOff)로 좁히고, 안전 경계를 계약 테스트로 고정했습니다.
장애 대응 도구에서 가장 위험한 것은 못 고치는 것이 아니라 잘못 고치는 것이라고 봤기 때문입니다.

| 항목 | 정리 전 | 정리 후 |
|---|---|---|
| 완결 보장 시나리오 | 여러 경로가 부분 구현 | ImagePullBackOff 1개를 계약 테스트로 고정 |
| `make demo` | 실행 실패 (없는 파일 참조) | 86개 계약 테스트 통과 |
| 전체 테스트 | — | 294개 통과 (핵심 주장은 속성 테스트로 증명) |
| 운영 데이터 계층 | 별도 저장소(`k8s-ops-min`)에 분산 | 정본에 흡수 — 카탈로그 79개 통과 |

## 기술 스펙

| 구분 | 내용 |
|---|---|
| 백엔드 언어 | Python 3.13 |
| 패키지·잠금 | uv (`uv.lock`), ruff (lint·format), pytest |
| 백엔드 주요 라이브러리 | FastAPI + Uvicorn, SQLAlchemy 2.0 + Alembic, OpenTelemetry(API·SDK·OTLP), psycopg 3, PyJWT, cryptography |
| 메시징 | in-process event bus / NATS (`nats-py`, 두 모드 동등성 검사) |
| 저장소 | PostgreSQL (outbox·ledger·DLQ, 운영 데이터 카탈로그), Redis |
| 데이터 계층 | 카탈로그 배치 DAG(Airflow 계약), 품질 검사 SQL 8종 + 조회 2종 |
| 프론트엔드 | `clue-console` — React 19, Vite, TypeScript, Vitest (Node.js 22) |
| 배포 | Helm chart [`charts/clue`](../charts/clue/), 컨테이너 이미지 |
| 라이선스 | Apache-2.0 ([`NOTICE`](../NOTICE) — upstream Radar에서 상당 부분 재작성) |

## 남은 마이그레이션

기존 암호문·cursor·DB·event·환경변수의 `kyro` / `KYRO` 식별자는 저장·통신 호환 때문에 유지합니다.
이름 변경만으로 기존 Helm release·PVC·DB가 이전되지는 않습니다. 마이그레이션 경로는 아직 정하지 않았습니다.
