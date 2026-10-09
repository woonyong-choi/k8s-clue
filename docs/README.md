# Clue 문서

Clue의 Python 구현과 제품 완성 계획을 설명한다. 문서는 한국어로 쓴다. 처음이면 [Python 제품 완성 계획](PYTHON-FIRST-PLAN.md)과 [설계 근거](design.md)부터 읽는다.

| 문서 | 내용 |
|---|---|
| [Project Map](PROJECT-MAP.md) | 현재 서비스, 화면과 디렉터리 책임 |
| [Python 제품 완성 계획](PYTHON-FIRST-PLAN.md) | 로컬 실행 환경, 보존할 코드와 완료 기준 |
| [설계 근거](design.md) | byte span 치환, 규칙 기반 판정과 수집 범위의 한계 |
| [Golden Path](GOLDEN-PATH.md) | ImagePullBackOff 증거부터 회복 확인까지의 안전 계약 |
| [팀 과제와 개인 확장의 경계](personal-extension.md) | 역할, 커밋 범위와 검증 근거 |
| [수집 완전성 계약](collection-contract.md) | completed, partial, unavailable과 사유 전달 |
| [메타데이터 카탈로그](metadata-catalog.md) | 자산, 스키마 계약, 리니지와 실행 단위 모델 |
| [품질 검사 SQL](sql-quality-checks.md) | 신선도, 드리프트, 중복과 리니지 검사 |
| [카탈로그 조회 API](catalog-api.md) | 응답 envelope, 페이지네이션과 상태 코드 |

실행과 검증 명령은 루트 [README](../README.md)를 따른다.
