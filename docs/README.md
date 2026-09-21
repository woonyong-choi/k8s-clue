# Clue Python 참조 구현 문서

이 저장소는 Clue를 Java로 포팅하기 전에 제품 행동과 안전 계약을 Python으로 정리하는 참조 구현입니다.

- [Python 선행 정리 계획](./PYTHON-FIRST-PLAN.md): Clue CLI 기준 재구성, 제거 순서와 Java 인수 조건
- [Golden Path](./GOLDEN-PATH.md): ImagePullBackOff 증거부터 배포 후 검증까지의 현재 안전 계약
- [Project Map](./PROJECT-MAP.md): 정리 전 runtime, route와 디렉터리의 현재 책임

## 운영 데이터 카탈로그

`k8s-ops-min` 에서 옮겨 온 수집·정규화·카탈로그·품질 검증 계층입니다.

- [수집 완전성 계약](./collection-contract.md): completed / partial / unavailable 과 사유를 함께 넘기는 규칙
- [메타데이터 카탈로그](./metadata-catalog.md): 자산·스키마 계약·리니지·실행 단위의 데이터 모델
- [품질 검사 SQL](./sql-quality-checks.md): 신선도·드리프트·중복·리니지 단절 검사 질의와 근거
- [카탈로그 조회 API](./catalog-api.md): 응답 envelope·페이지네이션·상태 코드와 검증표

빠른 실행과 전체 검증 명령은 저장소 루트 [README](../README.md)를 기준으로 합니다.
