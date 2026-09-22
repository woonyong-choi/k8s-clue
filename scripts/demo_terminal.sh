#!/bin/bash
# 터미널 데모: make demo(계약 테스트 3단계) 출력의 앞부분과 Draft PR 수명주기 계약 테스트 이름. 사전 조건: make sync
# 실제 클러스터·GitHub 를 쓰는 E2E 가 아니라 계약 테스트 실행이다.
cd "$(dirname "$0")/.." || exit 1
show() { printf '\033[1;32m$\033[0m %s\n' "$1"; }
echo "# k8s-clue: make demo = 증거/RCA -> base SHA 고정 Draft PR -> 배포 후 검증 계약 테스트 (E2E 아님)"
sleep 2.5
show "make demo 2>&1 | cut -c1-100 | sed -n 1,22p"
make demo 2>&1 | cut -c1-100 | sed -n 1,22p
sleep 4.0
show "uv run pytest tests/test_recovery_pr_lifecycle.py -v 2>&1 | grep PASSED | sed 's/.*:://' | head -8"
uv run pytest tests/test_recovery_pr_lifecycle.py -v 2>&1 | grep PASSED | sed 's/.*:://' | head -8
sleep 4.0
