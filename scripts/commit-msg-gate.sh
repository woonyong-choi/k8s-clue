#!/usr/bin/env bash
set -euo pipefail

python3 - "$@" <<'PY'
import re
import subprocess
import sys
from pathlib import Path

if len(sys.argv) == 3 and sys.argv[1] == "--range":
    subjects = subprocess.check_output(
        ["git", "log", "--no-merges", "--format=%s", sys.argv[2]], text=True
    ).splitlines()
elif len(sys.argv) == 2:
    subjects = Path(sys.argv[1]).read_text().splitlines()[:1] or [""]
else:
    raise SystemExit("usage: commit-msg-gate.sh <message-file> or --range <git-range>")

pattern = re.compile(
    r"(?:feat|fix|refactor|perf|test|docs|style|build|ci|chore|revert)"
    r"\([a-z][a-z0-9-]*\)!?: (.+)"
)
for subject in subjects:
    match = pattern.fullmatch(subject)
    description = match.group(1) if match else ""
    if not (
        description
        and len(description) <= 50
        and re.search(r"[가-힣]", description)
        and re.search(r"(?:추가|제거|수정|변경|개선|분리|통합|이동|정리|갱신|적용|도입|되돌림)$", description)
        and not description.endswith((".", "!", "?"))
    ):
        raise SystemExit("commit subject must use type(scope): 한글 설명 (50 characters or fewer)")
PY
