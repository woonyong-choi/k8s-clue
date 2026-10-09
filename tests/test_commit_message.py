from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("subject", "returncode"),
    [
        ("fix(repo): 초기 실행 오류 수정", 0),
        ("refactor(runtime): 미사용 세션 코드 제거", 0),
        ("fix: 실행 / 오류", 1),
        ("fix(repo): fix startup", 1),
    ],
)
def test_existing_commit_hook_uses_current_subject_contract(
    tmp_path: Path, subject: str, returncode: int
) -> None:
    # #2: 기존 hook 경로를 유지하면서 type(scope) 형식을 허용해야 한다.
    message = tmp_path / "message"
    message.write_text(subject + "\n")

    result = subprocess.run(
        ["bash", "scripts/commit-msg-gate.sh", str(message)],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == returncode
