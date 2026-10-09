from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_demo_rejects_unimplemented_live_pr_before_running_scenes() -> None:
    # #2: 실행하지 않는 live PR을 검증한 것처럼 출력하면 안 된다.
    result = subprocess.run(
        ["bash", "-c", "uv() { echo contract-tests; }; export -f uv; bash scripts/oss-demo.sh"],
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, "DEMO_SKIP_PR": "0", "DEMO_KIND_CONTEXT": "", "GITHUB_TOKEN": "unused"},
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 1
    assert "live Draft PR creation is not implemented" in result.stderr
    assert "contract-tests" not in result.stdout
