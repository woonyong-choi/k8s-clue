from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_chart_provides_controller_local_session_authority() -> None:
    # #2: 기본 설치의 인증 저장소는 같은 Pod에서만 접근할 수 있어야 한다.
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "clue",
            "charts/clue",
            "--set",
            "image.tag=local",
            "--set",
            "console.image.tag=local",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    objects = list(yaml.safe_load_all(rendered.stdout))
    deployment = next(
        item
        for item in objects
        if item.get("kind") == "Deployment" and item["metadata"]["name"].endswith("-controller")
    )
    containers = {
        item["name"]: item for item in deployment["spec"]["template"]["spec"]["containers"]
    }
    environment = {item["name"]: item.get("value") for item in containers["controller"]["env"]}

    assert environment.get("REDIS_URL") == "redis://127.0.0.1:6379/0"
    assert containers["redis"]["command"][:3] == ["redis-server", "--bind", "127.0.0.1"]
    for item in objects:
        if item.get("kind") == "Service":
            assert all(port["port"] != 6379 for port in item["spec"]["ports"])


def test_chart_rejects_multiple_process_local_session_authorities() -> None:
    # #2: 각 Pod의 독립 세션 저장소를 공유 세션처럼 제공하면 안 된다.
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "clue",
            "charts/clue",
            "--set",
            "image.tag=local",
            "--set",
            "console.image.tag=local",
            "--set",
            "controller.replicas=2",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert rendered.returncode != 0
    assert "controller.replicas must be 1" in rendered.stderr
