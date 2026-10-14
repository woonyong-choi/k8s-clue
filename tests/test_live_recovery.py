from __future__ import annotations

import base64
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import yaml

from domains.rca.image_pull_recovery import evaluate_image_recovery
from entrypoints import live_recovery as live


@pytest.fixture
def snapshot() -> dict:
    return {
        "observed_at": "2026-10-14T12:01:00+00:00",
        "deployment": {
            "metadata": {"uid": "deployment-1", "generation": 2, "resourceVersion": "10"},
            "spec": {
                "replicas": 1,
                "template": {"spec": {"containers": [{"name": "app", "image": "busybox:1.37.0"}]}},
            },
            "status": {
                "observedGeneration": 2,
                "replicas": 1,
                "updatedReplicas": 1,
                "readyReplicas": 1,
                "availableReplicas": 1,
            },
        },
        "replicasets": {
            "items": [
                {
                    "metadata": {
                        "uid": "rs-1",
                        "ownerReferences": [{"uid": "deployment-1", "controller": True}],
                    }
                }
            ]
        },
        "pods": {
            "items": [
                {
                    "metadata": {
                        "uid": "pod-1",
                        "ownerReferences": [{"uid": "rs-1", "controller": True}],
                    },
                    "spec": {"containers": [{"name": "app", "image": "busybox:1.37.0"}]},
                    "status": {
                        "phase": "Running",
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "containerStatuses": [{"name": "app", "ready": True}],
                    },
                }
            ]
        },
    }


# #5: merge 사실만으로 복구를 선언하지 않고 새 대상 상태를 확인한다.
@pytest.mark.parametrize(
    "case,expected",
    [
        ("healthy", "recovered"),
        ("open", "awaiting_merge"),
        ("stale", "stale_evidence"),
        ("replaced", "deployment_replaced"),
        ("old_generation", "awaiting_rollout"),
        ("controller", "awaiting_controller"),
        ("scaled_down", "replica_count_changed"),
        ("wrong_image", "image_not_applied"),
        ("partial", "rollout_incomplete"),
        ("foreign_pod", "pods_not_ready"),
        ("unready", "pods_not_ready"),
    ],
)
def test_image_recovery_rejects_incomplete_evidence(
    snapshot: dict, case: str, expected: str
) -> None:
    incident = {
        "deployment_uid": "deployment-1",
        "generation": 1,
        "replicas": 1,
        "container": "app",
        "image": "busybox:1.37.0",
    }
    pull = {"merged": True, "merged_at": "2026-10-14T12:00:00Z", "merge_commit_sha": "merged"}
    deployment = snapshot["deployment"]
    pod = snapshot["pods"]["items"][0]
    if case == "open":
        pull["merged"] = False
    elif case == "stale":
        snapshot["observed_at"] = "2026-10-14T11:59:59+00:00"
    elif case == "replaced":
        deployment["metadata"]["uid"] = "different"
    elif case == "old_generation":
        deployment["metadata"]["generation"] = 1
    elif case == "controller":
        deployment["status"]["observedGeneration"] = 1
    elif case == "scaled_down":
        deployment["spec"]["replicas"] = 0
    elif case == "wrong_image":
        deployment["spec"]["template"]["spec"]["containers"][0]["image"] = "other"
    elif case == "partial":
        deployment["status"]["updatedReplicas"] = 0
    elif case == "foreign_pod":
        pod["metadata"]["ownerReferences"][0]["uid"] = "unrelated-rs"
    elif case == "unready":
        pod["status"]["containerStatuses"][0]["ready"] = False

    assert evaluate_image_recovery(incident, pull, snapshot) == expected


# #5: 실제 provider를 포함한 CLI의 외부 I/O 계약과 재실행·검증 경계를 검사한다.
def test_live_cli_reuses_pr_and_requires_merge_and_fresh_rollout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, snapshot: dict
) -> None:
    source = (Path(__file__).parents[1] / "deploy/oss/live-recovery.yaml").read_text()
    files = {"deploy/oss/live-recovery.yaml": source}
    created: list[dict] = []
    pull: dict = {}
    branch_created = False
    now = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
    before = deepcopy(snapshot)
    before["deployment"]["metadata"]["generation"] = 1
    before["deployment"]["spec"]["template"]["spec"]["containers"][0]["image"] = (
        "busybox:clue-missing-image"
    )
    before["pods"]["items"][0]["status"]["containerStatuses"] = [
        {"name": "app", "state": {"waiting": {"reason": "ImagePullBackOff"}}}
    ]
    current_snapshot = before

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal branch_created
        path = request.url.path
        method = request.method
        if path == "/user":
            return httpx.Response(200, json={"login": "operator"})
        if "/git/ref/heads/" in path:
            if path.endswith("/demo/base"):
                return httpx.Response(200, json={"object": {"sha": "a" * 40}})
            return httpx.Response(200 if branch_created else 404, json={})
        if path.endswith("/git/refs"):
            branch_created = True
            return httpx.Response(201, json={})
        if "/contents/" in path:
            filename = path.split("/contents/", 1)[1]
            if method == "PUT":
                files[filename] = base64.b64decode(json.loads(request.content)["content"]).decode()
                return httpx.Response(201, json={})
            content = files[filename]
            return httpx.Response(
                200,
                json={"encoding": "base64", "content": base64.b64encode(content.encode()).decode()},
            )
        if path.endswith("/pulls") and method == "GET":
            return httpx.Response(200, json=[pull] if pull else [])
        if path.endswith("/pulls") and method == "POST":
            body = json.loads(request.content)
            created.append(body)
            pull.update(
                number=12,
                node_id="PR_12",
                html_url="https://github.com/owner/repo/pull/12",
                title=body["title"],
                body=body["body"],
                draft=body["draft"],
                merged=False,
                head={"ref": body["head"], "sha": "b" * 40, "repo": {"full_name": "owner/repo"}},
                base={"ref": body["base"], "sha": "a" * 40, "repo": {"full_name": "owner/repo"}},
            )
            return httpx.Response(201, json=pull)
        if path.endswith("/pulls/12"):
            return httpx.Response(200, json=pull)
        raise AssertionError(f"unexpected API request: {method} {path}")

    module = live._provider_module()
    monkeypatch.setattr(
        module.GithubScmProvider,
        "client",
        lambda self, token: httpx.AsyncClient(
            base_url="https://api.github.com", transport=httpx.MockTransport(handler)
        ),
    )
    monkeypatch.setattr(live, "_provider_module", lambda: module)
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    monkeypatch.setattr(live, "_snapshot", lambda target: deepcopy(current_snapshot))
    state = tmp_path / "incident.sqlite"
    common = _proposal_arguments(state)

    def invoke(arguments: list[str]) -> dict:
        monkeypatch.setattr("sys.argv", ["live_recovery", *arguments])
        assert live.main() == 0
        return json.loads(capsys.readouterr().out)

    assert invoke(common)["status"] == "awaiting_merge"
    assert invoke(common)["pr_url"].endswith("/12")
    verify = ["verify", "--state", str(state)]
    assert invoke(verify)["status"] == "awaiting_merge"
    assert len(created) == 1 and created[0]["draft"] is True
    assert (
        yaml.safe_load(files["deploy/oss/live-recovery.yaml"])["spec"]["template"]["spec"][
            "containers"
        ][0]["image"]
        == "busybox:1.37.0"
    )
    pull.update(merged=True, merged_at=now.isoformat(), merge_commit_sha="c" * 40)
    current_snapshot = snapshot
    assert invoke(verify)["status"] == "stabilizing"
    current_snapshot["observed_at"] = (now + timedelta(seconds=91)).isoformat()
    assert invoke(verify)["status"] == "recovered"
    current_snapshot["pods"]["items"][0]["status"]["phase"] = "Pending"
    assert invoke(verify)["status"] == "pods_not_ready"
    current_snapshot["pods"]["items"][0]["status"]["phase"] = "Running"
    current_snapshot["observed_at"] = (now + timedelta(seconds=92)).isoformat()
    assert invoke(verify)["status"] == "stabilizing"


def _proposal_arguments(state: Path) -> list[str]:
    return [
        "propose",
        "--state",
        str(state),
        "--repo",
        "owner/repo",
        "--base",
        "demo/base",
        "--manifest",
        "deploy/oss/live-recovery.yaml",
        "--context",
        "kind-demo",
        "--namespace",
        "clue-live-recovery",
        "--deployment",
        "clue-live-recovery",
        "--container",
        "app",
        "--image",
        "busybox:1.37.0",
        "--incident",
        "image-pull",
    ]
