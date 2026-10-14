"""격리된 실증용 CLI: Draft PR 제안과 읽기 전용 복구 관측을 분리한다."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import logging
import os
import re
import sqlite3
import subprocess
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import yaml

from domains.gitops.source_patch import (
    ImageScalarReplacement,
    canonical_manifest_digest,
    materialize_image_patch,
)
from domains.rca.image_pull_recovery import evaluate_image_recovery, image_pull_failed
from domains.scm.events import SafePrFilePatch, SafePrRequestedBody
from packages.runtime.dispatch import EventContext

LOGGER = logging.getLogger(__name__)
STABILITY_SECONDS = 30
MAX_SAMPLE_GAP_SECONDS = 60


class IncidentStore:
    # cost: time O(n), heap O(n), stack O(1), io 3
    # vars: n = 저장한 사건 JSON 바이트 수
    # basis: estimate
    def __init__(self, path: Path) -> None:
        self.connection = sqlite3.connect(path, timeout=5)
        os.chmod(path, 0o600)
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS incident (id INTEGER PRIMARY KEY, body TEXT)"
        )
        self.connection.commit()
        self.connection.execute("BEGIN IMMEDIATE")
        row = self.connection.execute("SELECT body FROM incident WHERE id = 1").fetchone()
        self.data: dict[str, Any] = json.loads(row[0]) if row else {}

    # cost: time O(n), heap O(n), stack O(1), io 2
    # vars: n = 사건 JSON 바이트 수
    # basis: estimate
    def save(self) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO incident VALUES (1, ?)", (json.dumps(self.data),)
        )
        self.connection.commit()
        self.connection.execute("BEGIN IMMEDIATE")

    def close(self) -> None:
        self.connection.rollback()
        self.connection.close()

    async def get_workflow_approval(self, approval_id: str, workspace_id: str) -> dict | None:
        approval = self.data["approval"]
        if approval_id != approval["approval_id"] or workspace_id != approval["workspace_id"]:
            return None
        return approval

    async def save_pull_request(
        self, correlation_id: str, pr_url: str, title: str, body: str, status: str
    ) -> None:
        if correlation_id != self.data["incident_id"]:
            raise ValueError("incident identity mismatch")
        self.data["pr_url"] = pr_url
        self.data["status"] = status
        self.save()


# cost: time O(n), heap O(n), stack O(1), io O(1)
# vars: n = 단일 사건의 GitHub 및 Kubernetes 응답 바이트 수
# basis: estimate
def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    store = None
    try:
        store = IncidentStore(args.state)
        _ensure_token()
        result = asyncio.run(_run(args, store))
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (
        OSError,
        ValueError,
        RuntimeError,
        sqlite3.Error,
        httpx.HTTPError,
        subprocess.SubprocessError,
        KeyError,
        TypeError,
        yaml.YAMLError,
    ) as exc:
        if store is not None and args.command == "verify":
            store.data.update(status="verification_error", healthy_since=None)
            try:
                store.save()
            except sqlite3.Error:
                LOGGER.error("failed to persist verification failure")
        LOGGER.error("live recovery failed: %s", exc)
        return 1
    finally:
        if store is not None:
            store.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    propose = subparsers.add_parser("propose", help="제안만 생성하며 merge나 배포는 하지 않음")
    propose.add_argument("--state", type=Path, required=True)
    for name in (
        "repo",
        "base",
        "manifest",
        "context",
        "namespace",
        "deployment",
        "container",
        "image",
        "incident",
    ):
        propose.add_argument("--" + name, required=True)
    verify = subparsers.add_parser("verify", help="실제 merge와 새 상태를 조회; 30초 안정화 관측")
    verify.add_argument("--state", type=Path, required=True)
    return parser


# cost: time O(n), heap O(n), stack O(1), io 1
# vars: n = 명령 출력 바이트 수; 실행 시간은 30초로 제한
# basis: estimate
def _command(arguments: list[str]) -> str:
    result = subprocess.run(arguments, capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(f"{arguments[0]} failed with exit code {result.returncode}")
    return result.stdout


def _ensure_token() -> None:
    if not os.environ.get("GITHUB_TOKEN"):
        os.environ["GITHUB_TOKEN"] = _command(["gh", "auth", "token"]).strip()
    if os.environ.get("GITHUB_API_BASE", "https://api.github.com") != "https://api.github.com":
        raise ValueError("live verification requires the real GitHub API")


def _provider_module() -> Any:
    path = Path(__file__).parents[1] / "services/gitops/scm-worker/github_provider.py"
    spec = importlib.util.spec_from_file_location("live_recovery_github", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load GitHub provider")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _run(args: argparse.Namespace, store: IncidentStore) -> dict[str, Any]:
    module = _provider_module()
    provider = module.GithubScmProvider()
    async with provider.client(provider.github_token()) as client:
        if args.command == "propose":
            await _prepare(args, store, provider, client)
            await _propose(store, module, provider, client)
        else:
            await _verify(store, provider, client)
    return {
        key: store.data.get(key)
        for key in ("incident_id", "pr_url", "status", "merge_sha", "observed_at", "healthy_since")
    }


# cost: time O(n), heap O(n), stack O(1), io 4
# vars: n = namespace 내 Deployment·ReplicaSet·Pod 응답 바이트 수
# basis: estimate
def _snapshot(target: dict[str, Any]) -> dict[str, Any]:
    prefix = [
        "kubectl",
        "--context",
        target["context"],
        "--request-timeout=15s",
        "-n",
        target["namespace"],
        "get",
    ]
    started = datetime.now(UTC).isoformat()
    deployment = json.loads(_command([*prefix, "deployment", target["deployment"], "-o", "json"]))
    replicasets = json.loads(_command([*prefix, "replicasets", "-o", "json"]))
    pods = json.loads(_command([*prefix, "pods", "-o", "json"]))
    final = json.loads(_command([*prefix, "deployment", target["deployment"], "-o", "json"]))
    if deployment["metadata"]["resourceVersion"] != final["metadata"]["resourceVersion"]:
        raise RuntimeError("deployment changed during evidence collection; retry")
    return {
        "deployment": deployment,
        "replicasets": replicasets,
        "pods": pods,
        "observed_at": started,
    }


async def _prepare(
    args: argparse.Namespace, store: IncidentStore, provider: Any, client: httpx.AsyncClient
) -> None:
    target = {
        key: getattr(args, key)
        for key in (
            "repo",
            "base",
            "manifest",
            "context",
            "namespace",
            "deployment",
            "container",
            "image",
        )
    }
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", args.incident):
        raise ValueError("incident must be a lowercase identifier of at most 64 characters")
    if store.data:
        if store.data["target"] != target or store.data["incident_id"] != args.incident:
            raise ValueError("state already belongs to a different incident or target")
        return
    base_sha = await provider.base_branch_sha(client, args.repo, args.base)
    source = await provider.source_file_content(client, args.repo, base_sha, args.manifest)
    before = _snapshot(target)
    desired = _image_patch(source, before, target)
    response = await client.get("/user")
    response.raise_for_status()
    actor = response.json()["login"]
    request = _request(target, args.incident, base_sha, desired)
    metadata = before["deployment"]["metadata"]
    store.data = {
        "incident_id": args.incident,
        "target": target,
        "request": asdict(request),
        "approval": _approval(request, source, actor),
        "before": before,
        "deployment_uid": metadata["uid"],
        "generation": metadata["generation"],
        "replicas": before["deployment"]["spec"].get("replicas", 1),
        "container": args.container,
        "image": args.image,
        "status": "prepared",
    }
    store.save()


def _image_patch(source: str, before: dict[str, Any], target: dict[str, Any]) -> str:
    manifest = yaml.safe_load(source)
    identity = manifest["metadata"]
    if manifest.get("kind") != "Deployment" or identity.get("name") != target["deployment"]:
        raise ValueError("source does not match target Deployment")
    if identity.get("namespace") != target["namespace"]:
        raise ValueError("source must explicitly declare the target namespace")
    if not image_pull_failed(before, target["container"]):
        raise ValueError("target has no observed image pull failure")
    containers = before["deployment"]["spec"]["template"]["spec"]["containers"]
    matches = [item for item in containers if item["name"] == target["container"]]
    if len(matches) != 1:
        raise ValueError("target container is ambiguous")
    return materialize_image_patch(
        source,
        source_type="raw-yaml",
        expected_source_sha256=canonical_manifest_digest(manifest),
        replacements=(
            ImageScalarReplacement(target["container"], matches[0]["image"], target["image"]),
        ),
    )


def _digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def _request(target: dict[str, Any], incident: str, sha: str, desired: str) -> SafePrRequestedBody:
    return SafePrRequestedBody(
        title=f"fix(gitops): {incident} 이미지 복구 제안",
        body=f"ImagePullBackOff 복구 제안. 사건 ID: {incident}. 이미지 하나만 변경한다. "
        "검토 후 반영하며 실제 클러스터의 새 증거로 복구 여부를 확인한다.",
        provider="github",
        pr_kind="safe_pr_manifest_edit",
        patches=[SafePrFilePatch(path=target["manifest"], content=desired)],
        workflow_run_id=incident,
        repository_id=target["repo"],
        repo_ref=target["repo"],
        base_branch=target["base"],
        manifest_path=target["manifest"],
        commit_sha=sha,
        approval_ref=f"{incident}-proposal",
        patch_sha256=_digest(desired),
    )


def _approval(request: SafePrRequestedBody, source: str, actor: str) -> dict[str, Any]:
    approval = {
        key: getattr(request, key)
        for key in (
            "workspace_id",
            "workflow_run_id",
            "application_id",
            "binding_id",
            "environment",
        )
    }
    return {
        **approval,
        "approval_id": request.approval_ref,
        "status": "granted",
        "decision": "granted",
        "requested_by": actor,
        "decided_by": actor,
        "details": {
            "authority": request.pr_kind,
            "repository_id": request.repository_id,
            "repo_ref": request.repo_ref,
            "branch": request.base_branch,
            "manifest_path": request.manifest_path,
            "base_sha": request.commit_sha,
            "patch_sha256": request.patch_sha256,
            "source_sha256": _digest(source),
            "desired_sha256": _digest(request.patches[0].content),
        },
    }


def _stored_request(store: IncidentStore) -> SafePrRequestedBody:
    data = dict(store.data["request"])
    data["patches"] = [SafePrFilePatch(**patch) for patch in data["patches"]]
    return SafePrRequestedBody(**data)


async def _propose(
    store: IncidentStore, module: Any, provider: Any, client: httpx.AsyncClient
) -> None:
    request = _stored_request(store)
    if store.data.get("pr_number"):
        pull = await _pull(client, request.repo_ref, store.data["pr_number"])
        _validate_pull(pull, store, module)
        return
    existing = await provider.find_existing_pr(
        client,
        request.repo_ref,
        module.branch_name(request),
        request.base_branch,
        request,
        require_request_match=True,
    )
    if existing is None:
        branch = module.branch_name(request)
        response = await client.get(f"/repos/{request.repo_ref}/git/ref/heads/{branch}")
        if response.status_code != 404:
            response.raise_for_status()
            raise ValueError(
                "proposal branch exists without a matching draft; inspect before retry"
            )
        context = EventContext(
            event_id=request.workflow_run_id,
            subject="safe_pr.requested",
            correlation_id=request.workflow_run_id,
            causation_id=None,
            db=store,
        )
        result = await provider.create_pull_request(request, context)
        if result.number is None:
            raise ValueError("GitHub did not return a PR number")
        existing = await _pull(client, request.repo_ref, result.number)
    _validate_pull(existing, store, module)
    content = await provider.source_file_content(
        client, request.repo_ref, existing["head"]["sha"], request.manifest_path
    )
    if content != request.patches[0].content:
        raise ValueError("existing PR content differs from the recorded proposal")
    store.data.update(
        pr_number=existing["number"],
        pr_url=existing["html_url"],
        head_sha=existing["head"]["sha"],
        status="awaiting_merge",
    )
    store.save()


def _validate_pull(pull: dict[str, Any], store: IncidentStore, module: Any) -> None:
    request = _stored_request(store)
    expected = (
        request.repo_ref,
        request.base_branch,
        module.branch_name(request),
        module.pull_request_body(request),
    )
    actual = (
        pull["base"]["repo"]["full_name"],
        pull["base"]["ref"],
        pull["head"]["ref"],
        pull.get("body"),
    )
    if actual != expected:
        raise ValueError("PR identity does not match the incident")
    if pull["head"]["repo"]["full_name"] != request.repo_ref:
        raise ValueError("PR head repository differs from the proposal")
    if store.data.get("head_sha", pull["head"]["sha"]) != pull["head"]["sha"]:
        raise ValueError("PR head changed after the recorded proposal")


# cost: time O(n), heap O(n), stack O(1), io 1
# vars: n = PR API 응답 바이트 수
# basis: estimate
async def _pull(client: httpx.AsyncClient, repo: str, number: int) -> dict[str, Any]:
    response = await client.get(f"/repos/{repo}/pulls/{number}")
    response.raise_for_status()
    return response.json()


async def _verify(store: IncidentStore, provider: Any, client: httpx.AsyncClient) -> None:
    if not store.data.get("pr_number"):
        raise ValueError("no recorded PR; run propose first")
    request = _stored_request(store)
    store.data["status"] = "verification_started"
    store.save()
    pull = await _pull(client, request.repo_ref, store.data["pr_number"])
    _validate_pull(pull, store, _provider_module())
    if pull.get("merged") is not True:
        store.data.update(status="awaiting_merge", healthy_since=None)
        store.save()
        return
    merged = await provider.source_file_content(
        client, request.repo_ref, pull["merge_commit_sha"], request.manifest_path
    )
    current = await provider.source_file_content(
        client, request.repo_ref, request.base_branch, request.manifest_path
    )
    if merged != request.patches[0].content or current != merged:
        raise ValueError("merged or current source differs from the proposal")
    snapshot = _snapshot(store.data["target"])
    result = evaluate_image_recovery(store.data, pull, snapshot)
    result = _stabilize(store.data, snapshot, result)
    store.data.update(
        status=result,
        after=snapshot,
        observed_at=snapshot["observed_at"],
        merge_sha=pull["merge_commit_sha"],
    )
    store.save()


def _stabilize(incident: dict[str, Any], snapshot: dict[str, Any], result: str) -> str:
    now = datetime.fromisoformat(snapshot["observed_at"])
    previous = incident.get("observed_at")
    gap = (now - datetime.fromisoformat(previous)).total_seconds() if previous else None
    if result != "recovered":
        incident["healthy_since"] = None
        return result
    if not incident.get("healthy_since") or gap is None or not 0 < gap <= MAX_SAMPLE_GAP_SECONDS:
        incident["healthy_since"] = now.isoformat()
    elapsed = (now - datetime.fromisoformat(incident["healthy_since"])).total_seconds()
    return "recovered" if elapsed >= STABILITY_SECONDS else "stabilizing"


if __name__ == "__main__":
    raise SystemExit(main())
