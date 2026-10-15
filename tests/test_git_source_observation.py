"""Issue #23: commit-pinned Git source observations."""

from __future__ import annotations

import asyncio
import base64
import os
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select

from domains.gitops import source_observation
from domains.gitops.events import GitWebhookReceivedBody
from domains.gitops.models import GitSourceObservation
from domains.gitops.repository import RepoChangeRepository
from domains.gitops.repository_discovery import RepositoryDiscoveryError
from domains.gitops.router import build_git_webhook_bodies
from packages.events.envelope import event as envelope
from packages.runtime.controller import ControllerRuntime
from packages.runtime.dispatch import EventContext, make_event_handler

ROOT = Path(__file__).resolve().parents[1]
COMMIT_SHA = "a" * 40
TARGET = {
    "workspace_id": "default",
    "repository_id": "repository-1",
    "repo_ref": "owner/config",
    "branch": "main",
    "watch_target_id": "watch-1",
    "binding_id": "binding-1",
    "application_id": "app-1",
    "environment": "sandbox",
    "cluster_id": "cluster-1",
    "manifest_path": "deploy/game.yaml",
    "source_type": "raw-yaml",
    "namespace": "games",
    "app_name": "game",
    "credential_ref": "",
}


class FakeStore:
    def __init__(
        self,
        rows: dict[tuple[str, ...], dict[str, Any]] | None = None,
        target: dict[str, Any] | None = None,
    ) -> None:
        self.rows = rows if rows is not None else {}
        self.target = target if target is not None else TARGET.copy()

    async def list_active_github_poll_targets(
        self, workspace_id: str, *, binding_id: str
    ) -> list[dict[str, Any]]:
        return (
            [self.target.copy()]
            if workspace_id == "default" and binding_id == self.target["binding_id"]
            else []
        )

    async def get_workspace_credential(self, workspace_id: str, provider: str, scope: str) -> None:
        return None

    async def get_git_source_observation(
        self, workspace_id: str, binding_id: str, commit_sha: str, manifest_path: str
    ) -> dict[str, Any] | None:
        return self.rows.get((workspace_id, binding_id, commit_sha, manifest_path))

    async def record_git_source_observation(self, payload: dict[str, Any]) -> dict[str, Any]:
        key = (
            str(payload["workspace_id"]),
            str(payload["binding_id"]),
            str(payload["commit_sha"]),
            str(payload["manifest_path"]),
        )
        existing = self.rows.get(key)
        if existing is not None and existing["status"] == "observed":
            return existing
        self.rows[key] = payload.copy()
        return self.rows[key]


class RouteStore:
    def list_active_github_poll_targets(self, *, limit: int) -> list[dict[str, Any]]:
        assert limit == 1000
        return [TARGET.copy()]


def _context(store: FakeStore) -> EventContext[FakeStore]:
    return EventContext(
        event_id="event-1",
        subject="git.webhook.received",
        correlation_id="incident-1",
        causation_id=None,
        db=store,
        workspace_id="default",
    )


def _body() -> GitWebhookReceivedBody:
    return GitWebhookReceivedBody(
        commit_sha=COMMIT_SHA,
        image="webhook.example/wrong:v9",
        replicas=9,
        workspace_id="default",
        repository_id="repository-1",
        repo_ref="owner/config",
        branch="main",
        watch_target_id="watch-1",
        binding_id="binding-1",
        application_id="app-1",
        environment="sandbox",
        cluster_id="cluster-1",
        manifest_path="deploy/game.yaml",
        source_type="raw-yaml",
    )


def _github_response(content: bytes) -> dict[str, Any]:
    return {
        "type": "file",
        "size": len(content),
        "encoding": "base64",
        "content": base64.b64encode(content).decode(),
    }


def test_controller_subscribes_to_git_webhook_received() -> None:
    runtime = ControllerRuntime(ROOT)

    runtime.check_report()
    scm = next(item for item in runtime.loaded if item.service.name == "scm-worker")

    assert "git.webhook.received" in scm.module.app.handler_spec().subjects


def test_push_uses_commit_file_instead_of_webhook_or_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITOPS_WEBHOOK_IMAGE", "environment.example/wrong:v8")
    source = b"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: game
  namespace: games
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: game
        image: registry.example/game:v2
"""
    requests: list[str] = []

    def github(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        assert request.url.params["ref"] == COMMIT_SHA
        assert request.url.path == "/repos/owner/config/contents/deploy/game.yaml"
        return httpx.Response(200, json=_github_response(source))

    transport = httpx.MockTransport(github)
    original = source_observation.GitHubRepositoryClient
    monkeypatch.setattr(source_observation, "github_token", lambda: "")
    monkeypatch.setattr(
        source_observation,
        "GitHubRepositoryClient",
        lambda *, token: original(token=token, transport=transport),
    )
    store = FakeStore()
    runtime = ControllerRuntime(ROOT)
    scm = next(item for item in runtime.loaded if item.service.name == "scm-worker")
    subscription = next(
        sub for sub in scm.module.app.subscriptions if sub.subject == "git.webhook.received"
    )
    handler = make_event_handler(subscription, store, "scm-worker")
    received = envelope(
        "git.webhook.received",
        "api-gateway",
        _body().to_body(),
        "incident-1",
        workspace_id="default",
    )

    first = asyncio.run(handler(received))
    repeated = asyncio.run(handler(received))

    assert len(first) == 1 and first[0].payload["image"] == "registry.example/game:v2"
    assert first[0].payload["replicas"] == 3
    assert len(repeated) == 1 and repeated[0].payload == first[0].payload
    assert len(store.rows) == 1
    assert len(requests) == 1


def test_github_permission_error_stays_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(403, json={}))
    original = source_observation.GitHubRepositoryClient
    monkeypatch.setattr(source_observation, "github_token", lambda: "")
    monkeypatch.setattr(
        source_observation,
        "GitHubRepositoryClient",
        lambda *, token: original(token=token, transport=transport),
    )
    store = FakeStore()

    result = asyncio.run(source_observation.observe_git_source(_body(), _context(store)))

    assert result is None
    assert next(iter(store.rows.values()))["status"] == "unavailable"


def test_multiple_deployments_do_not_become_observed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    content = (
        b"apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: game\n---\nkind: Deployment\n"
    )
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json=_github_response(content))
    )
    original = source_observation.GitHubRepositoryClient
    monkeypatch.setattr(source_observation, "github_token", lambda: "")
    monkeypatch.setattr(
        source_observation,
        "GitHubRepositoryClient",
        lambda *, token: original(token=token, transport=transport),
    )
    store = FakeStore()

    result = asyncio.run(source_observation.observe_git_source(_body(), _context(store)))

    assert result is None
    assert next(iter(store.rows.values()))["status"] == "invalid"


@pytest.mark.parametrize(
    "response",
    [
        {"type": "file", "size": 4, "encoding": "base64", "content": "@@@="},
        {"type": "file", "size": 8, "encoding": "base64", "content": "dGVzdA=="},
    ],
)
def test_incomplete_github_file_never_becomes_observed(
    monkeypatch: pytest.MonkeyPatch, response: dict[str, Any]
) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=response))
    original = source_observation.GitHubRepositoryClient
    monkeypatch.setattr(source_observation, "github_token", lambda: "")
    monkeypatch.setattr(
        source_observation,
        "GitHubRepositoryClient",
        lambda *, token: original(token=token, transport=transport),
    )
    store = FakeStore()

    with pytest.raises(RepositoryDiscoveryError):
        asyncio.run(source_observation.observe_git_source(_body(), _context(store)))

    assert not store.rows


def test_mismatched_credential_reference_stays_unavailable() -> None:
    store = FakeStore(target={**TARGET, "credential_ref": "db:github:other-repository"})

    result = asyncio.run(source_observation.observe_git_source(_body(), _context(store)))

    assert result is None
    assert next(iter(store.rows.values()))["status"] == "unavailable"


def test_push_webhook_does_not_require_configured_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GITOPS_WEBHOOK_IMAGE", raising=False)
    payload = {
        "repository": {"full_name": "owner/config"},
        "ref": "refs/heads/main",
        "after": COMMIT_SHA,
    }

    bodies = build_git_webhook_bodies(payload, db=RouteStore(), event_name="push")

    assert len(bodies) == 1
    assert bodies[0].commit_sha == COMMIT_SHA
    assert bodies[0].image == ""


@pytest.mark.skipif(not os.getenv("TEST_DATABASE_URL"), reason="isolated PostgreSQL required")
def test_database_retries_keep_one_immutable_source_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", os.environ["TEST_DATABASE_URL"])
    repository = RepoChangeRepository()
    binding_id = f"binding-{uuid.uuid4().hex}"
    payload = {
        "workspace_id": "default",
        "binding_id": binding_id,
        "repository_id": "repository-1",
        "application_id": "app-1",
        "cluster_id": "cluster-1",
        "namespace": "games",
        "deployment_name": "game",
        "repo_ref": "owner/config",
        "commit_sha": COMMIT_SHA,
        "manifest_path": "deploy/game.yaml",
        "source_type": "raw-yaml",
        "status": "unavailable",
        "reason": "github_403",
    }

    unavailable = repository.record_git_source_observation(
        {**payload, "repo_ref": "owner/previous-config"}
    )
    observed = repository.record_git_source_observation(
        {
            **payload,
            "status": "observed",
            "image": "registry.example/game:v2",
            "replicas": 3,
            "source_sha256": "a" * 64,
            "reason": None,
        }
    )
    repeated = RepoChangeRepository().record_git_source_observation(payload)
    with repository.connection() as connection:
        count = connection.execute(
            select(func.count())
            .select_from(GitSourceObservation.__table__)
            .where(GitSourceObservation.__table__.c.binding_id == binding_id)
        ).scalar_one()

    assert unavailable["status"] == "unavailable"
    assert observed["status"] == repeated["status"] == "observed"
    assert observed["repo_ref"] == "owner/config"
    assert repeated["image"] == "registry.example/game:v2"
    assert count == 1

    with pytest.raises(ValueError, match="same commit"):
        repository.record_git_source_observation(
            {
                **payload,
                "status": "observed",
                "image": "registry.example/game:v3",
                "replicas": 3,
                "source_sha256": "b" * 64,
            }
        )
