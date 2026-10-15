"""Issue #24: Argo CD reports are bounded evidence, not recovery verdicts."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from domains.gitops import argo_observation, argo_router
from domains.gitops.argo_observation import (
    MAX_APPLICATION_RESPONSE_BYTES,
    ArgoApplicationClient,
    ArgoApplicationTarget,
    ArgoObservationError,
    classify_argo_application,
)
from domains.gitops.events import GitChangedBody
from domains.gitops.models import ArgoApplicationObservation
from domains.gitops.repository import RepoChangeRepository
from domains.identity.dependencies import require_session
from packages.events.envelope import event as envelope
from packages.runtime.controller import ControllerRuntime
from packages.runtime.dependencies import get_db
from packages.runtime.dispatch import EventContext, make_event_handler

SHA = "a" * 40
OTHER_SHA = "b" * 40
TARGET = ArgoApplicationTarget(
    server_url="https://argocd.example.test",
    project="games",
    application="game-prod",
    repo_ref="owner/config",
    manifest_path="deploy/game.yaml",
)
ROOT = Path(__file__).resolve().parents[1]


def _response() -> dict[str, Any]:
    return {
        "metadata": {"name": "game-prod"},
        "spec": {
            "project": "games",
            "source": {
                "repoURL": "https://github.com/owner/config.git",
                "path": "deploy",
            },
        },
        "status": {
            "sync": {"status": "Synced", "revision": SHA},
            "operationState": {
                "phase": "Succeeded",
                "syncResult": {"revision": SHA},
            },
            "health": {"status": "Degraded"},
            "reconciledAt": "2026-10-16T00:00:00Z",
        },
    }


def test_argo_get_keeps_revision_sync_and_health_separate() -> None:
    seen: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_response())

    client = ArgoApplicationClient("test-token", transport=httpx.MockTransport(respond))

    observation = asyncio.run(client.observe(TARGET))

    assert len(seen) == observation.request_count == 1
    assert seen[0].url.path == "/api/v1/applications/game-prod"
    assert seen[0].url.params["project"] == "games"
    assert seen[0].headers["Authorization"] == "Bearer test-token"
    assert observation.sync_revision == observation.operation_revision == SHA
    assert observation.health_status == "Degraded"
    assert observation.response_bytes > 0 and observation.elapsed_ms >= 0
    assert (
        classify_argo_application(
            observation, SHA, now=observation.checked_at, maximum_age_seconds=60
        )
        == "reported_applied"
    )


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (lambda app: app["status"]["sync"].update(revision=OTHER_SHA), "pending"),
        (lambda app: app["status"]["sync"].update(status="OutOfSync"), "pending"),
        (
            lambda app: app["status"]["operationState"]["syncResult"].update(revision=OTHER_SHA),
            "unconfirmed",
        ),
        (lambda app: app["status"]["operationState"].update(phase="Failed"), "unconfirmed"),
        (lambda app: app["status"].pop("reconciledAt"), "unconfirmed"),
        (
            lambda app: app["status"].update(
                conditions=[{"type": "ComparisonError", "message": "secret detail"}]
            ),
            "unconfirmed",
        ),
    ],
)
def test_argo_mismatched_or_incomplete_revision_never_reports_applied(
    change: Any, expected: str
) -> None:
    app = _response()
    change(app)
    client = ArgoApplicationClient(
        "test-token", transport=httpx.MockTransport(lambda request: httpx.Response(200, json=app))
    )

    observation = asyncio.run(client.observe(TARGET))

    assert (
        classify_argo_application(
            observation, SHA, now=observation.checked_at, maximum_age_seconds=60
        )
        == expected
    )


def test_argo_old_observation_stays_stale() -> None:
    client = ArgoApplicationClient(
        "test-token",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=_response())),
    )
    observation = asyncio.run(client.observe(TARGET))

    verdict = classify_argo_application(
        observation,
        SHA,
        now=observation.checked_at + timedelta(seconds=61),
        maximum_age_seconds=60,
    )

    assert verdict == "stale"


@pytest.mark.parametrize(
    ("status", "reason"),
    [(401, "access_denied"), (403, "access_denied"), (404, "application_missing")],
)
def test_argo_http_failure_is_not_application_evidence(status: int, reason: str) -> None:
    client = ArgoApplicationClient(
        "test-token", transport=httpx.MockTransport(lambda request: httpx.Response(status))
    )

    with pytest.raises(ArgoObservationError, match=reason):
        asyncio.run(client.observe(TARGET))


def test_argo_multiple_sources_are_unsupported() -> None:
    app = _response()
    app["spec"]["sources"] = [app["spec"].pop("source")]
    client = ArgoApplicationClient(
        "test-token", transport=httpx.MockTransport(lambda request: httpx.Response(200, json=app))
    )

    with pytest.raises(ArgoObservationError, match="multiple_sources_unsupported"):
        asyncio.run(client.observe(TARGET))


def test_argo_response_has_strict_byte_budget() -> None:
    client = ArgoApplicationClient(
        "test-token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"x" * (MAX_APPLICATION_RESPONSE_BYTES + 1))
        ),
    )

    with pytest.raises(ArgoObservationError, match="response_too_large"):
        asyncio.run(client.observe(TARGET))


def test_argo_observation_requires_https() -> None:
    target = ArgoApplicationTarget(
        server_url="http://argocd.example.test",
        project=TARGET.project,
        application=TARGET.application,
        repo_ref=TARGET.repo_ref,
        manifest_path=TARGET.manifest_path,
    )
    client = ArgoApplicationClient(
        "test-token",
        transport=httpx.MockTransport(lambda request: pytest.fail("must not make a request")),
    )

    with pytest.raises(ArgoObservationError, match="invalid_target"):
        asyncio.run(client.observe(target))


class FakeArgoStore:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    async def list_active_github_poll_targets(
        self, workspace_id: str, *, binding_id: str
    ) -> list[dict[str, Any]]:
        if workspace_id != "default" or binding_id != "binding-1":
            return []
        return [
            {
                "repository_id": "repository-1",
                "repo_ref": "owner/config",
                "application_id": "app-1",
                "cluster_id": "cluster-1",
                "manifest_path": "deploy/game.yaml",
                "deploy_policy": {"argocd": {"project": "games", "application": "game-prod"}},
            }
        ]

    async def get_workspace_credential(
        self, workspace_id: str, provider: str, scope: str
    ) -> dict[str, str]:
        return {
            "workspace_id": workspace_id,
            "provider": provider,
            "scope": scope,
            "encrypted_value": "encrypted",
        }

    async def record_argo_application_observation(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.rows.append(payload)
        return payload


def test_controller_records_git_change_as_argo_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = ControllerRuntime(ROOT)
    runtime.check_report()
    worker = next(item for item in runtime.loaded if item.service.name == "rca-feedback-worker")
    subscription = next(
        item for item in worker.module.app.subscriptions if item.subject == "git.changed"
    )
    application = _response()
    original = argo_observation.ArgoApplicationClient
    monkeypatch.setattr(argo_observation, "env", lambda name, default: TARGET.server_url)
    monkeypatch.setattr(argo_observation, "decrypt_credential", lambda value: "test-token")
    monkeypatch.setattr(
        argo_observation,
        "ArgoApplicationClient",
        lambda token: original(
            token,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=application)),
        ),
    )
    store = FakeArgoStore()
    handler = make_event_handler(subscription, store, "rca-feedback-worker")
    changed = GitChangedBody(
        commit_sha=SHA,
        image="registry.example/game:v2",
        replicas=2,
        workspace_id="default",
        repository_id="repository-1",
        repo_ref="owner/config",
        binding_id="binding-1",
        application_id="app-1",
        cluster_id="cluster-1",
        manifest_path="deploy/game.yaml",
    )

    asyncio.run(
        handler(
            envelope(
                "git.changed", "scm-worker", changed.to_body(), "incident-1", workspace_id="default"
            )
        )
    )

    assert len(store.rows) == 1
    assert store.rows[0]["status"] == "reported_applied"
    assert store.rows[0]["correlation_id"] == "incident-1"
    assert store.rows[0]["response_bytes"] > 0


def test_argo_permission_failure_records_gap_and_call_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = argo_observation.ArgoApplicationClient
    monkeypatch.setattr(argo_observation, "env", lambda name, default: TARGET.server_url)
    monkeypatch.setattr(argo_observation, "decrypt_credential", lambda value: "test-token")
    monkeypatch.setattr(
        argo_observation,
        "ArgoApplicationClient",
        lambda token: original(
            token, transport=httpx.MockTransport(lambda request: httpx.Response(403))
        ),
    )
    store = FakeArgoStore()
    context = EventContext(
        event_id="event-1",
        subject="git.changed",
        correlation_id="incident-1",
        causation_id=None,
        db=store,
        workspace_id="default",
    )
    changed = GitChangedBody(
        commit_sha=SHA,
        image="registry.example/game:v2",
        replicas=2,
        workspace_id="default",
        repository_id="repository-1",
        repo_ref="owner/config",
        binding_id="binding-1",
        application_id="app-1",
        cluster_id="cluster-1",
        manifest_path="deploy/game.yaml",
    )

    asyncio.run(argo_observation.observe_argocd_for_git_change(changed, context))

    assert store.rows[0]["status"] == "unavailable"
    assert store.rows[0]["reason"] == "access_denied"
    assert store.rows[0]["request_count"] == 1
    assert store.rows[0]["response_bytes"] == 0


class FakeConfigStore:
    def __init__(self) -> None:
        self.saved: dict[str, Any] = {}

    def list_active_github_poll_targets(
        self, workspace_id: str, *, binding_id: str
    ) -> list[dict[str, str]]:
        if workspace_id != "default" or binding_id != "binding-1":
            return []
        return [
            {
                "cluster_id": "cluster-1",
                "repo_ref": "owner/config",
                "manifest_path": "deploy/game.yaml",
            }
        ]

    def lock_workspace_credential_scope(self, workspace_id: str, provider: str, scope: str) -> None:
        self.saved["lock"] = (workspace_id, provider, scope)

    def upsert_workspace_credential(self, payload: dict[str, Any]) -> None:
        self.saved["credential"] = payload

    def configure_argocd_binding(
        self, workspace_id: str, binding_id: str, project: str, application: str
    ) -> None:
        self.saved["binding"] = (workspace_id, binding_id, project, application)


def test_argo_connection_checks_application_and_stores_only_encrypted_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeConfigStore()
    app = FastAPI()
    app.include_router(argo_router.router)
    app.dependency_overrides[require_session] = lambda: SimpleNamespace(workspace_id="default")
    app.dependency_overrides[get_db] = lambda: store
    monkeypatch.setattr(argo_router, "resolve_allowed_cluster_ids", lambda *args: {"cluster-1"})
    monkeypatch.setattr(argo_router, "env", lambda name, default: TARGET.server_url)
    monkeypatch.setattr(argo_router, "encrypt_credential", lambda token: "encrypted-token")
    original = argo_router.ArgoApplicationClient
    monkeypatch.setattr(
        argo_router,
        "ArgoApplicationClient",
        lambda token: original(
            token,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=_response())),
        ),
    )

    response = TestClient(app).put(
        "/integrations/argocd/bindings/binding-1",
        json={"project": "games", "application": "game-prod", "token": "private-token"},
    )

    assert response.status_code == 200
    assert response.json()["sync_revision"] == SHA
    assert "private-token" not in response.text
    assert store.saved["credential"]["encrypted_value"] == "encrypted-token"
    assert store.saved["binding"] == ("default", "binding-1", "games", "game-prod")


@pytest.mark.parametrize("upstream_status", [403, 404])
def test_argo_connection_rejects_unavailable_application_without_saving_token(
    monkeypatch: pytest.MonkeyPatch, upstream_status: int
) -> None:
    store = FakeConfigStore()
    app = FastAPI()
    app.include_router(argo_router.router)
    app.dependency_overrides[require_session] = lambda: SimpleNamespace(workspace_id="default")
    app.dependency_overrides[get_db] = lambda: store
    monkeypatch.setattr(argo_router, "resolve_allowed_cluster_ids", lambda *args: {"cluster-1"})
    monkeypatch.setattr(argo_router, "env", lambda name, default: TARGET.server_url)
    original = argo_router.ArgoApplicationClient
    monkeypatch.setattr(
        argo_router,
        "ArgoApplicationClient",
        lambda token: original(
            token,
            transport=httpx.MockTransport(lambda request: httpx.Response(upstream_status)),
        ),
    )

    response = TestClient(app).put(
        "/integrations/argocd/bindings/binding-1",
        json={"project": "games", "application": "game-prod", "token": "private-token"},
    )

    assert response.status_code == 422
    assert "private-token" not in response.text
    assert store.saved == {}


def test_argo_connection_requires_authorized_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeConfigStore()
    app = FastAPI()
    app.include_router(argo_router.router)
    app.dependency_overrides[require_session] = lambda: SimpleNamespace(workspace_id="default")
    app.dependency_overrides[get_db] = lambda: store
    monkeypatch.setattr(argo_router, "resolve_allowed_cluster_ids", lambda *args: {"other"})

    response = TestClient(app).put(
        "/integrations/argocd/bindings/binding-1",
        json={"project": "games", "application": "game-prod", "token": "private-token"},
    )

    assert response.status_code == 404
    assert store.saved == {}


@pytest.mark.skipif(not os.getenv("TEST_DATABASE_URL"), reason="isolated PostgreSQL required")
def test_argo_status_retries_coalesce_without_losing_incident_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", os.environ["TEST_DATABASE_URL"])
    repository = RepoChangeRepository()
    binding_id = f"binding-{uuid.uuid4().hex}"
    checked_at = datetime.now(UTC)
    payload = {
        "workspace_id": "default",
        "binding_id": binding_id,
        "correlation_id": "incident-1",
        "expected_sha": SHA,
        "project": "games",
        "application": "game-prod",
        "fingerprint": "pending",
        "status": "pending",
        "sync_revision": OTHER_SHA,
        "checked_at": checked_at,
        "request_count": 1,
        "response_bytes": 500,
    }

    repository.record_argo_application_observation(payload)
    repeated = RepoChangeRepository().record_argo_application_observation(
        {**payload, "checked_at": checked_at + timedelta(seconds=30)}
    )
    repository.record_argo_application_observation(
        {**payload, "fingerprint": "applied", "status": "reported_applied", "sync_revision": SHA}
    )
    repository.record_argo_application_observation(
        {**payload, "checked_at": checked_at + timedelta(seconds=60)}
    )
    repository.record_argo_application_observation({**payload, "correlation_id": "incident-2"})
    with repository.connection() as connection:
        count = connection.execute(
            select(func.count())
            .select_from(ArgoApplicationObservation.__table__)
            .where(ArgoApplicationObservation.__table__.c.binding_id == binding_id)
        ).scalar_one()
    incident_rows = repository.list_argo_application_observations(
        "default", "incident-1", binding_id
    )

    assert repeated["request_count"] == 2
    assert repeated["response_bytes"] == 1000
    assert count == 4
    assert [row["status"] for row in incident_rows] == [
        "pending",
        "reported_applied",
        "pending",
    ]
