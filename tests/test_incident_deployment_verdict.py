"""Issue #25: image-pull recovery uses actual agent summary identities."""

from __future__ import annotations

import asyncio
import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest

from domains.gitops import argo_observation
from domains.rca.deployment_verdict import assess_image_pull_window
from domains.rca.events import ClusterEvidenceReceivedBody
from domains.rca.repository import RcaRepository
from packages.events.envelope import event as envelope
from packages.runtime.controller import ControllerRuntime
from packages.runtime.dispatch import make_event_handler

IMAGE = "registry.example/game:v2"
SHA = "a" * 40
ROOT = Path(__file__).resolve().parents[1]


def _kubernetes() -> dict[str, Any]:
    return {
        "cluster": {"cluster_id": "cluster-1", "namespace": "games"},
        "collection_coverage": [{"collection": "pods", "namespace": "games", "complete": True}],
        "workloads": [
            {
                "kind": "Deployment",
                "namespace": "games",
                "name": "game",
                "uid": "deployment-new",
                "generation": 2,
                "observed_generation": 2,
                "desired_replicas": 1,
                "ready_replicas": 1,
                "updated_replicas": 1,
                "available_replicas": 1,
                "pod_template": {"spec": {"containers": [{"name": "game", "image": IMAGE}]}},
            }
        ],
        "workload_revisions": [
            {
                "kind": "ReplicaSet",
                "namespace": "games",
                "name": "game-123",
                "uid": "rs-new",
                "owner_uid": "deployment-new",
                "template": {"spec": {"containers": [{"name": "game", "image": IMAGE}]}},
            }
        ],
        "pods": [
            {
                "namespace": "games",
                "uid": "pod-new",
                "owner_uid": "rs-new",
                "waiting_reasons": [],
                "containers": [
                    {
                        "name": "game",
                        "declared_image": IMAGE,
                        "image": IMAGE,
                        "ready": True,
                        "state": "running",
                    }
                ],
            }
        ],
    }


def _assess(snapshot: dict[str, Any]) -> str:
    return assess_image_pull_window(
        snapshot, namespace="games", deployment_name="game", expected_image=IMAGE
    ).status


def test_image_pull_window_requires_target_replicaset_pod_to_run_expected_image() -> None:
    assert _assess(_kubernetes()) == "healthy"


def test_image_pull_window_accepts_normalized_runtime_image() -> None:
    snapshot = _kubernetes()
    snapshot["pods"][0]["containers"][0]["image"] = "registry.example/game@sha256:" + "b" * 64

    assert _assess(snapshot) == "healthy"


def test_image_pull_window_without_declared_pod_image_is_unknown() -> None:
    snapshot = _kubernetes()
    snapshot["pods"][0]["containers"][0].pop("declared_image")

    assert _assess(snapshot) == "unknown"


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (lambda data: data["pods"][0].update(waiting_reasons=["ImagePullBackOff"]), "failing"),
        (lambda data: data["pods"][0].update(owner_uid="old-rs"), "unknown"),
        (lambda data: data["workloads"][0].update(observed_generation=1), "failing"),
        (lambda data: data["pods"][0]["containers"][0].update(ready=False), "failing"),
        (
            lambda data: data["workloads"][0]["pod_template"]["spec"]["containers"][0].update(
                image="registry.example/game:v1"
            ),
            "failing",
        ),
        (
            lambda data: data.update(
                collection_limits={"truncated": True, "lists": {"pods": {"truncated": True}}}
            ),
            "unknown",
        ),
        (lambda data: data["collection_coverage"][0].update(complete=False), "unknown"),
        (lambda data: data["pods"].append("malformed"), "unknown"),
    ],
)
def test_image_pull_window_does_not_infer_recovery_from_missing_or_failing_evidence(
    change: Any, expected: str
) -> None:
    snapshot = deepcopy(_kubernetes())
    change(snapshot)

    assert _assess(snapshot) == expected


@pytest.mark.skipif(not os.getenv("TEST_DATABASE_URL"), reason="isolated PostgreSQL required")
def test_image_pull_windows_need_two_distinct_ordered_healthy_cuts_after_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", os.environ["TEST_DATABASE_URL"])
    correlation_id = f"incident-{uuid4().hex}"
    first_at = datetime.now(UTC)
    payload = {
        "workspace_id": "default",
        "correlation_id": correlation_id,
        "binding_id": "binding-1",
        "expected_sha": "a" * 40,
        "expected_image": IMAGE,
        "evidence_key": "window-1",
        "status": "healthy",
        "reason": "target_image_ready",
        "deployment_uid": "deployment-new",
        "replicaset_uid": "rs-new",
        "ready_pod_count": 1,
        "observed_at": first_at,
    }

    first = RcaRepository().record_image_pull_observation(payload)
    replay = RcaRepository().record_image_pull_observation(payload)
    second = RcaRepository().record_image_pull_observation(
        {**payload, "evidence_key": "window-2", "observed_at": first_at + timedelta(seconds=30)}
    )
    late = RcaRepository().record_image_pull_observation(
        {**payload, "evidence_key": "window-late", "observed_at": first_at - timedelta(seconds=30)}
    )
    latest = RcaRepository().list_image_pull_observations("default", correlation_id, "binding-1")

    assert first["confirmed_recovered"] is False
    assert replay["id"] == first["id"]
    assert second["confirmed_recovered"] is True
    assert late["status"] == "unknown"
    assert latest[0]["evidence_key"] == "window-2"
    assert len(latest) == 3


@pytest.mark.skipif(not os.getenv("TEST_DATABASE_URL"), reason="isolated PostgreSQL required")
def test_image_pull_failure_after_recovery_resets_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", os.environ["TEST_DATABASE_URL"])
    correlation_id = f"incident-{uuid4().hex}"
    first_at = datetime.now(UTC)
    payload = {
        "workspace_id": "default",
        "correlation_id": correlation_id,
        "binding_id": "binding-1",
        "expected_sha": "a" * 40,
        "expected_image": IMAGE,
        "status": "healthy",
        "reason": "target_image_ready",
        "deployment_uid": "deployment-new",
        "replicaset_uid": "rs-new",
        "ready_pod_count": 1,
    }
    repository = RcaRepository()
    repository.record_image_pull_observation(
        {**payload, "evidence_key": "window-1", "observed_at": first_at}
    )
    repository.record_image_pull_observation(
        {**payload, "evidence_key": "window-2", "observed_at": first_at + timedelta(seconds=30)}
    )

    failed = repository.record_image_pull_observation(
        {
            **payload,
            "evidence_key": "window-3",
            "observed_at": first_at + timedelta(seconds=60),
            "status": "failing",
            "reason": "image_pull_failure_persists",
            "ready_pod_count": 0,
        }
    )

    assert failed["confirmed_recovered"] is False
    assert failed["consecutive_healthy_windows"] == 0


class _EventStore:
    def __init__(self, merged_at: datetime, *, argo_binding: bool, plan_status: str) -> None:
        self.merged_at = merged_at
        self.argo_binding = argo_binding
        self.argo_rows: list[dict[str, Any]] = []
        self.image_rows: list[dict[str, Any]] = []
        self.plan_status = plan_status

    async def get_evidence_window_payload_for_workspace(
        self, workspace_id: str, evidence_key: str
    ) -> dict[str, Any]:
        return {
            "window_start": (
                self.merged_at + timedelta(seconds=60 if evidence_key == "first" else 90)
            ).isoformat(),
            "kubernetes": _kubernetes(),
        }

    async def list_image_pull_tracking_plans(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
        return [
            {
                "plan_id": "plan-1",
                "incident_id": "incident-1",
                "status": self.plan_status,
                "correlation_id": "incident-1",
                "payload": {
                    "root_cause": "wrong_image_tag",
                    "target": {
                        "cluster_id": "cluster-1",
                        "namespace": "games",
                        "resource_kind": "Deployment",
                        "resource_name": "game",
                    },
                    "lifecycle": {
                        "merge": {
                            "merged_at": self.merged_at.isoformat(),
                            "merge_commit_sha": SHA,
                            "binding_id": "binding-1",
                            "deployment_request": {
                                "repository_id": "repository-1",
                                "repo_ref": "owner/config",
                                "branch": "main",
                                "watch_target_id": "watch-1",
                                "application_id": "app-1",
                                "workflow_run_id": "workflow-1",
                                "environment": "test",
                                "manifest_path": "deploy/game.yaml",
                            },
                        }
                    },
                },
            }
        ]

    async def get_git_source_observation(self, *_: Any) -> dict[str, Any]:
        return {
            "status": "observed",
            "image": IMAGE,
            "cluster_id": "cluster-1",
            "namespace": "games",
            "deployment_name": "game",
        }

    async def list_argo_application_observations(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
        return self.argo_rows

    async def record_argo_application_observation(self, payload: dict[str, Any]) -> None:
        self.argo_rows[:] = [{**payload, "last_checked_at": payload["checked_at"]}]

    async def record_image_pull_observation(self, payload: dict[str, Any]) -> None:
        self.image_rows.insert(
            0,
            {
                **payload,
                "confirmed_recovered": len(self.image_rows) == 1,
                "consecutive_healthy_windows": len(self.image_rows) + 1,
            },
        )

    async def list_image_pull_observations(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
        return self.image_rows

    async def update_recovery_plan_lifecycle_if_status(self, *_: Any, **__: Any) -> dict[str, Any]:
        self.plan_status = "completed"
        return {"status": "completed"}

    async def list_recovery_verification_plans(self, *_: Any, **__: Any) -> list[Any]:
        if self.plan_status != "verification_pending":
            return []
        return await self.list_image_pull_tracking_plans()

    async def list_active_github_poll_targets(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
        if not self.argo_binding:
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

    async def get_workspace_credential(self, *_: Any) -> dict[str, str]:
        return {
            "workspace_id": "default",
            "provider": "argocd",
            "scope": "binding:binding-1",
            "encrypted_value": "encrypted",
        }


@pytest.mark.parametrize("argo_binding", [True, False])
@pytest.mark.parametrize("plan_status", ["deploy_pending", "verification_pending"])
def test_controller_links_agent_evidence_without_treating_argocd_failure_as_recovery(
    monkeypatch: pytest.MonkeyPatch, argo_binding: bool, plan_status: str
) -> None:
    now = datetime.now(UTC)
    merged_at = now - timedelta(seconds=100)
    reconciled_at = now - timedelta(seconds=60)
    application = {
        "metadata": {"name": "game-prod"},
        "spec": {
            "project": "games",
            "source": {"repoURL": "https://github.com/owner/config.git", "path": "deploy"},
        },
        "status": {
            "sync": {"status": "Synced", "revision": SHA},
            "operationState": {"phase": "Succeeded", "syncResult": {"revision": SHA}},
            "health": {"status": "Healthy"},
            "reconciledAt": reconciled_at.isoformat(),
        },
    }
    original = argo_observation.ArgoApplicationClient
    monkeypatch.setattr(argo_observation, "env", lambda name, default: "https://argo.test")
    monkeypatch.setattr(argo_observation, "decrypt_credential", lambda value: "test-token")
    monkeypatch.setattr(
        argo_observation,
        "ArgoApplicationClient",
        lambda token: original(
            token,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=application)),
        ),
    )
    runtime = ControllerRuntime(ROOT)
    runtime.check_report()
    worker = next(item for item in runtime.loaded if item.service.name == "rca-feedback-worker")
    subscription = next(
        item
        for item in worker.module.app.subscriptions
        if item.subject == "cluster.evidence.received"
    )
    store = _EventStore(merged_at, argo_binding=argo_binding, plan_status=plan_status)
    handler = make_event_handler(subscription, store, "rca-feedback-worker")

    emitted = []
    for evidence_key in ("first", "second"):
        body = ClusterEvidenceReceivedBody(
            cluster_id="cluster-1",
            kubernetes={},
            metrics={},
            logs=[],
            traces={},
            workspace_id="default",
            evidence_key=evidence_key,
        )
        emitted.extend(
            asyncio.run(
                handler(
                    envelope(
                        "cluster.evidence.received",
                        "cluster-agent",
                        body.to_body(),
                        f"agent-{evidence_key}",
                        workspace_id="default",
                    )
                )
            )
        )

    assert len(store.image_rows) == 2
    assert {row["correlation_id"] for row in store.image_rows} == {"incident-1"}
    assert {row["status"] for row in store.image_rows} == {"healthy"}
    assert len(store.argo_rows) == 1
    assert store.argo_rows[0]["correlation_id"] == "incident-1"
    assert store.argo_rows[0]["status"] == ("reported_applied" if argo_binding else "unavailable")
    if argo_binding:
        assert store.plan_status == "completed"
        assert {item.correlation_id for item in emitted} == {"incident-1"}
        assert {item.subject for item in emitted} == {
            "recovery.verification.updated",
            "incident.resolved",
        }
    else:
        assert store.plan_status == plan_status
        assert emitted == []
