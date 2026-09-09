from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from conftest import ROOT
from sqlalchemy import UniqueConstraint

from domains.gitops.source_patch import (
    ManifestScalarPatchPlan,
    ManifestSourcePatchError,
    ScalarFieldReplacement,
    canonical_manifest_digest,
    materialize_scalar_patch,
    scalar_patch_content,
)
from domains.rca.models import IncidentSignalClaim
from domains.scm.events import SafePrFilePatch, SafePrRequestedBody
from domains.scm.policy import (
    DefaultSafePrPreflightPolicy,
    SafePrPolicyResult,
    safe_pr_failed_body,
)
from packages.events.envelope import event
from packages.runtime.dispatch import _collect


def source(path: str) -> str:
    return Path(ROOT, path).read_text()


def request(*, delivery: str = "pull_request") -> SafePrRequestedBody:
    return SafePrRequestedBody(
        title="ImagePullBackOff recovery",
        body="Review the exact image tag correction.",
        provider="github",
        patches=[
            SafePrFilePatch(
                path=".gitops/safe-pr/patches/recovery.yaml",
                content="kind: GitOpsScalarPatch\n",
            )
        ],
        workflow_run_id="workflow-1",
        manifest_path="deploy/api.yaml",
        repo_ref="org/repo",
        base_branch="main",
        commit_sha="a" * 40,
        evidence_ref="object://evidence/correlation-1.json",
        delivery=delivery,
    )


def test_agent_kubernetes_surface_is_read_only() -> None:
    provider = source(
        "src/services/target/cluster-agent/providers/kubernetes_providers.py"
    )
    rbac = source("charts/clue/templates/agent-rbac.yaml")

    assert not any(
        token in provider
        for token in ("client.post(", "client.put(", "client.patch(", "client.delete(")
    )
    assert 'verbs: ["get", "list", "watch"]' in rbac
    assert not any(
        token in rbac
        for token in ("pods/exec", "pods/attach", "pods/portforward", "nodes/proxy")
    )


def test_worker_child_event_inherits_correlation_and_causation() -> None:
    parent = event(
        "cluster.evidence.received",
        "api-gateway",
        {"workspace_id": "workspace-1"},
        correlation_id="correlation-1",
    )

    async def result() -> SafePrRequestedBody:
        return request()

    child = asyncio.run(_collect("safe-pr-worker", parent, result()))[0]

    assert child.correlation_id == "correlation-1"
    assert child.causation_id == parent.event_id


def test_incident_claim_has_a_durable_unique_identity() -> None:
    constraint = next(
        value
        for value in IncidentSignalClaim.__table__.constraints
        if isinstance(value, UniqueConstraint)
        and value.name == "uq_incident_signal_claim_identity"
    )

    assert tuple(column.name for column in constraint.columns) == (
        "workspace_id",
        "cluster_id",
        "signal_key",
    )


def test_patch_allowlist_rejects_non_deployment_and_unapproved_field() -> None:
    stateful_set = """\
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: api
spec:
  replicas: 1
"""
    replica = ScalarFieldReplacement("spec.replicas", 1, 2)
    plan = ManifestScalarPatchPlan(
        action_type="replica_scale",
        source_type="raw-yaml",
        source_manifest_sha256=canonical_manifest_digest(
            {
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "api"},
                "spec": {"replicas": 1},
            }
        ),
        expected_base_sha="a" * 40,
        manifest_path="deploy/api.yaml",
        replacements=(replica,),
        rollback_replacements=(
            ScalarFieldReplacement("spec.replicas", 2, 1),
        ),
    )
    with pytest.raises(ManifestSourcePatchError, match="must be a Deployment"):
        materialize_scalar_patch(stateful_set, plan)

    unsafe = ManifestScalarPatchPlan(
        action_type="replica_scale",
        source_type="raw-yaml",
        source_manifest_sha256="sha256:" + "b" * 64,
        expected_base_sha="a" * 40,
        manifest_path="deploy/api.yaml",
        replacements=(
            ScalarFieldReplacement("metadata.labels.owner", "ops", "attacker"),
        ),
        rollback_replacements=(
            ScalarFieldReplacement("metadata.labels.owner", "attacker", "ops"),
        ),
    )
    with pytest.raises(ManifestSourcePatchError):
        scalar_patch_content(unsafe)


def test_safe_pr_rejects_direct_delivery_and_provider_has_no_merge_path() -> None:
    result = DefaultSafePrPreflightPolicy().evaluate(
        request(delivery="direct_commit")
    )
    provider = source("src/services/gitops/scm-worker/github_provider.py")

    assert result.allowed is False
    assert result.reason_code == "unsafe_delivery"
    assert "direct_commit" not in provider
    assert "/merge" not in provider


def test_safe_pr_failure_preserves_original_evidence_reference() -> None:
    failed = safe_pr_failed_body(
        request(),
        SafePrPolicyResult.reject(
            reason_code="provider_error",
            message="GitHub request failed",
        ),
        stage="scm",
    )

    assert failed.evidence_ref == "object://evidence/correlation-1.json"
    assert failed.details["evidence_ref"] == failed.evidence_ref
