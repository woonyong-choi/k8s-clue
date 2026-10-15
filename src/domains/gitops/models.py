"""gitops 도메인 테이블."""

from __future__ import annotations

from typing import Any

from sqlalchemy import BigInteger, ForeignKey, Index, PrimaryKeyConstraint, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from packages.storage.base import (
    Base,
    created_at_column,
    jsonb_column,
    text_column,
    updated_at_column,
)


class GitRepository(Base):
    __tablename__ = "git_repositories"
    __table_args__ = (UniqueConstraint("workspace_id", "repo_ref"),)

    repository_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.workspace_id"))
    provider: Mapped[str] = text_column()
    repo_ref: Mapped[str] = text_column()
    default_branch: Mapped[str] = text_column()
    credential_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = text_column()
    access_policy: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class WorkspaceCredential(Base):
    __tablename__ = "workspace_credentials"
    __table_args__ = (UniqueConstraint("workspace_id", "provider", "scope"),)

    credential_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.workspace_id"))
    provider: Mapped[str] = text_column()
    scope: Mapped[str] = text_column()
    encrypted_value: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, nullable=False)
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class GitWatchTarget(Base):
    __tablename__ = "git_watch_targets"
    __table_args__ = (UniqueConstraint("workspace_id", "repository_id", "branch", "manifest_path"),)

    watch_target_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.workspace_id"))
    repository_id: Mapped[str] = text_column()
    branch: Mapped[str] = text_column()
    manifest_path: Mapped[str] = text_column()
    interval_seconds: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_seen_commit_sha: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_polled_at: Mapped[Any | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    status: Mapped[str] = text_column()
    settings: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class DeploymentBinding(Base):
    __tablename__ = "deployment_bindings"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "repository_id",
            "cluster_id",
            "namespace",
            "app_name",
        ),
    )

    binding_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.workspace_id"))
    repository_id: Mapped[str] = text_column()
    watch_target_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    cluster_id: Mapped[str] = text_column()
    namespace: Mapped[str] = text_column()
    app_name: Mapped[str] = text_column()
    manifest_path: Mapped[str] = text_column()
    environment: Mapped[str] = text_column()
    resource_class: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    deploy_policy: Mapped[dict[str, Any]] = jsonb_column()
    access_policy: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class Application(Base):
    __tablename__ = "applications"
    __table_args__ = (UniqueConstraint("workspace_id", "repository_id", "name"),)

    application_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.workspace_id"))
    repository_id: Mapped[str] = text_column()
    name: Mapped[str] = text_column()
    manifest_path: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, nullable=False)
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    workflow_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.workspace_id"))
    application_id: Mapped[str] = text_column()
    binding_id: Mapped[str] = text_column()
    environment: Mapped[str] = text_column()
    cluster_id: Mapped[str] = text_column()
    commit_sha: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    current_step: Mapped[str] = text_column()
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    command_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, nullable=False)
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class ApprovedResourceSnapshot(Base):
    """Last successfully applied Git intent for one binding/resource identity."""

    __tablename__ = "gitops_approved_resource_snapshots"
    __table_args__ = (
        PrimaryKeyConstraint(
            "workspace_id",
            "binding_id",
            "cluster_id",
            "namespace",
            "resource_kind",
            "resource_name",
        ),
        Index(
            "ix_gitops_approved_snapshots_workflow",
            "workspace_id",
            "workflow_run_id",
            "command_id",
        ),
    )

    workspace_id: Mapped[str] = text_column()
    binding_id: Mapped[str] = text_column()
    cluster_id: Mapped[str] = text_column()
    namespace: Mapped[str] = text_column()
    resource_kind: Mapped[str] = text_column()
    resource_name: Mapped[str] = text_column()
    workflow_run_id: Mapped[str] = text_column()
    command_id: Mapped[str] = text_column()
    commit_sha: Mapped[str] = text_column()
    artifact_digest: Mapped[str] = text_column()
    managed_fields: Mapped[dict[str, Any]] = jsonb_column()
    snapshot: Mapped[dict[str, Any]] = jsonb_column()
    completed_at: Mapped[Any] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class WorkflowRunStep(Base):
    __tablename__ = "workflow_run_steps"
    __table_args__ = (UniqueConstraint("workflow_run_id", "name"),)

    step_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workflow_run_id: Mapped[str] = text_column()
    workspace_id: Mapped[str] = text_column()
    application_id: Mapped[str] = text_column()
    binding_id: Mapped[str] = text_column()
    environment: Mapped[str] = text_column()
    name: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    details: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class Approval(Base):
    __tablename__ = "approvals"

    approval_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workflow_run_id: Mapped[str] = text_column()
    workspace_id: Mapped[str] = text_column()
    application_id: Mapped[str] = text_column()
    binding_id: Mapped[str] = text_column()
    environment: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    reason: Mapped[str] = text_column()
    requested_role: Mapped[str] = text_column()
    requested_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    decision: Mapped[str | None] = mapped_column(Text, nullable=True)
    details: Mapped[dict[str, Any]] = jsonb_column()
    expires_at: Mapped[Any | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class ManifestArtifact(Base):
    __tablename__ = "manifest_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "binding_id",
            "commit_sha",
            "manifest_path",
            name="ux_manifest_artifacts_workspace_binding_commit_path",
        ),
    )

    artifact_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.workspace_id"))
    repository_id: Mapped[str] = text_column()
    watch_target_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    binding_id: Mapped[str] = text_column()
    commit_sha: Mapped[str] = text_column()
    manifest_path: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    status_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    rendered_manifest: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    source_summary: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class RepoChange(Base):
    __tablename__ = "repo_changes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = text_column()
    correlation_id: Mapped[str] = text_column()
    commit_sha: Mapped[str] = text_column()
    repository_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    watch_target_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    binding_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    manifest_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    manifest: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()


class GitSourceObservation(Base):
    __tablename__ = "git_source_observations"
    __table_args__ = (
        PrimaryKeyConstraint("workspace_id", "binding_id", "commit_sha", "manifest_path"),
    )

    workspace_id: Mapped[str] = text_column()
    binding_id: Mapped[str] = text_column()
    repository_id: Mapped[str] = text_column()
    application_id: Mapped[str] = text_column()
    cluster_id: Mapped[str] = text_column()
    namespace: Mapped[str] = text_column()
    deployment_name: Mapped[str] = text_column()
    repo_ref: Mapped[str] = text_column()
    commit_sha: Mapped[str] = text_column()
    manifest_path: Mapped[str] = text_column()
    source_type: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    image: Mapped[str | None] = mapped_column(Text, nullable=True)
    replicas: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_sha256: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class ArgoApplicationObservation(Base):
    __tablename__ = "argo_application_observations"
    __table_args__ = (
        Index(
            "ix_argo_application_observations_incident",
            "workspace_id",
            "correlation_id",
            "binding_id",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = text_column()
    binding_id: Mapped[str] = text_column()
    correlation_id: Mapped[str] = text_column()
    expected_sha: Mapped[str] = text_column()
    application: Mapped[str] = text_column()
    project: Mapped[str] = text_column()
    fingerprint: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    sync_revision: Mapped[str | None] = mapped_column(Text, nullable=True)
    operation_revision: Mapped[str | None] = mapped_column(Text, nullable=True)
    sync_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    operation_phase: Mapped[str | None] = mapped_column(Text, nullable=True)
    health_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    condition_types: Mapped[str | None] = mapped_column(Text, nullable=True)
    reconciled_at: Mapped[Any | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    first_checked_at: Mapped[Any] = created_at_column()
    last_checked_at: Mapped[Any] = updated_at_column()
    request_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    response_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
