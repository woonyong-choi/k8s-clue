"""rca 도메인 테이블."""

from __future__ import annotations

from typing import Any

from sqlalchemy import TIMESTAMP, BigInteger, Float, Index, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from packages.storage.base import (
    Base,
    created_at_column,
    jsonb_column,
    text_column,
    updated_at_column,
)


class Evidence(Base):
    __tablename__ = "evidence"
    __table_args__ = (
        Index(
            "ix_evidence_workspace_correlation_created",
            "workspace_id",
            "correlation_id",
            "created_at",
            "id",
        ),
        Index("ix_evidence_workspace_created_id", "workspace_id", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = text_column()
    correlation_id: Mapped[str] = text_column()
    kind: Mapped[str] = text_column()
    payload: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()


class IncidentSignalClaim(Base):
    """One durable claim for a concrete workload termination signal.

    Evidence snapshots are polled repeatedly, so the same Kubernetes
    ``lastState.terminated`` remains visible after the container recovers.  This
    ledger makes incident creation idempotent across workers and process
    restarts without discarding the evidence snapshots themselves.
    """

    __tablename__ = "incident_signal_claims"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "cluster_id",
            "signal_key",
            name="uq_incident_signal_claim_identity",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = text_column()
    cluster_id: Mapped[str] = text_column()
    signal_key: Mapped[str] = text_column()
    first_correlation_id: Mapped[str] = text_column()
    payload: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()


class RcaReport(Base):
    __tablename__ = "rca_reports"
    __table_args__ = (
        Index(
            "ix_rca_reports_workspace_correlation_created",
            "workspace_id",
            "correlation_id",
            "created_at",
            "id",
        ),
        Index("ix_rca_reports_workspace_created_id", "workspace_id", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = text_column()
    correlation_id: Mapped[str] = text_column()
    root_cause: Mapped[str] = text_column()
    action: Mapped[str] = text_column()
    incident_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    cluster_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    symptom: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    supporting_evidence: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    missing_evidence: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    resource_kind: Mapped[str | None] = mapped_column(Text, nullable=True)
    resource_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    namespace: Mapped[str | None] = mapped_column(Text, nullable=True)
    secondary_symptoms: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    selected_candidate_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidates: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, nullable=True)
    supporting_evidence_refs: Mapped[list[dict[str, Any]] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    missing_evidence_checks: Mapped[list[dict[str, Any]] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    payload: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()


class RcaBacklogItem(Base):
    __tablename__ = "rca_backlog_items"

    backlog_id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = text_column()
    incident_id: Mapped[str] = text_column()
    symptom: Mapped[str] = text_column()
    title: Mapped[str] = text_column()
    reason: Mapped[str] = text_column()
    evidence_ref: Mapped[str] = text_column()
    missing_evidence: Mapped[dict[str, Any]] = jsonb_column()
    status: Mapped[str] = text_column()
    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class RecoveryPlanRecord(Base):
    __tablename__ = "recovery_plans"
    __table_args__ = (UniqueConstraint("workspace_id", "plan_id"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    plan_id: Mapped[str] = text_column()
    workspace_id: Mapped[str] = text_column()
    correlation_id: Mapped[str] = text_column()
    incident_id: Mapped[str] = text_column()
    evidence_ref: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    selected_action_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload: Mapped[dict[str, Any]] = jsonb_column()
    created_at: Mapped[Any] = created_at_column()
    updated_at: Mapped[Any] = updated_at_column()


class ImagePullObservation(Base):
    __tablename__ = "image_pull_observations"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "correlation_id",
            "binding_id",
            "expected_sha",
            "evidence_key",
            name="uq_image_pull_observation_window",
        ),
        Index(
            "ix_image_pull_observation_incident",
            "workspace_id",
            "correlation_id",
            "binding_id",
            "observed_at",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = text_column()
    correlation_id: Mapped[str] = text_column()
    binding_id: Mapped[str] = text_column()
    expected_sha: Mapped[str] = text_column()
    expected_image: Mapped[str] = text_column()
    evidence_key: Mapped[str] = text_column()
    status: Mapped[str] = text_column()
    reason: Mapped[str] = text_column()
    deployment_uid: Mapped[str | None] = mapped_column(Text, nullable=True)
    replicaset_uid: Mapped[str | None] = mapped_column(Text, nullable=True)
    ready_pod_count: Mapped[int] = mapped_column(Integer, nullable=False)
    consecutive_healthy_windows: Mapped[int] = mapped_column(Integer, nullable=False)
    confirmed_recovered: Mapped[bool] = mapped_column(nullable=False)
    observed_at: Mapped[Any] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[Any] = created_at_column()
