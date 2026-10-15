"""Keep image-pull verification windows under the original incident.

Revision ID: 20261016_0003
Revises: 20261016_0002
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261016_0003"
down_revision: str | None = "20261016_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "image_pull_observations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("workspace_id", sa.Text(), nullable=False),
        sa.Column("correlation_id", sa.Text(), nullable=False),
        sa.Column("binding_id", sa.Text(), nullable=False),
        sa.Column("expected_sha", sa.Text(), nullable=False),
        sa.Column("expected_image", sa.Text(), nullable=False),
        sa.Column("evidence_key", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("deployment_uid", sa.Text(), nullable=True),
        sa.Column("replicaset_uid", sa.Text(), nullable=True),
        sa.Column("ready_pod_count", sa.Integer(), nullable=False),
        sa.Column("consecutive_healthy_windows", sa.Integer(), nullable=False),
        sa.Column("confirmed_recovered", sa.Boolean(), nullable=False),
        sa.Column("observed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "correlation_id",
            "binding_id",
            "expected_sha",
            "evidence_key",
            name="uq_image_pull_observation_window",
        ),
    )
    op.create_index(
        "ix_image_pull_observation_incident",
        "image_pull_observations",
        ["workspace_id", "correlation_id", "binding_id", "observed_at", "id"],
    )


def downgrade() -> None:
    op.drop_table("image_pull_observations")
