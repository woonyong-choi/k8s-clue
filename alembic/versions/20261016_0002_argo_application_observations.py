"""Store bounded Argo CD Application status transitions.

Revision ID: 20261016_0002
Revises: 20261016_0001
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261016_0002"
down_revision: str | None = "20261016_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "argo_application_observations",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("workspace_id", sa.Text(), nullable=False),
        sa.Column("binding_id", sa.Text(), nullable=False),
        sa.Column("correlation_id", sa.Text(), nullable=False),
        sa.Column("expected_sha", sa.Text(), nullable=False),
        sa.Column("application", sa.Text(), nullable=False),
        sa.Column("project", sa.Text(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("sync_revision", sa.Text(), nullable=True),
        sa.Column("operation_revision", sa.Text(), nullable=True),
        sa.Column("sync_status", sa.Text(), nullable=True),
        sa.Column("operation_phase", sa.Text(), nullable=True),
        sa.Column("health_status", sa.Text(), nullable=True),
        sa.Column("condition_types", sa.Text(), nullable=True),
        sa.Column("reconciled_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "first_checked_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "last_checked_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("request_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("response_bytes", sa.BigInteger(), server_default="0", nullable=False),
    )
    op.create_index(
        "ix_argo_application_observations_incident",
        "argo_application_observations",
        ["workspace_id", "correlation_id", "binding_id", "id"],
    )


def downgrade() -> None:
    op.drop_table("argo_application_observations")
