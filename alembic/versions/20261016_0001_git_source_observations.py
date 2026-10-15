"""Store commit-pinned Git source observations independently from rendered artifacts.

Revision ID: 20261016_0001
Revises: 20260725_0400
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261016_0001"
down_revision: str | None = "20260725_0400"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "git_source_observations",
        sa.Column("workspace_id", sa.Text(), nullable=False),
        sa.Column("binding_id", sa.Text(), nullable=False),
        sa.Column("repository_id", sa.Text(), nullable=False),
        sa.Column("application_id", sa.Text(), nullable=False),
        sa.Column("cluster_id", sa.Text(), nullable=False),
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("deployment_name", sa.Text(), nullable=False),
        sa.Column("repo_ref", sa.Text(), nullable=False),
        sa.Column("commit_sha", sa.Text(), nullable=False),
        sa.Column("manifest_path", sa.Text(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("image", sa.Text(), nullable=True),
        sa.Column("replicas", sa.BigInteger(), nullable=True),
        sa.Column("source_sha256", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "observed_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("workspace_id", "binding_id", "commit_sha", "manifest_path"),
    )


def downgrade() -> None:
    op.drop_table("git_source_observations")
