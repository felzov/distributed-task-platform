"""Create the jobs table.

Revision ID: 0001
Revises:
Create Date: 2026-10-25

Written by hand. Use literal values (not imports from task_platform) so this revision never changes
when the application code does, and keep downgrade() working.

Lease, error and result columns are added in phase 3, when code first writes them.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("payload_version", sa.Integer(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'queued'"), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        # No default: the API always sets it from configuration.
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        # Timestamps come from PostgreSQL's clock, never the app's (ADR 0001 decision 9).
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # NULL until the dispatcher's publish is confirmed (ADR 0002).
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # Set explicitly by every UPDATE from phase 3 on; there is no trigger.
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
        # Known types are checked by the API, so adding a type needs no migration.
        sa.CheckConstraint("char_length(type) BETWEEN 1 AND 100", name="ck_jobs_type_length"),
        sa.CheckConstraint("payload_version >= 1", name="ck_jobs_payload_version_positive"),
        # The four statuses of ADR 0001; text + CHECK instead of an ENUM.
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed')", name="ck_jobs_status"
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_jobs_attempts_non_negative"),
        # The upper bound is a placeholder cap, not a measured value.
        sa.CheckConstraint("max_attempts BETWEEN 1 AND 20", name="ck_jobs_max_attempts_range"),
        # Attempts are counted at claim time and never exceed the budget (ADR 0001).
        sa.CheckConstraint("attempts <= max_attempts", name="ck_jobs_attempts_within_max"),
    )
    # Matches the dispatcher's poll for due, unpublished jobs (ADR 0002).
    op.create_index(
        "ix_jobs_dispatch_due",
        "jobs",
        ["available_at"],
        postgresql_where=sa.text("status = 'queued' AND dispatched_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_dispatch_due", table_name="jobs")
    op.drop_table("jobs")
