"""Add lease, error and result columns to jobs, and the fake_notifications table.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-31

Written by hand. Use literal values (not imports from task_platform) so this revision never changes
when the application code does, and keep downgrade() working.

All new jobs columns are nullable and need no backfill: rows from 0001 are `queued`, so the
running-lease CHECK holds for them.

The downgrade drops the leases. A database that has `running` jobs by then cannot be upgraded
again until those rows get a lease or a different status: adding the CHECK validates every
existing row, and one violation fails the whole upgrade.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Set by the claim; recovery of expired leases comes in phase 8 (ADR 0001 decision 8).
    op.add_column("jobs", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    # A safe error category, never an exception message (which could contain payload data).
    op.add_column("jobs", sa.Column("error_code", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("result", postgresql.JSONB(), nullable=True))
    # Length only, not tied to status: a retried job keeps its last error while queued (phase 5).
    op.create_check_constraint(
        "ck_jobs_error_code_length", "jobs", "char_length(error_code) BETWEEN 1 AND 100"
    )
    # A running job always has a lease, so recovery can tell when it is stuck.
    op.create_check_constraint(
        "ck_jobs_running_has_lease", "jobs", "status <> 'running' OR lease_expires_at IS NOT NULL"
    )

    # What the fake provider "sent" (ADR 0004): one row per call, so duplicates are visible.
    # No unique key on purpose; phase 7 adds the idempotency guard. No copy of the payload.
    op.create_table(
        "fake_notifications",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        # The job attempt that made the call.
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_fake_notifications"),
        # Jobs are never deleted, so there is no ON DELETE rule.
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], name="fk_fake_notifications_job_id"),
        sa.CheckConstraint("attempt >= 1", name="ck_fake_notifications_attempt_positive"),
    )


def downgrade() -> None:
    op.drop_table("fake_notifications")
    op.drop_constraint("ck_jobs_running_has_lease", "jobs", type_="check")
    op.drop_constraint("ck_jobs_error_code_length", "jobs", type_="check")
    op.drop_column("jobs", "result")
    op.drop_column("jobs", "error_code")
    op.drop_column("jobs", "lease_expires_at")
