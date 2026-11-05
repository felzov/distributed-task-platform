"""Create the job_attempts table: one row per attempt of a job (ADR 0005 decision 5).

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-05

Written by hand. Use literal values (not imports from task_platform) so this revision never changes
when the application code does, and keep downgrade() working.

No backfill (ADR 0005 decision 7): jobs that ran before this revision keep their `attempts` count
but get no rows, since their attempts' start and end times were never stored. The downgrade drops
the table and with it every attempt's history; the jobs themselves are untouched.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "job_attempts",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        # The attempt number the claim counted (jobs.attempts after the claim, ADR 0001 decision 4).
        sa.Column("attempt", sa.Integer(), nullable=False),
        # Set in the claim's transaction, from PostgreSQL's clock (ADR 0001 decision 9).
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # NULL while the attempt runs, and after a crash until recovery closes it (phase 8). No
        # CHECK finished_at >= started_at: the two come from different transactions, and if
        # PostgreSQL's clock stepped back in between, the CHECK would fail the finish's
        # transaction and leave the job running. A negative duration in the history is the lesser
        # harm.
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.Text(), nullable=True),
        # A safe failure category, never an exception message (same rule as jobs.error_code).
        sa.Column("error_code", sa.Text(), nullable=True),
        # The primary key also serves "all attempts of one job", ordered by attempt.
        sa.PrimaryKeyConstraint("job_id", "attempt", name="pk_job_attempts"),
        # Jobs are never deleted, so there is no ON DELETE rule (as for fake_notifications).
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], name="fk_job_attempts_job_id"),
        sa.CheckConstraint("attempt >= 1", name="ck_job_attempts_attempt_positive"),
        sa.CheckConstraint("outcome IN ('succeeded', 'failed')", name="ck_job_attempts_outcome"),
        # An attempt is either open (neither set) or closed (both set).
        sa.CheckConstraint(
            "(finished_at IS NULL) = (outcome IS NULL)", name="ck_job_attempts_finished_has_outcome"
        ),
        # A code exactly on failed attempts. IS NOT DISTINCT FROM, not =, so an open attempt
        # (outcome NULL) counts as "not failed" instead of making the check NULL, which passes.
        sa.CheckConstraint(
            "(outcome IS NOT DISTINCT FROM 'failed') = (error_code IS NOT NULL)",
            name="ck_job_attempts_error_code_on_failure",
        ),
        sa.CheckConstraint(
            "char_length(error_code) BETWEEN 1 AND 100", name="ck_job_attempts_error_code_length"
        ),
    )


def downgrade() -> None:
    op.drop_table("job_attempts")
