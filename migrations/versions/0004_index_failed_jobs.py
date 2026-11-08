"""Index failed jobs, newest first, for `GET /jobs?status=failed` (phase 6).

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-07

Written by hand. Use literal values (not imports from task_platform) so this revision never changes
when the application code does, and keep downgrade() working.

Partial: a job enters the index only when it fails, and `failed` is terminal (ADR 0001), so its
`updated_at` is the time it failed and never changes again. Other writes to `jobs` never touch the
index. Built with a plain CREATE INDEX inside Alembic's transaction, which blocks writes to `jobs`
while it runs (CONCURRENTLY cannot run in a transaction); short for the MVP's table sizes, but
not measured.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Matches the list's ORDER BY updated_at DESC, id DESC and its keyset condition on the same
    # pair; id breaks ties between jobs that failed in the same microsecond.
    op.create_index(
        "ix_jobs_failed",
        "jobs",
        [sa.text("updated_at DESC"), sa.text("id DESC")],
        postgresql_where=sa.text("status = 'failed'"),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_failed", table_name="jobs")
