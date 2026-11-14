"""Index the jobs the two recovery sweeps look for (ADR 0008 decision 4).

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-11

Written by hand. Use literal values (not imports from task_platform) so this revision never changes
when the application code does, and keep downgrade() working.

Both indexes are partial, so they hold only the jobs a sweep can find: running jobs (one per busy
worker, by lease expiry) and queued jobs already published (by dispatch time). The sweeps write
the same status literals into their SQL, so a generic plan can still use the indexes (phase 6).
Built with a plain CREATE INDEX inside Alembic's transaction, which blocks writes to `jobs` while
it runs (CONCURRENTLY cannot run in a transaction); short for the MVP's table sizes, but not
measured. No data changes: the downgrade drops the indexes only.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Lease recovery: running jobs whose lease_expires_at has passed, earliest expiry first.
    op.create_index(
        "ix_jobs_lease_expiry",
        "jobs",
        ["lease_expires_at"],
        postgresql_where=sa.text("status = 'running'"),
    )
    # Lost-message recovery: queued jobs published longer ago than a threshold. A queued job not
    # yet published is the dispatcher's, in ix_jobs_dispatch_due.
    op.create_index(
        "ix_jobs_dispatched",
        "jobs",
        ["dispatched_at"],
        postgresql_where=sa.text("status = 'queued' AND dispatched_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_dispatched", table_name="jobs")
    op.drop_index("ix_jobs_lease_expiry", table_name="jobs")
