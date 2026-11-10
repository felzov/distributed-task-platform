"""Add the provider's idempotency key to fake_notifications (ADR 0007 decision 8).

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-10

Written by hand. Use literal values (not imports from task_platform) so this revision never changes
when the application code does, and keep downgrade() working.

The column is nullable and needs no backfill: rows recorded before this revision keep NULL, and
their duplicates stay as phases 3-6 made them. NULLs are distinct by default, so they do not
conflict with each other.

ALTER TABLE takes an ACCESS EXCLUSIVE lock on fake_notifications (PostgreSQL documentation), held
until this revision commits: reads and writes of the table wait while the column is added and the
unique index is built. Not measured.

The downgrade drops the keys: the provider then records every call again.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Given by the caller of the fake provider: every call with one key is one notification.
    op.add_column("fake_notifications", sa.Column("idempotency_key", sa.Text(), nullable=True))
    op.create_unique_constraint(
        "uq_fake_notifications_idempotency_key", "fake_notifications", ["idempotency_key"]
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_fake_notifications_idempotency_key", "fake_notifications", type_="unique"
    )
    op.drop_column("fake_notifications", "idempotency_key")
