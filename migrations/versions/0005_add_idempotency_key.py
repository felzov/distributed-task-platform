"""Add the submission idempotency key and request fingerprint to jobs (ADR 0007).

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-09

Written by hand. Use literal values (not imports from task_platform) so this revision never changes
when the application code does, and keep downgrade() working.

Both columns are nullable and need no backfill: existing jobs were saved without a key. A key never
expires (ADR 0007 decision 4), so the UNIQUE constraint covers every job ever saved with one.

ALTER TABLE takes an ACCESS EXCLUSIVE lock on jobs (PostgreSQL documentation), held until this
revision commits: reads and writes of jobs wait while the columns are added, the unique index is
built and the CHECKs scan the table. Not measured.

The downgrade drops the keys and fingerprints: a client retrying with a key after it gets a second
job.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Sent by the client in the Idempotency-Key header. Its characters are checked only in
    # domain/idempotency.py; its length there and by the CHECK below.
    op.add_column("jobs", sa.Column("idempotency_key", sa.Text(), nullable=True))
    # SHA-256 of the request as the API parsed it, to tell a retry from a reused key.
    op.add_column("jobs", sa.Column("request_hash", sa.LargeBinary(), nullable=True))
    # NULLs are distinct by default, so any number of jobs can be saved without a key.
    op.create_unique_constraint("uq_jobs_idempotency_key", "jobs", ["idempotency_key"])
    # A key is useless without the fingerprint that tells a retry from a reuse, and the reverse.
    op.create_check_constraint(
        "ck_jobs_idempotency_key_has_hash",
        "jobs",
        "(idempotency_key IS NULL) = (request_hash IS NULL)",
    )
    op.create_check_constraint(
        "ck_jobs_idempotency_key_length", "jobs", "char_length(idempotency_key) BETWEEN 1 AND 255"
    )
    op.create_check_constraint(
        "ck_jobs_request_hash_length", "jobs", "octet_length(request_hash) = 32"
    )


def downgrade() -> None:
    op.drop_constraint("ck_jobs_request_hash_length", "jobs", type_="check")
    op.drop_constraint("ck_jobs_idempotency_key_length", "jobs", type_="check")
    op.drop_constraint("ck_jobs_idempotency_key_has_hash", "jobs", type_="check")
    op.drop_constraint("uq_jobs_idempotency_key", "jobs", type_="unique")
    op.drop_column("jobs", "request_hash")
    op.drop_column("jobs", "idempotency_key")
