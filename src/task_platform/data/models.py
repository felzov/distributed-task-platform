"""ORM mapping of the tables created by the hand-written migrations in `migrations/versions/`.

The migrations own the schema: constraints, defaults and indexes are defined only there. This
module describes columns and nullability so that queries are typed; a test checks that both agree.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, FetchedValue, LargeBinary, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"

    # FetchedValue: PostgreSQL generates the id (gen_random_uuid() in migration 0001); without
    # this marker SQLAlchemy warns about a primary key that nothing fills in.
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, server_default=FetchedValue())
    type: Mapped[str] = mapped_column(Text)
    payload_version: Mapped[int]
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    # Plain text here; the services layer converts it to JobStatus.
    status: Mapped[str] = mapped_column(Text)
    attempts: Mapped[int]
    max_attempts: Mapped[int]
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # Added by migration 0002.
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_code: Mapped[str | None] = mapped_column(Text)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    # Added by migration 0005 (ADR 0007): both set or both NULL.
    idempotency_key: Mapped[str | None] = mapped_column(Text)
    request_hash: Mapped[bytes | None] = mapped_column(LargeBinary)


class JobAttempt(Base):
    """One attempt of a job, opened by its claim and closed by its finish (ADR 0005 decision 5)."""

    __tablename__ = "job_attempts"

    # The foreign key to jobs and the CHECKs live in migration 0003, like every other constraint.
    job_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    attempt: Mapped[int] = mapped_column(primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(Text)
    error_code: Mapped[str | None] = mapped_column(Text)


class FakeNotification(Base):
    """One notification of the fake provider (ADR 0004, ADR 0007 decision 8).

    Calls with one idempotency key share one row. Rows recorded before migration 0006 have no key,
    one per call, duplicates included.
    """

    __tablename__ = "fake_notifications"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, server_default=FetchedValue())
    # The foreign key lives in migration 0002, like every other constraint.
    job_id: Mapped[uuid.UUID]
    # The attempt that made the call; with a key, the first call with it.
    attempt: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    idempotency_key: Mapped[str | None] = mapped_column(Text)
