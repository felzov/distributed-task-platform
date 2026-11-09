"""`submit_job`'s own decisions about a key, with the database query stubbed or never reached."""

import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import Settings
from task_platform.data.db import create_db_engine
from task_platform.data.jobs import KeyedJob
from task_platform.domain import payloads
from task_platform.domain.idempotency import request_fingerprint
from task_platform.domain.payloads import NOTIFICATION_SEND, PayloadModel
from task_platform.services import jobs
from task_platform.services.jobs import submit_job
from tests.settings import UNREACHABLE_DATABASE_URL, make_test_settings

SENTINEL = "s3cr3t-sentinel-7731"
KEY = f"key-{SENTINEL}"
PAYLOAD = {"recipient": "max@example.com", "subject": "Hi", "body": "Hello"}


@pytest.fixture
def settings() -> Settings:
    return make_test_settings(UNREACHABLE_DATABASE_URL)


@pytest.fixture
async def session_factory(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    # The database is unreachable: a session that only begins and commits never connects, so any
    # real query fails the test.
    engine = create_db_engine(settings)
    try:
        yield async_sessionmaker(engine)
    finally:
        await engine.dispose()


def stub_keyed_insert(
    monkeypatch: pytest.MonkeyPatch, answer: KeyedJob | None = None
) -> list[dict[str, Any]]:
    """Replace the query; returns the keyword arguments of every call."""
    calls: list[dict[str, Any]] = []

    async def insert(session: AsyncSession, **kwargs: Any) -> KeyedJob:
        calls.append(kwargs)
        return answer or KeyedJob(
            uuid.uuid4(), "queued", created=True, request_hash=kwargs["request_hash"]
        )

    monkeypatch.setattr(jobs, "insert_job_or_find_by_key", insert)
    return calls


async def submit(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    payload: dict[str, Any] = PAYLOAD,
    key: str = KEY,
) -> jobs.SubmittedJob:
    return await submit_job(
        session_factory,
        settings,
        job_type=NOTIFICATION_SEND,
        payload_version=1,
        payload=payload,
        idempotency_key=key,
    )


# Normal path


class WithDefault(PayloadModel):
    recipient: str
    subject: str
    body: str
    # A field a later version of a payload model might add.
    priority: str = "normal"


async def test_the_fingerprint_covers_the_request_as_sent_not_the_stored_model(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    # ADR 0007 decision 3: a default field added to a model must not change the fingerprint of a
    # request that does not send it, or retries of keys saved before the change would get 422.
    monkeypatch.setattr(payloads, "PAYLOAD_MODELS", {(NOTIFICATION_SEND, 1): WithDefault})
    calls = stub_keyed_insert(monkeypatch)

    await submit(session_factory, settings)

    [call] = calls
    assert call["payload"] == PAYLOAD | {"priority": "normal"}
    assert call["request_hash"] == request_fingerprint(NOTIFICATION_SEND, 1, PAYLOAD)


# Invalid input


@pytest.mark.parametrize("key", ["", f"{SENTINEL} x", "x" * 256])
async def test_an_invalid_key_is_a_caller_bug_raised_before_the_database(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings, key: str
) -> None:
    # The API checks the key first; the database checks only its length, so the service checks
    # the characters again rather than store a key the API would refuse.
    with pytest.raises(ValueError, match="idempotency key is not valid") as exc_info:
        await submit(session_factory, settings, key=key)
    assert SENTINEL not in str(exc_info.value)


# Failure path: a keyed row without its fingerprint


async def test_a_keyed_row_without_a_hash_is_a_server_error_not_a_reused_key(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # ck_jobs_idempotency_key_has_hash forbids such a row, so only a broken database returns it.
    job_id = uuid.uuid4()
    stub_keyed_insert(monkeypatch, KeyedJob(job_id, "queued", created=False, request_hash=None))

    with pytest.raises(RuntimeError), caplog.at_level(logging.ERROR):
        await submit(session_factory, settings)

    assert str(job_id) in caplog.text
    assert SENTINEL not in caplog.text
