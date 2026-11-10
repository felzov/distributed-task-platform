"""Executing one delivery against the real database: every outcome of ADR 0004 decision 6 that the
service decides (the broker's answer to each is the consumer's, tested with the worker)."""

import asyncio
import contextlib
import json
import logging
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.domain import job_status, retries
from task_platform.domain.errors import InvalidTransitionError
from task_platform.domain.failures import FailureCode
from task_platform.domain.job_status import JobStatus
from task_platform.domain.payloads import NOTIFICATION_SEND, NotificationSendV1, PayloadModel
from task_platform.services.execution import ClaimedJob, Handler, Outcome, execute_job
from task_platform.services.notifications import send_notification
from tests.integration.rows import fake_notification_rows, fake_result, insert_job
from tests.settings import UNREACHABLE_BROKER_URL, make_test_broker_settings

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"
PAYLOAD = {"recipient": f"{SENTINEL}@example.com", "subject": "Hello", "body": "A message."}
HANDLERS: Mapping[str, Handler] = {NOTIFICATION_SEND: send_notification}
# Hours away, so the test machine's clock and PostgreSQL's never disagree about it.
FUTURE = datetime.now(UTC) + timedelta(hours=3)


@pytest.fixture
def settings(test_database_url: str) -> BrokerSettings:
    return make_test_broker_settings(test_database_url, UNREACHABLE_BROKER_URL)


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def insert_due_job(engine: AsyncEngine, **overrides: Any) -> uuid.UUID:
    values: dict[str, Any] = {"payload": json.dumps(PAYLOAD)} | overrides
    job_id: uuid.UUID = (await insert_job(engine, **values))["id"]
    return job_id


async def job_row(engine: AsyncEngine, job_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


async def attempts(engine: AsyncEngine, job_id: uuid.UUID) -> list[dict[str, Any]]:
    async with engine.connect() as connection:
        result = await connection.execute(
            text("SELECT * FROM job_attempts WHERE job_id = :id ORDER BY attempt"), {"id": job_id}
        )
        return [dict(row) for row in result.mappings()]


async def notifications(engine: AsyncEngine, job_id: uuid.UUID) -> list[int]:
    async with engine.connect() as connection:
        rows = await connection.scalars(
            text("SELECT attempt FROM fake_notifications WHERE job_id = :id ORDER BY attempt"),
            {"id": job_id},
        )
        return list(rows)


def assert_no_sentinel(caplog: pytest.LogCaptureFixture) -> None:
    assert SENTINEL not in caplog.text


@pytest.fixture(autouse=True)
def _info_logs(caplog: pytest.LogCaptureFixture) -> None:
    # "No payload in logs at INFO+" (phase acceptance criterion 9) needs INFO captured.
    caplog.set_level(logging.INFO)


# Normal path


async def test_a_due_job_is_claimed_run_and_succeeds(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = await insert_due_job(engine)

    assert await execute_job(sessions, job_id, HANDLERS, settings) == Outcome.SUCCEEDED

    row = await job_row(engine, job_id)
    [notification] = await fake_notification_rows(engine, job_id)
    assert (row["status"], row["attempts"], row["result"]) == (
        "succeeded",
        1,
        fake_result(notification),
    )
    assert row["lease_expires_at"] is None
    assert row["error_code"] is None
    assert (notification["attempt"], notification["idempotency_key"]) == (
        1,
        f"notification.send:{job_id}",
    )
    [attempt] = await attempts(engine, job_id)
    assert (attempt["outcome"], attempt["error_code"]) == ("succeeded", None)
    # Closed in the transaction of the job's final update.
    assert attempt["finished_at"] == row["updated_at"]
    assert_no_sentinel(caplog)


# Deliveries that claim nothing


async def test_an_unknown_job_is_reported_and_nothing_is_written(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = uuid.uuid4()

    assert await execute_job(sessions, job_id, HANDLERS, settings) == Outcome.UNKNOWN_JOB
    assert f"unknown job {job_id}" in caplog.text
    assert await notifications(engine, job_id) == []
    assert await attempts(engine, job_id) == []


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param(
            {"status": "running", "attempts": 1, "lease_expires_at": FUTURE}, id="running"
        ),
        pytest.param({"status": "succeeded", "attempts": 1}, id="succeeded"),
        pytest.param({"status": "failed", "attempts": 1}, id="failed"),
        pytest.param({"available_at": FUTURE}, id="not due yet"),
        pytest.param({"attempts": 5, "max_attempts": 5}, id="no attempts left"),
    ],
)
async def test_a_job_that_cannot_be_claimed_is_left_as_it_is(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    overrides: dict[str, Any],
) -> None:
    job_id = await insert_due_job(engine, **overrides)
    before = await job_row(engine, job_id)

    assert await execute_job(sessions, job_id, HANDLERS, settings) == Outcome.NOT_CLAIMED
    assert await job_row(engine, job_id) == before
    assert await notifications(engine, job_id) == []
    # A lost claim starts no attempt.
    assert await attempts(engine, job_id) == []


async def test_two_deliveries_of_one_job_run_its_handler_once(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> None:
    # Two workers get the same job at once (a duplicate publish, ADR 0002 decision 5).
    job_id = await insert_due_job(engine)

    outcomes = await asyncio.gather(
        execute_job(sessions, job_id, HANDLERS, settings),
        execute_job(sessions, job_id, HANDLERS, settings),
    )

    assert sorted(outcomes) == sorted([Outcome.SUCCEEDED, Outcome.NOT_CLAIMED])
    assert (await job_row(engine, job_id))["attempts"] == 1
    assert await notifications(engine, job_id) == [1]
    assert [row["attempt"] for row in await attempts(engine, job_id)] == [1]


async def test_the_claim_opens_the_attempts_row_before_the_handler_runs(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> None:
    # ADR 0005 decision 5: the row is committed with the claim, so it is there while the handler
    # runs, and a worker that crashes now leaves an open attempt behind.
    job_id = await insert_due_job(engine, attempts=2)
    seen: list[tuple[list[dict[str, Any]], dict[str, Any]]] = []

    async def looks(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        seen.append((await attempts(engine, job.id), await job_row(engine, job.id)))
        return {}

    await execute_job(sessions, job_id, {NOTIFICATION_SEND: looks}, settings)

    [(rows, job)] = seen
    [row] = rows
    # The claim's own attempt number, and the claim's own now().
    assert row["attempt"] == 3
    assert row["started_at"] == job["updated_at"]
    assert (row["finished_at"], row["outcome"], row["error_code"]) == (None, None, None)


async def test_a_second_row_for_the_claimed_attempt_rejects_the_claim(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A row for attempt 1 although the job was never claimed: a bug the claim must not hide. The
    # unique violation rolls the whole claim back, and redelivering would hit it again.
    job_id = await insert_due_job(engine)
    async with engine.begin() as connection:
        await connection.execute(
            text("INSERT INTO job_attempts (job_id, attempt) VALUES (:id, 1)"), {"id": job_id}
        )
    before = await job_row(engine, job_id)
    [existing] = await attempts(engine, job_id)

    assert await execute_job(sessions, job_id, HANDLERS, settings) == Outcome.CLAIM_REJECTED
    assert await job_row(engine, job_id) == before
    assert await attempts(engine, job_id) == [existing]
    assert await notifications(engine, job_id) == []
    # 23505: unique_violation.
    assert f"claim rejected by the database: job {job_id}," in caplog.text
    assert "(sqlstate 23505)" in caplog.text


# Stored payload no longer valid


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        pytest.param({"payload": '{"recipient": "x@example.com"}'}, "invalid_payload", id="schema"),
        pytest.param({"type": "mystery.job"}, "unknown_job_type", id="unknown type"),
        pytest.param({"payload_version": 2}, "unsupported_payload_version", id="unknown version"),
    ],
)
async def test_a_stored_payload_that_fails_validation_fails_the_job(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    overrides: dict[str, Any],
    code: str,
) -> None:
    job_id = await insert_due_job(engine, **overrides)

    assert await execute_job(sessions, job_id, HANDLERS, settings) == Outcome.FAILED

    row = await job_row(engine, job_id)
    assert (row["status"], row["attempts"], row["error_code"]) == ("failed", 1, code)
    assert row["lease_expires_at"] is None
    assert await notifications(engine, job_id) == []
    [attempt] = await attempts(engine, job_id)
    assert (attempt["outcome"], attempt["error_code"]) == ("failed", code)


async def test_a_stored_payload_over_the_current_limit_fails_the_job(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], test_database_url: str
) -> None:
    # The limit was lowered after the job was saved.
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, max_payload_bytes=10, max_request_body_bytes=10
    )
    job_id = await insert_due_job(engine)

    assert await execute_job(sessions, job_id, HANDLERS, settings) == Outcome.FAILED
    assert (await job_row(engine, job_id))["error_code"] == "payload_too_large"


async def test_a_type_without_a_handler_fails_the_job(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> None:
    job_id = await insert_due_job(engine)

    assert await execute_job(sessions, job_id, {}, settings) == Outcome.FAILED
    assert (await job_row(engine, job_id))["error_code"] == "unknown_job_type"


# Handler failures


async def raises_with_payload_text(
    session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
) -> dict[str, Any]:
    raise RuntimeError(f"provider rejected {SENTINEL}@example.com")


async def raises_its_own_timeout(
    session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
) -> dict[str, Any]:
    raise TimeoutError(f"provider timed out for {SENTINEL}")


@pytest.mark.parametrize(
    "handler",
    [
        pytest.param(raises_with_payload_text, id="raises"),
        # The handler's own timeout is an error of the handler, not handler_timeout_s.
        pytest.param(raises_its_own_timeout, id="raises TimeoutError itself"),
    ],
)
async def test_a_handler_error_fails_the_job_without_its_text(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
    handler: Handler,
) -> None:
    # The job's only attempt, so the retryable code fails it (retries are tested below).
    job_id = await insert_due_job(engine, max_attempts=1)

    outcome = await execute_job(sessions, job_id, {NOTIFICATION_SEND: handler}, settings)

    assert outcome == Outcome.FAILED
    row = await job_row(engine, job_id)
    assert (row["status"], row["error_code"], row["result"]) == ("failed", "handler_error", None)
    assert f"job {job_id} attempt 1 failed: handler_error (builtins." in caplog.text
    [attempt] = await attempts(engine, job_id)
    assert (attempt["outcome"], attempt["error_code"]) == ("failed", "handler_error")
    assert attempt["finished_at"] == row["updated_at"]
    # Neither the database rows (besides the job's payload) nor the log hold the exception's text.
    del row["payload"]
    assert SENTINEL not in json.dumps([row, attempt], default=str)
    assert_no_sentinel(caplog)


async def test_a_handler_over_its_timeout_is_cancelled_and_fails_the_job(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, handler_timeout_s=0.1, lease_duration_s=10
    )
    cancelled = asyncio.Event()

    async def hangs(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")

    job_id = await insert_due_job(engine, max_attempts=1)

    async with asyncio.timeout(5):
        outcome = await execute_job(sessions, job_id, {NOTIFICATION_SEND: hangs}, settings)

    assert outcome == Outcome.FAILED
    assert cancelled.is_set()
    assert (await job_row(engine, job_id))["error_code"] == "handler_timeout"
    assert f"job {job_id} attempt 1 failed: handler_timeout (builtins.TimeoutError)" in caplog.text


async def test_a_handler_that_returns_after_its_timeout_still_fails_the_job(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, handler_timeout_s=0.1, lease_duration_s=10
    )

    async def swallows_cancellation(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.Event().wait()
        return {"late": True}

    job_id = await insert_due_job(engine, max_attempts=1)

    async with asyncio.timeout(5):
        outcome = await execute_job(
            sessions, job_id, {NOTIFICATION_SEND: swallows_cancellation}, settings
        )

    assert outcome == Outcome.FAILED
    row = await job_row(engine, job_id)
    assert (row["error_code"], row["result"]) == ("handler_timeout", None)
    assert f"job {job_id} attempt 1 failed: handler_timeout (returned after" in caplog.text


# Retries (ADR 0005)


def fixed(value: float) -> Callable[[], float]:
    return lambda: value


async def raises(
    session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
) -> dict[str, Any]:
    raise RuntimeError("provider unavailable")


@pytest.mark.parametrize(
    ("seeded_attempts", "jitter", "delay"),
    [
        # Ceiling after attempt n: min(cap, base * 2^(n-1)), here base 2 s and cap 300 s.
        pytest.param(0, 0.5, timedelta(seconds=1), id="attempt 1: half of 2 s"),
        pytest.param(2, 0.25, timedelta(seconds=2), id="attempt 3: a quarter of 8 s"),
        pytest.param(0, 0.0, timedelta(0), id="jitter 0: due at once"),
    ],
)
async def test_a_retryable_failure_with_attempts_left_is_retried(
    engine: AsyncEngine,
    settings: BrokerSettings,
    sessions: async_sessionmaker[AsyncSession],
    caplog: pytest.LogCaptureFixture,
    seeded_attempts: int,
    jitter: float,
    delay: timedelta,
) -> None:
    caplog.set_level(logging.INFO, logger="task_platform")
    job_id = await insert_due_job(engine, attempts=seeded_attempts, dispatched_at=datetime.now(UTC))
    attempt = seeded_attempts + 1
    settings = settings.model_copy(update={"retry_base_delay_s": 2.0, "retry_max_delay_s": 300.0})

    outcome = await execute_job(
        sessions,
        job_id,
        {NOTIFICATION_SEND: raises_with_payload_text},
        settings,
        jitter=fixed(jitter),
    )

    assert outcome == Outcome.RETRY_SCHEDULED
    row = await job_row(engine, job_id)
    assert (row["status"], row["attempts"], row["error_code"]) == (
        "queued",
        attempt,
        "handler_error",
    )
    assert row["available_at"] - row["updated_at"] == delay
    assert (row["dispatched_at"], row["lease_expires_at"], row["result"]) == (None, None, None)
    # The attempt is closed as failed, the same as a final failure (AttemptOutcome.FAILED).
    [*_, last] = await attempts(engine, job_id)
    assert (last["attempt"], last["outcome"], last["error_code"]) == (
        attempt,
        "failed",
        "handler_error",
    )
    assert last["finished_at"] == row["updated_at"]
    assert (
        f"job {job_id} attempt {attempt} failed with handler_error;"
        f" retry in {delay.total_seconds():.3f} s"
    ) in caplog.text
    # Criterion 13 on the retry path: neither the rows (besides the job's payload) nor the logs
    # at INFO and above hold the exception's text.
    del row["payload"]
    assert SENTINEL not in json.dumps([row, last], default=str)
    assert_no_sentinel(caplog)


async def test_a_handler_timeout_is_retried_within_the_cap(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    test_database_url: str,
) -> None:
    # The other retryable code, past the cap: after attempt 3 the ceiling is min(3 s, 2 s * 4).
    settings = make_test_broker_settings(
        test_database_url,
        UNREACHABLE_BROKER_URL,
        handler_timeout_s=0.1,
        lease_duration_s=10,
        retry_base_delay_s=2.0,
        retry_max_delay_s=3.0,
    )

    async def hangs(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    job_id = await insert_due_job(engine, attempts=2)

    async with asyncio.timeout(5):
        outcome = await execute_job(
            sessions, job_id, {NOTIFICATION_SEND: hangs}, settings, jitter=fixed(0.5)
        )

    assert outcome == Outcome.RETRY_SCHEDULED
    row = await job_row(engine, job_id)
    assert (row["status"], row["attempts"], row["error_code"]) == ("queued", 3, "handler_timeout")
    assert row["available_at"] - row["updated_at"] == timedelta(seconds=1.5)
    [last] = await attempts(engine, job_id)
    assert (last["attempt"], last["outcome"], last["error_code"]) == (
        3,
        "failed",
        "handler_timeout",
    )


@pytest.mark.parametrize(
    ("job_budget", "setting", "expected"),
    [
        # Submitted with 2 attempts; the setting for new jobs now says 5 and must not count.
        pytest.param(2, 5, Outcome.FAILED, id="job budget below the setting"),
        # Submitted with 5 attempts; the setting now says 1 and must not count either.
        pytest.param(5, 1, Outcome.RETRY_SCHEDULED, id="job budget above the setting"),
    ],
)
async def test_the_retry_uses_the_jobs_own_attempt_budget(
    engine: AsyncEngine,
    settings: BrokerSettings,
    sessions: async_sessionmaker[AsyncSession],
    job_budget: int,
    setting: int,
    expected: Outcome,
) -> None:
    job_id = await insert_due_job(engine, attempts=1, max_attempts=job_budget)
    settings = settings.model_copy(update={"default_max_attempts": setting})

    outcome = await execute_job(
        sessions, job_id, {NOTIFICATION_SEND: raises}, settings, jitter=fixed(0.0)
    )

    assert outcome == expected
    row = await job_row(engine, job_id)
    status = "failed" if expected == Outcome.FAILED else "queued"
    assert (row["status"], row["attempts"], row["error_code"]) == (status, 2, "handler_error")


async def test_without_a_jitter_each_retry_draws_its_own_delay(
    engine: AsyncEngine, settings: BrokerSettings, sessions: async_sessionmaker[AsyncSession]
) -> None:
    # The worker passes no jitter: random.random() decides, within [0, ceiling). Two jobs that
    # failed together must not retry together, which a constant jitter would make them do. Odds of
    # a false failure: two equal draws, or one of 0.0, at microsecond resolution over 60 s, ~1e-8.
    settings = settings.model_copy(update={"retry_base_delay_s": 60.0, "retry_max_delay_s": 300.0})
    delays = []
    for _ in range(2):
        job_id = await insert_due_job(engine)
        assert await execute_job(sessions, job_id, {NOTIFICATION_SEND: raises}, settings) == (
            Outcome.RETRY_SCHEDULED
        )
        row = await job_row(engine, job_id)
        delays.append(row["available_at"] - row["updated_at"])

    assert all(timedelta(0) < delay < timedelta(seconds=60) for delay in delays)
    assert delays[0] != delays[1]


async def test_a_retryable_failure_on_the_last_attempt_fails_the_job(
    engine: AsyncEngine, settings: BrokerSettings, sessions: async_sessionmaker[AsyncSession]
) -> None:
    job_id = await insert_due_job(engine, attempts=4, max_attempts=5)

    outcome = await execute_job(
        sessions, job_id, {NOTIFICATION_SEND: raises}, settings, jitter=fixed(0.5)
    )

    assert outcome == Outcome.FAILED
    row = await job_row(engine, job_id)
    assert (row["status"], row["attempts"], row["error_code"]) == ("failed", 5, "handler_error")
    assert row["lease_expires_at"] is None
    [attempt] = await attempts(engine, job_id)
    assert (attempt["attempt"], attempt["outcome"]) == (5, "failed")


@pytest.mark.parametrize(
    ("jitter", "retryable", "error"),
    [
        pytest.param(1.0, None, "builtins.ValueError", id="jitter outside [0, 1)"),
        pytest.param(0.5, {}, "builtins.KeyError", id="code missing from RETRYABLE"),
    ],
)
async def test_a_retry_decision_that_cannot_be_made_fails_the_job(
    engine: AsyncEngine,
    settings: BrokerSettings,
    sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    jitter: float,
    retryable: dict[FailureCode, bool] | None,
    error: str,
) -> None:
    # A bug in the decision must not leave the job running: it fails, with its real code.
    if retryable is not None:
        monkeypatch.setattr(retries, "RETRYABLE", retryable)
    job_id = await insert_due_job(engine)

    outcome = await execute_job(
        sessions, job_id, {NOTIFICATION_SEND: raises}, settings, jitter=fixed(jitter)
    )

    assert outcome == Outcome.FAILED
    row = await job_row(engine, job_id)
    assert (row["status"], row["error_code"], row["lease_expires_at"]) == (
        "failed",
        "handler_error",
        None,
    )
    [attempt] = await attempts(engine, job_id)
    assert (attempt["outcome"], attempt["error_code"]) == ("failed", "handler_error")
    [record] = [r for r in caplog.records if "no retry decision" in r.getMessage()]
    assert record.levelno == logging.ERROR
    assert record.getMessage() == (
        f"job {job_id} attempt 1 of 5: no retry decision for handler_error ({error}); the job fails"
    )


async def test_a_retry_refused_by_a_lowered_budget_fails_the_job(
    engine: AsyncEngine,
    settings: BrokerSettings,
    sessions: async_sessionmaker[AsyncSession],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The decision used the budget from the claim; meanwhile the row's budget was cut to the
    # attempt that is running (by hand: no code changes it), so mark_retry's own condition refuses
    # the requeue. The job fails instead of staying running (ADR 0005 decision 2).
    job_id = await insert_due_job(engine)

    async def budget_cut_then_raises(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE jobs SET max_attempts = attempts WHERE id = :id"), {"id": job.id}
            )
        raise RuntimeError("provider unavailable")

    outcome = await execute_job(
        sessions, job_id, {NOTIFICATION_SEND: budget_cut_then_raises}, settings, jitter=fixed(0.5)
    )

    assert outcome == Outcome.FAILED
    row = await job_row(engine, job_id)
    assert (row["status"], row["attempts"], row["error_code"], row["lease_expires_at"]) == (
        "failed",
        1,
        "handler_error",
        None,
    )
    [attempt] = await attempts(engine, job_id)
    assert (attempt["outcome"], attempt["error_code"]) == ("failed", "handler_error")
    [record] = [r for r in caplog.records if "was not requeued" in r.getMessage()]
    assert record.levelno == logging.ERROR
    assert record.getMessage() == (
        f"job {job_id} attempt 1 was not requeued: its max_attempts was lowered after the claim;"
        " the job fails with handler_error"
    )
    assert "is no longer current" not in caplog.text
    assert "retry in" not in caplog.text


async def test_a_stale_attempts_retry_is_discarded(
    engine: AsyncEngine,
    settings: BrokerSettings,
    sessions: async_sessionmaker[AsyncSession],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A newer attempt owns the job: neither the retry nor the fallback to a failure applies.
    job_id = await insert_due_job(engine)

    async def overtaken_then_raises(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE jobs SET attempts = attempts + 1 WHERE id = :id"), {"id": job.id}
            )
        raise RuntimeError("provider unavailable")

    outcome = await execute_job(
        sessions, job_id, {NOTIFICATION_SEND: overtaken_then_raises}, settings, jitter=fixed(0.5)
    )

    assert outcome == Outcome.STALE
    row = await job_row(engine, job_id)
    assert (row["status"], row["attempts"], row["error_code"]) == ("running", 2, None)
    [attempt] = await attempts(engine, job_id)
    assert (attempt["attempt"], attempt["outcome"]) == (1, None)
    assert f"job {job_id} attempt 1 is no longer current" in caplog.text
    assert "was not requeued" not in caplog.text


# The final update


async def test_a_stale_attempt_keeps_its_side_effect_but_not_its_outcome(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = await insert_due_job(engine)

    async def overtaken(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        result = await send_notification(session_factory, job, payload)
        # Meanwhile the lease expired and a newer attempt claimed the job (phase 8 recovery).
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE jobs SET attempts = attempts + 1 WHERE id = :id"), {"id": job.id}
            )
        return result

    outcome = await execute_job(sessions, job_id, {NOTIFICATION_SEND: overtaken}, settings)

    assert outcome == Outcome.STALE
    row = await job_row(engine, job_id)
    # The newer attempt still owns the job; this attempt's result was not written.
    assert (row["status"], row["attempts"], row["result"]) == ("running", 2, None)
    # Fencing rejects writes, not side effects (ADR 0001).
    assert await notifications(engine, job_id) == [1]
    assert f"job {job_id} attempt 1 is no longer current" in caplog.text
    # Nor its history: the stale attempt's row stays open, for recovery to close (phase 8).
    [attempt] = await attempts(engine, job_id)
    assert (attempt["attempt"], attempt["finished_at"], attempt["outcome"]) == (1, None, None)


async def test_a_newer_attempt_after_a_stale_one_gets_the_same_notification(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
) -> None:
    # Fencing cannot stop the stale attempt's call; the provider's key makes the newer attempt's
    # call the same notification (ADR 0007 decision 8).
    job_id = await insert_due_job(engine)

    async def overtaken(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        result = await send_notification(session_factory, job, payload)
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE jobs SET attempts = attempts + 1 WHERE id = :id"), {"id": job.id}
            )
        return result

    assert (
        await execute_job(sessions, job_id, {NOTIFICATION_SEND: overtaken}, settings)
        == Outcome.STALE
    )
    # The newer attempt's handler, as its worker would run it.
    newer = await send_notification(
        sessions, ClaimedJob(id=job_id, attempt=2), NotificationSendV1.model_validate(PAYLOAD)
    )

    [notification] = await fake_notification_rows(engine, job_id)
    assert newer == fake_result(notification)
    assert notification["attempt"] == 1


@pytest.mark.parametrize("failure", ["handler_error", "handler_timeout"])
async def test_a_retry_after_the_provider_was_called_sends_nothing_new(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    test_database_url: str,
    failure: str,
) -> None:
    # Attempt 1 reaches the provider, then fails in a way that is retried; attempt 2 succeeds.
    # The job ends with one notification, and its result names it (ADR 0007 decisions 8 and 9).
    # The timeout also covers the provider's own transaction, which must commit before it fires:
    # 1 s, not 0.1 s, so a slow machine does not cancel attempt 1 before its notification exists.
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, handler_timeout_s=1.0, lease_duration_s=10
    )

    async def sends_then_fails(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        await send_notification(session_factory, job, payload)
        if failure == "handler_timeout":
            await asyncio.Event().wait()
        raise RuntimeError("after the provider call")

    job_id = await insert_due_job(engine)
    async with asyncio.timeout(5):
        first = await execute_job(
            sessions, job_id, {NOTIFICATION_SEND: sends_then_fails}, settings, jitter=fixed(0)
        )
    second = await execute_job(sessions, job_id, HANDLERS, settings)

    assert (first, second) == (Outcome.RETRY_SCHEDULED, Outcome.SUCCEEDED)
    row = await job_row(engine, job_id)
    [notification] = await fake_notification_rows(engine, job_id)
    assert (row["status"], row["attempts"], row["result"]) == (
        "succeeded",
        2,
        fake_result(notification),
    )
    assert notification["attempt"] == 1
    assert [(a["attempt"], a["error_code"]) for a in await attempts(engine, job_id)] == [
        (1, failure),
        (2, None),
    ]


async def test_an_attempt_without_a_row_still_finishes_the_job(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A row lost after the claim (a bug or a manual change): nothing to close. The job's outcome
    # matters more than its history, so the finish goes ahead and reports the gap as an error.
    job_id = await insert_due_job(engine)

    async def loses_its_row(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        async with engine.begin() as connection:
            await connection.execute(
                text("DELETE FROM job_attempts WHERE job_id = :id"), {"id": job.id}
            )
        return {"done": True}

    outcome = await execute_job(sessions, job_id, {NOTIFICATION_SEND: loses_its_row}, settings)

    assert outcome == Outcome.SUCCEEDED
    row = await job_row(engine, job_id)
    assert (row["status"], row["result"]) == ("succeeded", {"done": True})
    assert await attempts(engine, job_id) == []
    [record] = [r for r in caplog.records if "had no open attempt row" in r.getMessage()]
    assert record.levelno == logging.ERROR
    assert record.getMessage() == (
        f"job {job_id} attempt 1 had no open attempt row to close;"
        " its outcome succeeded is on the job only"
    )


async def test_a_later_attempt_closes_its_own_row(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Two failed attempts before this one: the finish closes attempt 3 and leaves their outcomes.
    job_id = await insert_due_job(engine, attempts=2)
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO job_attempts (job_id, attempt, finished_at, outcome, error_code)"
                " VALUES (:id, 1, now(), 'failed', 'handler_error'),"
                " (:id, 2, now(), 'failed', 'handler_timeout')"
            ),
            {"id": job_id},
        )
    earlier = await attempts(engine, job_id)

    assert await execute_job(sessions, job_id, HANDLERS, settings) == Outcome.SUCCEEDED

    row = await job_row(engine, job_id)
    *unchanged, last = await attempts(engine, job_id)
    assert unchanged == earlier
    assert (last["attempt"], last["outcome"], last["error_code"]) == (3, "succeeded", None)
    assert last["finished_at"] == row["updated_at"]
    assert "had no open attempt row" not in caplog.text


JOB_ROW_LOCK = "SELECT 1 FROM jobs WHERE id = :id FOR UPDATE"
ATTEMPT_ROW_LOCK = "SELECT 1 FROM job_attempts WHERE job_id = :id FOR UPDATE"


@pytest.mark.parametrize(
    ("lock", "fails"),
    [
        # The job's update waits past the timeout, before the attempt row is touched.
        pytest.param(JOB_ROW_LOCK, False, id="job row locked"),
        # The job's update goes through, then closing the attempt waits past the timeout: the
        # error rolls both back, so the job is not finished while its attempt stays open.
        pytest.param(ATTEMPT_ROW_LOCK, False, id="attempt row locked"),
        # The same for a retry: the job is not requeued while its attempt stays open.
        pytest.param(ATTEMPT_ROW_LOCK, True, id="attempt row locked, retry"),
    ],
)
async def test_a_final_update_the_database_cannot_take_leaves_the_job_running(
    engine: AsyncEngine,
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
    lock: str,
    fails: bool,
) -> None:
    # Another transaction holds a row lock past the worker's statement timeout.
    settings = make_test_broker_settings(
        test_database_url,
        UNREACHABLE_BROKER_URL,
        db_statement_timeout_ms=200,
        publish_timeout_s=0.1,
    )
    worker_engine = create_db_engine(settings)
    job_id = await insert_due_job(engine)
    holder = await engine.connect()

    async def locks_the_row(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        await holder.begin()
        await holder.execute(text(lock), {"id": job.id})
        if fails:
            raise RuntimeError("provider unavailable")
        return {"done": True}

    try:
        outcome = await execute_job(
            async_sessionmaker(worker_engine),
            job_id,
            {NOTIFICATION_SEND: locks_the_row},
            settings,
            jitter=fixed(0.5),
        )
    finally:
        await holder.close()
        await worker_engine.dispose()

    assert outcome == Outcome.FINISH_UNAVAILABLE
    row = await job_row(engine, job_id)
    assert (row["status"], row["attempts"], row["result"], row["error_code"]) == (
        "running",
        1,
        None,
        None,
    )
    [attempt] = await attempts(engine, job_id)
    assert (attempt["finished_at"], attempt["outcome"]) == (None, None)
    # 57014: query_canceled by the statement timeout.
    assert f"final update failed: job {job_id} attempt 1," in caplog.text
    assert "(sqlstate 57014)" in caplog.text


# Database errors at the claim


async def test_a_claim_waiting_past_the_statement_timeout_backs_off(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # The dispatcher holds the row lock while it publishes (ADR 0004 decision 8).
    settings = make_test_broker_settings(
        test_database_url,
        UNREACHABLE_BROKER_URL,
        db_statement_timeout_ms=200,
        publish_timeout_s=0.1,
    )
    worker_engine = create_db_engine(settings)
    job_id = await insert_due_job(engine)
    before = await job_row(engine, job_id)

    try:
        async with engine.begin() as holder:
            await holder.execute(
                text("SELECT 1 FROM jobs WHERE id = :id FOR UPDATE"), {"id": job_id}
            )
            outcome = await execute_job(
                async_sessionmaker(worker_engine), job_id, HANDLERS, settings
            )
    finally:
        await worker_engine.dispose()

    assert outcome == Outcome.CLAIM_UNAVAILABLE
    assert await job_row(engine, job_id) == before
    assert await attempts(engine, job_id) == []
    assert f"claim failed, database unavailable: job {job_id}," in caplog.text
    assert "(sqlstate 57014)" in caplog.text


async def test_a_claim_the_database_rejects_for_this_job_is_not_retried(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A lease so long that now() + lease is past PostgreSQL's timestamp range: a DataError that
    # every redelivery of this message would hit again.
    # model_copy skips validation, which bounds lease_duration_s to rule this out.
    settings = make_test_broker_settings(test_database_url, UNREACHABLE_BROKER_URL).model_copy(
        update={"lease_duration_s": 1e13}
    )
    job_id = await insert_due_job(engine)
    before = await job_row(engine, job_id)

    assert await execute_job(sessions, job_id, HANDLERS, settings) == Outcome.CLAIM_REJECTED
    assert await job_row(engine, job_id) == before
    assert await attempts(engine, job_id) == []
    # 22008: datetime_field_overflow.
    assert f"claim rejected by the database: job {job_id}," in caplog.text
    assert "(sqlstate 22008)" in caplog.text


# The transition table


@pytest.mark.parametrize(
    ("forbidden", "handlers", "status_after"),
    [
        pytest.param((JobStatus.QUEUED, JobStatus.RUNNING), HANDLERS, "queued", id="claim"),
        pytest.param((JobStatus.RUNNING, JobStatus.SUCCEEDED), HANDLERS, "running", id="finish"),
        pytest.param(
            (JobStatus.RUNNING, JobStatus.QUEUED),
            {NOTIFICATION_SEND: raises},
            "running",
            id="retry",
        ),
    ],
)
async def test_every_change_is_checked_against_the_transition_table(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    monkeypatch: pytest.MonkeyPatch,
    forbidden: tuple[JobStatus, JobStatus],
    handlers: Mapping[str, Handler],
    status_after: str,
) -> None:
    # If ADR 0001's table stopped allowing a change, the service would refuse to make it.
    current, target = forbidden
    table = dict(job_status.ALLOWED_TRANSITIONS) | {
        current: job_status.ALLOWED_TRANSITIONS[current] - {target}
    }
    monkeypatch.setattr(job_status, "ALLOWED_TRANSITIONS", table)
    job_id = await insert_due_job(engine)

    with pytest.raises(InvalidTransitionError):
        await execute_job(sessions, job_id, handlers, settings)
    assert (await job_row(engine, job_id))["status"] == status_after
