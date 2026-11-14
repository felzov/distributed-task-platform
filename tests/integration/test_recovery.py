"""One lease recovery sweep against the real database (ADR 0001 decision 8, ADR 0008 decision 1)."""

import asyncio
import json
import logging
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.config import BrokerSettings, Settings
from task_platform.data.attempts import open_attempt
from task_platform.data.jobs import claim_job
from task_platform.domain.failures import FAILURE_MESSAGES, FailureCode
from task_platform.domain.payloads import NOTIFICATION_SEND, PayloadModel
from task_platform.main import create_app
from task_platform.services import recovery
from task_platform.services.execution import ClaimedJob, Handler, Outcome, execute_job
from task_platform.services.recovery import LeaseSweep, recover_expired_leases
from tests.app_client import app_client
from tests.integration.rows import insert_job
from tests.settings import UNREACHABLE_BROKER_URL, make_test_broker_settings

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"
PAYLOAD = {"recipient": f"{SENTINEL}@example.com", "subject": "Hello", "body": "A message."}
# Hours away from now, so the test machine's clock and PostgreSQL's never disagree about them.
PAST = datetime.now(UTC) - timedelta(hours=3)
FUTURE = datetime.now(UTC) + timedelta(hours=3)


@pytest.fixture
def settings(test_database_url: str) -> BrokerSettings:
    # The sweep never connects to the broker.
    return make_test_broker_settings(test_database_url, UNREACHABLE_BROKER_URL)


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _info_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)


async def insert_crashed(
    engine: AsyncEngine,
    *,
    attempts: int = 1,
    max_attempts: int = 5,
    lease_expires_at: datetime = PAST,
    open_row: bool = True,
) -> uuid.UUID:
    """A job whose worker claimed attempt `attempts` and never finished it."""
    job_id: uuid.UUID = (
        await insert_job(
            engine,
            payload=json.dumps(PAYLOAD),
            status="running",
            attempts=attempts,
            max_attempts=max_attempts,
            lease_expires_at=lease_expires_at,
            dispatched_at=PAST,
        )
    )["id"]
    if open_row:
        async with async_sessionmaker(engine).begin() as session:
            await open_attempt(session, job_id, attempt=attempts)
    return job_id


async def job_row(engine: AsyncEngine, job_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


async def attempt_rows(engine: AsyncEngine, job_id: uuid.UUID) -> list[tuple[Any, ...]]:
    """(attempt, outcome, error_code, finished) per attempt row, lowest first."""
    async with engine.connect() as connection:
        result = await connection.execute(
            text(
                "SELECT attempt, outcome, error_code, finished_at IS NOT NULL FROM job_attempts"
                " WHERE job_id = :id ORDER BY attempt"
            ),
            {"id": job_id},
        )
        return [tuple(row) for row in result]


async def expire_lease(engine: AsyncEngine, job_id: uuid.UUID) -> None:
    # As if lease_duration_s had passed since the claim.
    async with engine.begin() as connection:
        await connection.execute(
            text("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = :id"),
            {"id": job_id},
        )


# Normal path


async def test_a_job_with_attempts_left_is_requeued_and_its_attempt_closed(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = await insert_crashed(engine, attempts=2, max_attempts=3)

    sweep = await recover_expired_leases(sessions, settings)

    assert sweep == LeaseSweep(expired=1, requeued=1, failed=0)
    row = await job_row(engine, job_id)
    # Criterion 1: due now, unpublished, no lease, the expiry recorded; the attempt still counts.
    assert (row["status"], row["attempts"], row["error_code"]) == ("queued", 2, "lease_expired")
    assert row["available_at"] == row["updated_at"]
    assert (row["dispatched_at"], row["lease_expires_at"]) == (None, None)
    assert await attempt_rows(engine, job_id) == [(2, "failed", "lease_expired", True)]
    assert f"job {job_id} attempt 2 of 3: lease expired; requeued" in caplog.text


async def test_a_job_on_its_last_attempt_fails_and_is_listed_as_failed(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    test_settings: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = await insert_crashed(engine, attempts=3, max_attempts=3)

    sweep = await recover_expired_leases(sessions, settings)

    assert sweep == LeaseSweep(expired=1, requeued=0, failed=1)
    row = await job_row(engine, job_id)
    assert (row["status"], row["attempts"], row["error_code"]) == ("failed", 3, "lease_expired")
    assert row["lease_expires_at"] is None
    assert await attempt_rows(engine, job_id) == [(3, "failed", "lease_expired", True)]
    assert f"job {job_id} attempt 3 of 3: lease expired; the job fails" in caplog.text
    # Criterion 2: failure inspection lists it with the code's fixed message.
    async with app_client(create_app(test_settings)) as client:
        jobs = (await client.get("/jobs?status=failed")).json()["jobs"]
    assert [(job["id"], job["failure"]) for job in jobs] == [
        (
            str(job_id),
            {"code": "lease_expired", "message": FAILURE_MESSAGES[FailureCode.LEASE_EXPIRED]},
        )
    ]


async def test_nothing_expired_recovers_nothing(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> None:
    # Criterion 3: a lease with time left is the worker's; other statuses never match (the
    # transition matrix and the query tests cover them one by one).
    job_id = await insert_crashed(engine, lease_expires_at=FUTURE)
    before = await job_row(engine, job_id)

    assert await recover_expired_leases(sessions, settings) == LeaseSweep(0, 0, 0)

    assert await job_row(engine, job_id) == before
    assert await attempt_rows(engine, job_id) == [(1, None, None, False)]


async def test_one_sweep_recovers_one_batch(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], test_database_url: str
) -> None:
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, dispatch_batch_size=2
    )
    earliest = [
        await insert_crashed(engine, lease_expires_at=PAST - timedelta(minutes=minutes))
        for minutes in (2, 1)
    ]
    last = await insert_crashed(engine, lease_expires_at=PAST)

    assert await recover_expired_leases(sessions, settings) == LeaseSweep(2, 2, 0)
    assert [(await job_row(engine, job_id))["status"] for job_id in earliest] == ["queued"] * 2
    assert (await job_row(engine, last))["status"] == "running"

    # The next sweep takes what the batch left.
    assert await recover_expired_leases(sessions, settings) == LeaseSweep(1, 1, 0)
    assert (await job_row(engine, last))["status"] == "queued"


async def test_logs_carry_no_payload_even_at_debug(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await insert_crashed(engine, attempts=1)
    await insert_crashed(engine, attempts=1, max_attempts=1)
    # Only now: the seeding's RETURNING * logs the payload. DEBUG also turns on SQLAlchemy's
    # logging of statements and result rows; the sweep never selects the payload.
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="sqlalchemy")

    assert await recover_expired_leases(sessions, settings) == LeaseSweep(2, 1, 1)

    assert "FROM jobs" in caplog.text
    assert SENTINEL not in caplog.text


# Invalid stored data


async def test_a_missing_attempt_row_is_an_error_and_the_job_is_still_recovered(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Expected only for an attempt claimed before migration 0003 (ADR 0005 decision 7); since then
    # every committed claim opens its row. Raising would roll back the batch and repeat on every
    # sweep, so the recovery is kept, as for a worker's finish.
    job_id = await insert_crashed(engine, open_row=False)

    assert await recover_expired_leases(sessions, settings) == LeaseSweep(1, 1, 0)

    assert (await job_row(engine, job_id))["status"] == "queued"
    assert await attempt_rows(engine, job_id) == []
    [record] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert record.getMessage() == (
        f"job {job_id} attempt 1 had no open attempt row to close; lease_expired is on the job only"
    )


async def test_a_running_job_with_no_counted_attempt_fails(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Only a hand-edited row: a claim always counts its attempt. Outside the budget, there is no
    # retry decision, so the job fails, as a worker's finish would.
    job_id = await insert_crashed(engine, attempts=0, open_row=False)

    assert await recover_expired_leases(sessions, settings) == LeaseSweep(1, 0, 1)

    row = await job_row(engine, job_id)
    assert (row["status"], row["error_code"]) == ("failed", "lease_expired")
    assert (
        f"job {job_id} attempt 0 of 5: no retry decision for lease_expired"
        " (builtins.ValueError); the job fails"
    ) in caplog.text


async def test_a_recovery_that_matches_no_row_is_an_error_and_not_counted(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The row lock makes it impossible, so the update is replaced by one that matches nothing.
    async def matches_nothing(session: AsyncSession, job_id: uuid.UUID, *, attempt: int) -> bool:
        return False

    monkeypatch.setattr(recovery, "requeue_expired_lease", matches_nothing)
    job_id = await insert_crashed(engine)

    assert await recover_expired_leases(sessions, settings) == LeaseSweep(1, 0, 0)

    assert (await job_row(engine, job_id))["status"] == "running"
    # Its attempt row stays open: only a recovered job closes it.
    assert await attempt_rows(engine, job_id) == [(1, None, None, False)]
    assert (
        f"job {job_id} attempt 1: lease recovery matched no row; the job is left as it is"
        in caplog.text
    )


# Failure paths


async def test_a_database_error_recovers_nothing_of_the_batch(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    first = await insert_crashed(engine, lease_expires_at=PAST - timedelta(minutes=1))
    second = await insert_crashed(engine, lease_expires_at=PAST)
    async with engine.begin() as connection:
        # The second job's update fails after the first one's went through.
        await connection.execute(
            text(
                "CREATE FUNCTION test_refuse_update() RETURNS trigger LANGUAGE plpgsql AS"
                " $$ BEGIN RAISE EXCEPTION 'refused by test'; END $$"
            )
        )
        await connection.execute(
            text(
                "CREATE TRIGGER test_refuse_update BEFORE UPDATE ON jobs FOR EACH ROW"
                f" WHEN (OLD.id = '{second}') EXECUTE FUNCTION test_refuse_update()"
            )
        )
    try:
        with pytest.raises(DBAPIError):
            await recover_expired_leases(sessions, settings)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DROP TRIGGER test_refuse_update ON jobs"))
            await connection.execute(text("DROP FUNCTION test_refuse_update()"))

    # One transaction: the first job's recovery and attempt closing are rolled back with it, and
    # the logs claim no recovery that was rolled back.
    for job_id in (first, second):
        assert (await job_row(engine, job_id))["status"] == "running"
        assert await attempt_rows(engine, job_id) == [(1, None, None, False)]
    assert "lease expired" not in caplog.text
    # The next sweep finds both again.
    assert await recover_expired_leases(sessions, settings) == LeaseSweep(2, 2, 0)


async def test_a_failed_attempt_close_rolls_back_the_jobs_recovery(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> None:
    # Criterion 1, "in the same transaction": closing the attempt row fails after the job's
    # update went through, and the job's recovery goes with it. A close in a later transaction
    # would leave the job requeued over an attempt that stays open for good.
    job_id = await insert_crashed(engine)
    before = await job_row(engine, job_id)
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "CREATE FUNCTION test_refuse_close() RETURNS trigger LANGUAGE plpgsql AS"
                " $$ BEGIN RAISE EXCEPTION 'refused by test'; END $$"
            )
        )
        await connection.execute(
            text(
                "CREATE TRIGGER test_refuse_close BEFORE UPDATE ON job_attempts FOR EACH ROW"
                " EXECUTE FUNCTION test_refuse_close()"
            )
        )
    try:
        with pytest.raises(DBAPIError):
            await recover_expired_leases(sessions, settings)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DROP TRIGGER test_refuse_close ON job_attempts"))
            await connection.execute(text("DROP FUNCTION test_refuse_close()"))

    assert await job_row(engine, job_id) == before
    assert await attempt_rows(engine, job_id) == [(1, None, None, False)]
    assert await recover_expired_leases(sessions, settings) == LeaseSweep(1, 1, 0)


async def test_concurrent_sweeps_share_the_jobs_and_recover_each_once(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Criterion 5 through the service; that SKIP LOCKED skips rather than waits is proven
    # deterministically in test_recovery_queries.py. Batches of 5 for 20 jobs and 4 sweeps: each
    # sweep finds at least 5 jobs no other sweep holds or has recovered, so each takes exactly 5,
    # in whatever order they run. A job recovered twice would show in the counts and as an ERROR
    # (no row matched, or no open attempt row).
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, dispatch_batch_size=5
    )
    job_ids = [await insert_crashed(engine) for _ in range(20)]

    sweeps = await asyncio.gather(*(recover_expired_leases(sessions, settings) for _ in range(4)))

    assert sweeps == [LeaseSweep(5, 5, 0)] * 4
    for job_id in job_ids:
        assert (await job_row(engine, job_id))["status"] == "queued"
        assert await attempt_rows(engine, job_id) == [(1, "failed", "lease_expired", True)]
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


# A worker whose lease expired, finishing after the sweep (criterion 4)


@pytest.mark.parametrize("claimed_again", [False, True], ids=["requeued", "claimed again"])
async def test_a_worker_finishing_after_the_sweep_writes_nothing(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    claimed_again: bool,
) -> None:
    job_id: uuid.UUID = (await insert_job(engine, payload=json.dumps(PAYLOAD)))["id"]

    async def outlives_its_lease(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        # While the handler runs, its lease runs out and the sweep recovers the job; maybe another
        # worker claims it too. Then the handler returns, too late.
        await expire_lease(engine, job.id)
        assert await recover_expired_leases(session_factory, settings) == LeaseSweep(1, 1, 0)
        if claimed_again:
            async with session_factory.begin() as session:
                assert await claim_job(session, job.id, lease_duration_s=60) is not None
                await open_attempt(session, job.id, attempt=2)
        return {"ok": True}

    handlers: Mapping[str, Handler] = {NOTIFICATION_SEND: outlives_its_lease}
    outcome = await execute_job(sessions, job_id, handlers, settings)

    assert outcome is Outcome.STALE
    row = await job_row(engine, job_id)
    assert row["result"] is None
    if claimed_again:
        assert (row["status"], row["attempts"]) == ("running", 2)
        assert await attempt_rows(engine, job_id) == [
            (1, "failed", "lease_expired", True),
            (2, None, None, False),
        ]
    else:
        assert (row["status"], row["error_code"]) == ("queued", "lease_expired")
        assert await attempt_rows(engine, job_id) == [(1, "failed", "lease_expired", True)]
