"""`POST /jobs` when PostgreSQL is slow, busy or rejects the row: 503 or 500, no payload leaked.

Every test captures logs at DEBUG, which also turns on SQLAlchemy's own SQL logging: the sentinel
must stay out of that too (the engine hides bound parameters).
"""

import logging

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from task_platform.config import Settings
from task_platform.main import create_app
from tests.app_client import app_client
from tests.settings import make_test_settings

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"

JOB = {
    "type": "notification.send",
    "payload_version": 1,
    "payload": {"recipient": "max@example.com", "subject": "Hi", "body": SENTINEL},
}


@pytest.fixture(autouse=True)
def capture_debug_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    # SQLAlchemy pins its own logger to WARNING on import, so the root level alone does not
    # reach it.
    caplog.set_level(logging.DEBUG, logger="sqlalchemy")


def assert_error(
    response: httpx.Response, caplog: pytest.LogCaptureFixture, status_code: int, code: str
) -> None:
    assert response.status_code == status_code
    assert response.json()["error"]["code"] == code
    assert SENTINEL not in response.text
    assert SENTINEL not in caplog.text


async def lock_jobs_table(engine: AsyncEngine) -> AsyncConnection:
    # Held until the caller rolls back: every INSERT into jobs waits behind it.
    connection = await engine.connect()
    await connection.begin()
    await connection.execute(text("LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE"))
    return connection


async def count_jobs(engine: AsyncEngine) -> int:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT count(*) FROM jobs"))
    count: int = result.scalar_one()
    return count


# Database slow: the server-side statement_timeout fires first (the normal case)


async def test_statement_timeout_returns_503(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    app = create_app(make_test_settings(test_database_url, db_statement_timeout_ms=200))
    locker = await lock_jobs_table(engine)
    try:
        async with app_client(app) as client:
            response = await client.post("/jobs", json=JOB)
    finally:
        await locker.rollback()
        await locker.close()

    assert_error(response, caplog, 503, "service_unavailable")
    assert "database unavailable: sqlalchemy.exc.DBAPIError (sqlstate 57014)" in caplog.text
    assert await count_jobs(engine) == 0


# Database unresponsive: the client-side command_timeout fires (backstop, D8)


async def test_command_timeout_returns_503_and_the_pool_recovers(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # model_copy skips validation: the settings forbid a command timeout under the statement
    # timeout, but that is the only way to make the client give up before the server does.
    settings = make_test_settings(test_database_url, db_pool_size=1).model_copy(
        update={"db_command_timeout_s": 0.2}
    )
    app = create_app(settings)
    async with app_client(app) as client:
        locker = await lock_jobs_table(engine)
        try:
            timed_out = await client.post("/jobs", json=JOB)
        finally:
            await locker.rollback()
            await locker.close()
        # The one pooled connection must be usable again, or invalidated and replaced.
        after = await client.post("/jobs", json=JOB)

    assert_error(timed_out, caplog, 503, "service_unavailable")
    assert "database unavailable: builtins.TimeoutError" in caplog.text
    assert after.status_code == 202
    assert await count_jobs(engine) == 1


async def test_command_timeout_also_bounds_commit(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # A deferred constraint trigger runs inside COMMIT, so the INSERT returns at once and only
    # COMMIT is slow. Without command_timeout the request would wait 2 s and get a 202.
    settings = make_test_settings(test_database_url).model_copy(
        update={"db_command_timeout_s": 0.3}
    )
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "CREATE OR REPLACE FUNCTION test_sleep_at_commit() RETURNS trigger"
                " LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2); RETURN NULL; END $$"
            )
        )
        await connection.execute(text("DROP TRIGGER IF EXISTS test_slow_commit ON jobs"))
        await connection.execute(
            text(
                "CREATE CONSTRAINT TRIGGER test_slow_commit AFTER INSERT ON jobs"
                " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW"
                " EXECUTE FUNCTION test_sleep_at_commit()"
            )
        )
    try:
        async with app_client(create_app(settings)) as client:
            response = await client.post("/jobs", json=JOB)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DROP TRIGGER test_slow_commit ON jobs"))
            await connection.execute(text("DROP FUNCTION test_sleep_at_commit()"))

    # Whether the job was saved is not asserted: that is exactly what the client cannot know
    # (README, known limitations).
    assert_error(response, caplog, 503, "service_unavailable")
    assert "database unavailable: builtins.TimeoutError" in caplog.text


# Database busy: no free connection in the pool


async def test_exhausted_pool_returns_503(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    settings = make_test_settings(test_database_url, db_pool_size=1, db_pool_timeout_s=0.2)
    app = create_app(settings)
    async with app_client(app) as client:
        async with app.state.engine.connect() as held:
            await held.execute(text("SELECT 1"))
            response = await client.post("/jobs", json=JOB)
        after = await client.post("/jobs", json=JOB)

    assert_error(response, caplog, 503, "service_unavailable")
    # Not the client-side command timeout (builtins.TimeoutError): this one says "pool too small".
    assert "database unavailable: sqlalchemy.exc.TimeoutError (sqlstate None)" in caplog.text
    assert after.status_code == 202


# Connection dropped between requests (PostgreSQL restarted, idle connection killed)


async def test_dropped_connection_returns_503_and_the_next_request_reconnects(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    app = create_app(make_test_settings(test_database_url, db_pool_size=1))
    async with app_client(app) as client:
        # pool_size=1: this is the connection the next request gets.
        async with app.state.engine.connect() as connection:
            pid = await connection.scalar(text("SELECT pg_backend_pid()"))
        async with engine.connect() as admin:
            # The timeout argument makes the call wait until the backend has exited.
            await admin.execute(text("SELECT pg_terminate_backend(:pid, 5000)"), {"pid": pid})
        dropped = await client.post("/jobs", json=JOB)
        after = await client.post("/jobs", json=JOB)

    assert_error(dropped, caplog, 503, "service_unavailable")
    # Not a 503 because of its class (InterfaceError) but because SQLAlchemy invalidated it.
    assert "database unavailable: sqlalchemy.exc.InterfaceError (sqlstate None)" in caplog.text
    assert after.status_code == 202


# Row rejected by the database: a bug, not an outage


async def test_check_violation_returns_500_without_the_row(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # model_copy skips validation, so the INSERT reaches ck_jobs_max_attempts_range. PostgreSQL's
    # error DETAIL then quotes the whole row, payload included.
    settings: Settings = make_test_settings(test_database_url).model_copy(
        update={"default_max_attempts": 21}
    )
    async with app_client(create_app(settings)) as client:
        response = await client.post("/jobs", json=JOB)

    assert_error(response, caplog, 500, "internal_error")
    # The constraint name is a schema identifier, not row data, so it is safe to log.
    expected = (
        "sqlalchemy.exc.IntegrityError (sqlstate 23514, constraint ck_jobs_max_attempts_range)"
    )
    assert "database error: " + expected in caplog.text
    assert await count_jobs(engine) == 0
