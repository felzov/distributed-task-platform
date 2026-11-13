"""Start-up against real PostgreSQL and RabbitMQ (ADR 0008 decision 7, criterion 11): the schema
check, configuration errors that stop the process, and outages it waits out through a proxy."""

import asyncio
import logging
import signal

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from task_platform import startup
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.data.schema import SCHEMA_REVISION, current_revisions
from task_platform.startup import StartupError, wait_for_broker, wait_for_schema
from tests.integration.processes import run_to_exit, stop_with_signal
from tests.proxy import running_proxy
from tests.settings import UNREACHABLE_BROKER_URL, make_test_broker_settings, with_url_parts

pytestmark = pytest.mark.integration

WRONG_PASSWORD = "wrong-s3cr3t-8812"


@pytest.fixture(autouse=True)
def _info_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="task_platform")


def settings_for(database_url: str, broker_url: str) -> BrokerSettings:
    return make_test_broker_settings(database_url, broker_url, startup_retry_interval_s=0.05)


async def wait_for_log(caplog: pytest.LogCaptureFixture, text: str) -> None:
    async with asyncio.timeout(5):
        while text not in caplog.text:
            await asyncio.sleep(0.01)


async def schema_check(database_url: str, expected: str = SCHEMA_REVISION) -> bool:
    settings = settings_for(database_url, UNREACHABLE_BROKER_URL)
    engine = create_db_engine(settings)
    try:
        return await wait_for_schema(engine, settings, asyncio.Event(), expected)
    finally:
        await engine.dispose()


# The schema check


async def test_a_database_at_this_codes_revision_passes(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    assert await schema_check(test_database_url) is True
    assert caplog.records == []


async def test_a_database_at_another_revision_stops_and_names_both(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    with pytest.raises(StartupError):
        await schema_check(test_database_url, expected="0099")
    assert (
        f"the database schema is at revision {SCHEMA_REVISION}, this code expects 0099: run the"
        " migrations first"
    ) in caplog.text
    assert "retrying" not in caplog.text


async def test_a_database_never_migrated_stops(
    engine: AsyncEngine,
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def no_revisions(connection: object) -> tuple[str, ...]:
        return ()

    # The shared test database stays migrated; an empty alembic_version reads the same way.
    monkeypatch.setattr(startup, "current_revisions", no_revisions)
    with pytest.raises(StartupError):
        await schema_check(test_database_url)
    assert "at revision none (never migrated)" in caplog.text


@pytest.mark.parametrize(
    ("parts", "sqlstate"),
    [({"password": WRONG_PASSWORD}, "28P01"), ({"path": "/no_such_database_test"}, "3D000")],
    ids=["wrong password", "unknown database"],
)
async def test_a_database_refusing_the_connection_stops_without_a_retry(
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
    parts: dict[str, str],
    sqlstate: str,
) -> None:
    # Bounded: a refusal retried by mistake would wait forever.
    with pytest.raises(StartupError):
        async with asyncio.timeout(5):
            await schema_check(with_url_parts(test_database_url, **parts))
    assert (
        f"the database cannot be used at start: sqlalchemy.exc.DBAPIError (sqlstate {sqlstate})"
    ) in caplog.text
    assert "retrying" not in caplog.text
    assert WRONG_PASSWORD not in caplog.text


async def test_a_database_at_this_codes_revision_and_another_stops(
    engine: AsyncEngine,
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Two heads (a branched migration history): not the schema this code was written for.
    async def two_revisions(connection: object) -> tuple[str, ...]:
        return (SCHEMA_REVISION, "0099")

    monkeypatch.setattr(startup, "current_revisions", two_revisions)
    with pytest.raises(StartupError):
        await schema_check(test_database_url)
    assert f"at revision {SCHEMA_REVISION}, 0099, this code expects" in caplog.text


class DriverError(Exception):
    """A driver error with the SQLSTATE a server sends, as asyncpg's carry it."""

    def __init__(self, sqlstate: str) -> None:
        super().__init__(f"quoting {WRONG_PASSWORD}")
        self.sqlstate = sqlstate


def failing_once(monkeypatch: pytest.MonkeyPatch, sqlstate: str) -> None:
    """The schema check's query fails once with `sqlstate`, then reads the real revisions."""
    failed = False

    async def revisions(connection: AsyncConnection) -> tuple[str, ...]:
        nonlocal failed
        if not failed:
            failed = True
            raise DBAPIError("SELECT", None, DriverError(sqlstate))
        return await current_revisions(connection)

    monkeypatch.setattr(startup, "current_revisions", revisions)


@pytest.mark.parametrize(
    "sqlstate", ["57P03", "53300", "08006"], ids=["starting up", "too many connections", "lost"]
)
async def test_a_database_not_taking_connections_for_now_is_retried(
    engine: AsyncEngine,
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    sqlstate: str,
) -> None:
    failing_once(monkeypatch, sqlstate)
    async with asyncio.timeout(5):
        assert await schema_check(test_database_url) is True
    assert (
        f"database not available at start: sqlalchemy.exc.DBAPIError (sqlstate {sqlstate});"
        " retrying in 0.05 s"
    ) in caplog.text
    assert WRONG_PASSWORD not in caplog.text


async def test_a_database_error_no_retry_fixes_stops(
    engine: AsyncEngine,
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 42501: the user may not read alembic_version. Retried, it would never pass.
    failing_once(monkeypatch, "42501")
    with pytest.raises(StartupError):
        async with asyncio.timeout(5):
            await schema_check(test_database_url)
    assert (
        "the database cannot be used at start: sqlalchemy.exc.DBAPIError (sqlstate 42501)"
    ) in caplog.text
    assert "retrying" not in caplog.text
    assert WRONG_PASSWORD not in caplog.text


async def test_the_schema_check_waits_for_the_database_to_come_back(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    async with running_proxy(test_database_url) as proxy:
        proxy.cut()
        checking = asyncio.create_task(schema_check(proxy.url(test_database_url)))
        await wait_for_log(caplog, "database not available at start")
        assert not checking.done()

        proxy.restore()
        async with asyncio.timeout(5):
            assert await checking is True


# The broker


@pytest.mark.parametrize(
    ("parts", "error"),
    [
        ({"password": WRONG_PASSWORD}, "aiormq.exceptions.ProbableAuthenticationError"),
        ({"path": "/no-such-vhost"}, "aiormq.exceptions.InvalidFrameError"),
    ],
    ids=["wrong password", "unknown vhost"],
)
async def test_a_broker_refusing_the_connection_stops_without_a_retry(
    test_broker_url: str, caplog: pytest.LogCaptureFixture, parts: dict[str, str], error: str
) -> None:
    settings = settings_for(test_broker_url, with_url_parts(test_broker_url, **parts))
    with pytest.raises(StartupError):
        async with asyncio.timeout(5):
            await wait_for_broker(settings, asyncio.Event())
    assert f"could not connect to the broker at start: {error}" in caplog.text
    assert "retrying" not in caplog.text
    assert WRONG_PASSWORD not in caplog.text


async def test_the_broker_connection_waits_for_the_broker_to_come_back(
    test_broker_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    async with running_proxy(test_broker_url) as proxy:
        proxy.cut()
        settings = settings_for(test_broker_url, proxy.url(test_broker_url))
        connecting = asyncio.create_task(wait_for_broker(settings, asyncio.Event()))
        await wait_for_log(caplog, "broker not available at start")
        assert not connecting.done()

        proxy.restore()
        async with asyncio.timeout(5):
            connection = await connecting
        assert connection is not None
        await connection.close()


# The processes

MODULES = ["task_platform.worker", "task_platform.dispatcher"]


@pytest.mark.parametrize("module", MODULES)
async def test_a_process_with_a_wrong_database_password_exits_with_status_1(
    engine: AsyncEngine, test_database_url: str, test_broker_url: str, module: str
) -> None:
    env = {
        "DATABASE_URL": with_url_parts(test_database_url, password=WRONG_PASSWORD),
        "BROKER_URL": test_broker_url,
    }
    status, stderr = await run_to_exit(module, env)

    assert status == 1
    # Not printed on failure: the child's errors could quote its settings.
    assert b"the database cannot be used at start" in stderr
    assert b"Traceback" not in stderr
    assert WRONG_PASSWORD.encode() not in stderr


@pytest.mark.parametrize("module", MODULES)
async def test_a_process_waiting_for_the_broker_stops_on_a_signal_with_status_0(
    engine: AsyncEngine, test_database_url: str, test_broker_url: str, module: str
) -> None:
    async with running_proxy(test_broker_url) as proxy:
        proxy.cut()
        env = {
            "DATABASE_URL": test_database_url,
            "BROKER_URL": proxy.url(test_broker_url),
            "STARTUP_RETRY_INTERVAL_S": "0.1",
        }

        async def retrying(stderr: bytes) -> bool:
            return b"broker not available at start" in stderr

        status, stderr = await stop_with_signal(module, env, retrying, signal.SIGTERM)

    assert status == 0
    assert b"Traceback" not in stderr
