"""`/ready` against a real PostgreSQL: migrated, at another revision, refusing, slow, pool full."""

import functools
import logging
import time

import pytest
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.api import health as health_api
from task_platform.config import Settings
from task_platform.data.db import create_db_engine
from task_platform.data.schema import SCHEMA_REVISION, current_revisions
from task_platform.main import create_app
from task_platform.services.health import ReadinessCheck, check_readiness
from tests.app_client import app_client
from tests.settings import make_test_settings

pytestmark = pytest.mark.integration


async def test_ready_on_a_migrated_database(engine: AsyncEngine, test_settings: Settings) -> None:
    async with app_client(create_app(test_settings)) as client:
        response = await client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


async def test_not_ready_when_the_database_is_at_another_revision(
    engine: AsyncEngine, test_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    # Code expecting a revision the database lacks: deployed before `make db-migrate` ran. The
    # expected revision is passed in, so the shared test database is left as it is.
    caplog.set_level(logging.WARNING)
    app_engine = create_db_engine(test_settings)
    try:
        failed = await check_readiness(app_engine, timeout_s=2.0, expected_revision="9999")
    finally:
        await app_engine.dispose()

    assert failed is ReadinessCheck.MIGRATIONS
    assert "not ready: migrations" in caplog.text
    # Revision ids stay out of the log, the expected one and the database's own.
    assert "9999" not in caplog.text
    assert SCHEMA_REVISION not in caplog.text


async def test_ready_reports_the_migrations_check(
    engine: AsyncEngine, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The real check against the real database, only expecting a revision it does not have.
    monkeypatch.setattr(
        health_api, "check_readiness", functools.partial(check_readiness, expected_revision="9999")
    )
    async with app_client(create_app(test_settings)) as client:
        response = await client.get("/ready")

    assert response.status_code == 503
    assert response.json()["error"]["details"] == ["migrations"]


async def test_database_that_refuses_the_connection_is_not_ready(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # The server answers but refuses (3D000 here; 28P01 or 53300 take the same path): SQLAlchemy
    # wraps that in DBAPIError, not OSError, so it must still be "database", not a 500.
    caplog.set_level(logging.WARNING)
    unknown = make_url(test_database_url).set(database="does_not_exist_test")
    settings = make_test_settings(unknown.render_as_string(hide_password=False))
    async with app_client(create_app(settings)) as client:
        response = await client.get("/ready")

    assert response.status_code == 503
    assert response.json()["error"]["details"] == ["database"]
    expected = "not ready: database (sqlalchemy.exc.DBAPIError, sqlstate 3D000, pool 0/5 in use)"
    assert expected in caplog.text


async def test_never_migrated_database_has_no_revision(engine: AsyncEngine) -> None:
    # SET LOCAL hides alembic_version for this transaction only, as on a fresh database.
    async with engine.connect() as connection:
        await connection.execute(text("SET LOCAL search_path TO pg_catalog"))
        assert await current_revisions(connection) == ()


async def test_slow_database_fails_within_the_timeout_and_recovers(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    app = create_app(make_test_settings(test_database_url, ready_check_timeout_s=0.5))
    async with app_client(app) as client:
        # Reading alembic_version waits behind this lock until the probe's timeout.
        locker = await engine.connect()
        try:
            await locker.begin()
            await locker.execute(text("LOCK TABLE alembic_version IN ACCESS EXCLUSIVE MODE"))
            started = time.monotonic()
            slow = await client.get("/ready")
            elapsed = time.monotonic() - started
        finally:
            await locker.rollback()
            await locker.close()
        after = await client.get("/ready")

    assert slow.status_code == 503
    assert slow.json()["error"]["details"] == ["database"]
    assert elapsed < 0.5 + 1.0
    # Nothing left checked out: the database, not the pool, was slow.
    expected = "not ready: database (builtins.TimeoutError, sqlstate None, pool 0/5 in use)"
    assert expected in caplog.text
    assert after.status_code == 200


async def test_full_pool_is_told_apart_from_a_slow_database(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # The probe's timeout fires before the pool's own, so both cases end in builtins.TimeoutError;
    # only the pool usage in the log line tells them apart.
    caplog.set_level(logging.WARNING)
    settings = make_test_settings(test_database_url, db_pool_size=1, ready_check_timeout_s=0.3)
    app = create_app(settings)
    async with app_client(app) as client:
        async with app.state.engine.connect() as held:
            await held.execute(text("SELECT 1"))
            response = await client.get("/ready")
        # The cancelled wait for a connection left nothing behind.
        after = await client.get("/ready")

    assert response.status_code == 503
    assert after.status_code == 200
    expected = "not ready: database (builtins.TimeoutError, sqlstate None, pool 1/1 in use)"
    assert expected in caplog.text
