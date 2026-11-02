"""How a failed claim is classified (ADR 0004 decision 6). The real database produces class 22,
lock-wait timeouts and statement timeouts in tests/integration/test_execution.py; these are the
errors it cannot easily be made to raise."""

import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.services.execution import Outcome, execute_job
from tests.settings import (
    UNREACHABLE_BROKER_URL,
    UNREACHABLE_DATABASE_URL,
    make_test_broker_settings,
)

SETTINGS = make_test_broker_settings(UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL)


class DriverError(Exception):
    """Stands in for the asyncpg exception SQLAlchemy wraps (exc.orig)."""

    def __init__(self, sqlstate: str) -> None:
        super().__init__("DETAIL: Key (payload)=(secret) ...")
        self.sqlstate = sqlstate


def failing_sessions(exc: BaseException) -> async_sessionmaker[AsyncSession]:
    class Sessions:
        @asynccontextmanager
        async def begin(self) -> AsyncIterator[Any]:
            raise exc
            yield  # pragma: no cover

    return cast(async_sessionmaker[AsyncSession], Sessions())


def wrapped(error_class: type[DBAPIError], sqlstate: str) -> DBAPIError:
    return error_class("UPDATE jobs ...", None, DriverError(sqlstate))


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        pytest.param(wrapped(IntegrityError, "23514"), Outcome.CLAIM_REJECTED, id="23 check"),
        pytest.param(wrapped(DBAPIError, "22008"), Outcome.CLAIM_REJECTED, id="22 as DBAPIError"),
        pytest.param(wrapped(OperationalError, "57014"), Outcome.CLAIM_UNAVAILABLE, id="57 cancel"),
        pytest.param(wrapped(DBAPIError, "40P01"), Outcome.CLAIM_UNAVAILABLE, id="40 deadlock"),
        # A schema this code does not match: fixed by a deploy, so wait for it.
        pytest.param(wrapped(ProgrammingError, "42703"), Outcome.CLAIM_UNAVAILABLE, id="42"),
        pytest.param(ConnectionRefusedError(), Outcome.CLAIM_UNAVAILABLE, id="no database"),
        pytest.param(TimeoutError(), Outcome.CLAIM_UNAVAILABLE, id="client timeout"),
    ],
)
async def test_claim_errors_are_classified_by_sqlstate_class(
    exc: BaseException, expected: Outcome, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    outcome = await execute_job(failing_sessions(exc), uuid.uuid4(), {}, SETTINGS)

    assert outcome == expected
    level = logging.ERROR if expected == Outcome.CLAIM_REJECTED else logging.WARNING
    assert [record.levelno for record in caplog.records] == [level]
    # The driver's message can quote the row; only the class and SQLSTATE are logged.
    assert "secret" not in caplog.text
