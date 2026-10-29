"""Database failures map to 503 (try again later) or 500, and log no request data.

No database here: the closed port gives a real refused connection, and a test-only route raises
the other error shapes as SQLAlchemy delivers them. The real failures against PostgreSQL are in
tests/integration/test_submit_job_failures.py.
"""

import logging

import httpx
import pytest
import sqlalchemy.exc

from task_platform.main import create_app
from tests.app_client import app_client
from tests.settings import UNREACHABLE_DATABASE_URL, make_test_settings

SENTINEL = "s3cr3t-sentinel-7731"

JOB = {
    "type": "notification.send",
    "payload_version": 1,
    "payload": {"recipient": "max@example.com", "subject": "Hi", "body": SENTINEL},
}


class FakeDriverError(Exception):
    """Stands in for the asyncpg error SQLAlchemy wraps; only `sqlstate` is read from it."""

    def __init__(self, sqlstate: str | None) -> None:
        # The message is what str(exc) would print: PostgreSQL's DETAIL can quote the row.
        super().__init__(f"Failing row contains ({SENTINEL})")
        self.sqlstate = sqlstate


def db_error(sqlstate: str | None, *, invalidated: bool = False) -> sqlalchemy.exc.DBAPIError:
    return sqlalchemy.exc.DBAPIError(
        "INSERT INTO jobs ...",
        {"payload": SENTINEL},
        FakeDriverError(sqlstate),
        connection_invalidated=invalidated,
    )


def assert_error(response: httpx.Response, status_code: int, code: str) -> None:
    assert response.status_code == status_code
    assert response.json()["error"]["code"] == code
    assert SENTINEL not in response.text


async def test_unreachable_database_returns_503(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    async with app_client(create_app(make_test_settings(UNREACHABLE_DATABASE_URL))) as client:
        response = await client.post("/jobs", json=JOB)

    assert_error(response, 503, "service_unavailable")
    assert "database unavailable: builtins.ConnectionRefusedError" in caplog.text
    assert SENTINEL not in caplog.text


UNAVAILABLE = "database unavailable: "
FAILED = "database error: "


# The logged line is asserted whole: with tracebacks suppressed it is the only clue an operator
# gets, so e.g. a pool timeout and a command timeout must not read the same.
@pytest.mark.parametrize(
    ("error", "status_code", "code", "logged"),
    [
        pytest.param(
            db_error("08006"),
            503,
            "service_unavailable",
            UNAVAILABLE + "sqlalchemy.exc.DBAPIError (sqlstate 08006)",
            id="connection",
        ),
        pytest.param(
            db_error("53300"),
            503,
            "service_unavailable",
            UNAVAILABLE + "sqlalchemy.exc.DBAPIError (sqlstate 53300)",
            id="too many",
        ),
        pytest.param(
            db_error("57014"),
            503,
            "service_unavailable",
            UNAVAILABLE + "sqlalchemy.exc.DBAPIError (sqlstate 57014)",
            id="query canceled",
        ),
        pytest.param(
            db_error("57P01"),
            503,
            "service_unavailable",
            UNAVAILABLE + "sqlalchemy.exc.DBAPIError (sqlstate 57P01)",
            id="admin shutdown",
        ),
        pytest.param(
            db_error(None, invalidated=True),
            503,
            "service_unavailable",
            UNAVAILABLE + "sqlalchemy.exc.DBAPIError (sqlstate None)",
            id="connection invalidated",
        ),
        pytest.param(
            sqlalchemy.exc.TimeoutError(),
            503,
            "service_unavailable",
            UNAVAILABLE + "sqlalchemy.exc.TimeoutError (sqlstate None)",
            id="pool",
        ),
        pytest.param(
            TimeoutError(),
            503,
            "service_unavailable",
            UNAVAILABLE + "builtins.TimeoutError",
            id="command",
        ),
        pytest.param(
            db_error("23514"),
            500,
            "internal_error",
            FAILED + "sqlalchemy.exc.DBAPIError (sqlstate 23514, constraint None)",
            id="check violation",
        ),
        pytest.param(
            db_error(None),
            500,
            "internal_error",
            FAILED + "sqlalchemy.exc.DBAPIError (sqlstate None, constraint None)",
            id="no sqlstate",
        ),
        pytest.param(
            sqlalchemy.exc.NoResultFound(),
            500,
            "internal_error",
            FAILED + "sqlalchemy.exc.NoResultFound (sqlstate None, constraint None)",
            id="orm",
        ),
    ],
)
async def test_database_errors_are_classified_and_logged_safely(
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    status_code: int,
    code: str,
    logged: str,
) -> None:
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))

    async def raises() -> None:
        raise error

    app.add_api_route("/raises", raises)
    caplog.set_level(logging.DEBUG)
    # ASGITransport re-raises what the app re-raises: getting a response at all means the error
    # was handled here and not passed on to the server's traceback logging.
    async with app_client(app) as client:
        response = await client.get("/raises")

    assert_error(response, status_code, code)
    assert logged in caplog.text
    assert SENTINEL not in caplog.text
