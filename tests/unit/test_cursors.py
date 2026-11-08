"""The job list's cursor: exact round trip, and nothing else accepted."""

import base64
import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest

from task_platform.api.cursors import (
    MAX_CURSOR_LENGTH,
    InvalidCursorError,
    decode_cursor,
    encode_cursor,
)

SENTINEL = "s3cr3t-sentinel-7731"
JOB_ID = uuid.UUID("0f3a2b4c-5d6e-4f70-8192-a3b4c5d6e7f8")
FAILED_AT = datetime(2026, 11, 7, 23, 59, 1, 123456, tzinfo=UTC)


def b64(raw: str) -> str:
    return base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()


@pytest.mark.parametrize(
    "failed_at",
    [
        pytest.param(FAILED_AT, id="utc with microseconds"),
        pytest.param(FAILED_AT.replace(microsecond=0), id="whole second"),
        pytest.param(FAILED_AT.astimezone(timezone(timedelta(hours=2))), id="other offset"),
    ],
)
def test_a_cursor_names_exactly_the_row_it_was_made_from(failed_at: datetime) -> None:
    cursor = encode_cursor(failed_at, JOB_ID)

    assert decode_cursor(cursor) == (failed_at, JOB_ID)
    assert len(cursor) < MAX_CURSOR_LENGTH
    # URL-safe as it is: no padding, no "+" or "/".
    assert set(cursor) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")


VALID = encode_cursor(FAILED_AT, JOB_ID)


@pytest.mark.parametrize(
    "cursor",
    [
        pytest.param("", id="empty"),
        pytest.param(SENTINEL, id="sentinel"),
        pytest.param(b64(SENTINEL), id="base64 of junk"),
        pytest.param(VALID + "=", id="padded"),
        pytest.param(VALID[:-1], id="one character short"),
        pytest.param(VALID.replace("-", "+").replace("_", "/") + "+/", id="standard alphabet"),
        pytest.param("A" * (MAX_CURSOR_LENGTH + 1), id="too long"),
        pytest.param(b64(f"{FAILED_AT.isoformat()}{JOB_ID}"), id="no separator"),
        pytest.param(b64(f"{FAILED_AT.replace(tzinfo=None).isoformat()}_{JOB_ID}"), id="naive"),
        pytest.param(b64(f"{FAILED_AT.isoformat()}_{SENTINEL}"), id="bad id"),
        pytest.param(b64(f"{SENTINEL}_{JOB_ID}"), id="bad time"),
        pytest.param(b64(f"{FAILED_AT.isoformat()}_{JOB_ID}_x"), id="trailing part"),
        pytest.param(base64.urlsafe_b64encode(b"\xff\xfe").decode().rstrip("="), id="not ascii"),
        # Valid times with an offset, but outside the range of a UTC datetime: asyncpg converts
        # to UTC before sending, which would fail as a database error (500) instead of a 422.
        pytest.param(b64(f"0001-01-01T00:00:00+14:00_{JOB_ID}"), id="before year 1 in utc"),
        pytest.param(b64(f"9999-12-31T23:59:00-14:00_{JOB_ID}"), id="after year 9999 in utc"),
    ],
)
def test_anything_else_is_refused_without_echoing_it(cursor: str) -> None:
    with pytest.raises(InvalidCursorError) as exc_info:
        decode_cursor(cursor)

    assert str(exc_info.value) == "cursor is not valid"
    # Nothing chained either: a traceback would print the context.
    assert exc_info.value.__context__ is None
    assert exc_info.value.__cause__ is None
