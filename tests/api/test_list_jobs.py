"""`GET /jobs?status=failed` rejects bad query values before touching the database.

The app points at a closed port, so the expected 422s also show that no query ran.
"""

import base64
import uuid
from datetime import UTC, datetime

import httpx
import pytest

from task_platform.api.cursors import encode_cursor
from task_platform.main import create_app
from tests.app_client import app_client
from tests.settings import UNREACHABLE_DATABASE_URL, make_test_settings

SENTINEL = "s3cr3t-sentinel-7731"
CURSOR = encode_cursor(datetime(2026, 1, 1, tzinfo=UTC), uuid.uuid4())
# A well-formed cursor whose time cannot be converted to UTC (0001-01-01 at +14:00).
OUT_OF_RANGE = (
    base64.urlsafe_b64encode(f"0001-01-01T00:00:00+14:00_{uuid.uuid4()}".encode())
    .rstrip(b"=")
    .decode()
)


async def get(query: str) -> httpx.Response:
    async with app_client(create_app(make_test_settings(UNREACHABLE_DATABASE_URL))) as client:
        return await client.get(f"/jobs{query}")


@pytest.mark.parametrize(
    ("query", "detail"),
    [
        pytest.param("", "query.status: missing", id="no status"),
        pytest.param("?status=queued", "query.status: literal_error", id="other status"),
        pytest.param(f"?status={SENTINEL}", "query.status: literal_error", id="sentinel status"),
        pytest.param("?status=failed&limit=0", "query.limit: greater_than_equal", id="limit 0"),
        pytest.param("?status=failed&limit=101", "query.limit: less_than_equal", id="limit 101"),
        pytest.param(f"?status=failed&limit={SENTINEL}", "query.limit: int_parsing", id="limit"),
        pytest.param(f"?status=failed&cursor={SENTINEL}", "query.cursor: invalid_cursor", id="cur"),
        pytest.param(
            f"?status=failed&cursor={OUT_OF_RANGE}",
            "query.cursor: invalid_cursor",
            id="time outside utc range",
        ),
        pytest.param(
            f"?status=failed&cursor={SENTINEL * 11}",  # 220 characters, over 200
            "query.cursor: string_too_long",
            id="long cursor",
        ),
    ],
)
async def test_invalid_query_is_rejected_without_echoing_it(query: str, detail: str) -> None:
    response = await get(query)

    assert response.status_code == 422
    assert response.json() == {
        "error": {"code": "invalid_request", "message": "request is not valid", "details": [detail]}
    }
    assert SENTINEL not in response.text


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("?status=failed", id="defaults"),
        pytest.param("?status=failed&limit=1", id="limit 1"),
        pytest.param("?status=failed&limit=100", id="limit 100"),
        pytest.param(f"?status=failed&cursor={CURSOR}", id="cursor"),
    ],
)
async def test_valid_query_reaches_the_database(query: str) -> None:
    response = await get(query)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
