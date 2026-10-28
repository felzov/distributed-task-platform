"""`GET /jobs/{id}` rejects an invalid id before touching the database.

The app points at a closed port, so the expected 422s also show that no query ran.
"""

import uuid

import httpx
import pytest

from task_platform.main import create_app
from tests.app_client import app_client
from tests.settings import UNREACHABLE_DATABASE_URL, make_test_settings

SENTINEL = "s3cr3t-sentinel-7731"


async def get(path: str) -> httpx.Response:
    async with app_client(create_app(make_test_settings(UNREACHABLE_DATABASE_URL))) as client:
        return await client.get(path)


@pytest.mark.parametrize(
    "job_id",
    [
        pytest.param(SENTINEL, id="sentinel"),
        pytest.param("123", id="digits"),
        pytest.param("x" * 100, id="long"),
        pytest.param(str(uuid.uuid4())[:-1], id="one character short"),
    ],
)
async def test_invalid_id_is_rejected_without_echoing_it(job_id: str) -> None:
    response = await get(f"/jobs/{job_id}")

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "request is not valid",
            "details": ["path.job_id: uuid_parsing"],
        }
    }
    assert job_id not in response.text


async def test_unreachable_database_returns_503() -> None:
    response = await get(f"/jobs/{uuid.uuid4()}")

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
