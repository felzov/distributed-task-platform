"""`/health` and `/ready` without a database (closed port)."""

import logging

import httpx
import pytest

from task_platform.main import create_app
from tests.app_client import app_client
from tests.settings import UNREACHABLE_DATABASE_URL, make_test_settings


async def get(path: str) -> httpx.Response:
    async with app_client(create_app(make_test_settings(UNREACHABLE_DATABASE_URL))) as client:
        return await client.get(path)


async def test_health_does_not_need_the_database() -> None:
    response = await get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_ready_fails_on_the_database_check(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    response = await get("/ready")

    assert response.status_code == 503
    assert response.json() == {
        "error": {
            "code": "service_unavailable",
            "message": "service not ready",
            "details": ["database"],
        }
    }
    expected = (
        "not ready: database (builtins.ConnectionRefusedError, sqlstate None, pool 0/5 in use)"
    )
    assert expected in caplog.text
