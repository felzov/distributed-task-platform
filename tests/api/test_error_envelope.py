"""HTTP errors from routing (unknown path, wrong method) or any HTTPException use the envelope."""

import httpx
from fastapi import HTTPException

from task_platform.main import create_app
from tests.app_client import app_client
from tests.settings import make_test_settings

UNREACHABLE_DATABASE_URL = "postgresql+asyncpg://task_platform:unused@127.0.0.1:1/unused_test"
SENTINEL = "s3cr3t-sentinel-7731"


async def request(method: str, path: str) -> httpx.Response:
    async with app_client(make_test_settings(UNREACHABLE_DATABASE_URL)) as client:
        return await client.request(method, path)


async def test_unknown_path_returns_the_envelope() -> None:
    response = await request("GET", "/no-such-path")
    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "not_found", "message": "not found", "details": []}
    }


async def test_wrong_method_returns_the_envelope_and_keeps_allow() -> None:
    response = await request("GET", "/jobs")
    assert response.status_code == 405
    assert response.json() == {
        "error": {"code": "method_not_allowed", "message": "method not allowed", "details": []}
    }
    # RFC 9110 requires Allow on a 405; the handler must keep the exception's headers.
    assert response.headers["allow"] == "POST"


async def test_other_http_exceptions_use_the_envelope_without_their_detail() -> None:
    # A route added only for this test: other code may raise HTTPException with any detail.
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))

    async def raises() -> None:
        raise HTTPException(status_code=400, detail=SENTINEL)

    app.add_api_route("/raises", raises)
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/raises")
    finally:
        await app.state.engine.dispose()

    assert response.status_code == 400
    assert response.json() == {
        "error": {"code": "invalid_request", "message": "bad request", "details": []}
    }
    assert SENTINEL not in response.text
