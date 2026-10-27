"""HTTP errors from routing (unknown path, wrong method) or any HTTPException use the envelope."""

import logging

import httpx
import pytest
from fastapi import HTTPException
from starlette.types import Message, Scope

from task_platform.main import create_app
from tests.app_client import app_client
from tests.settings import UNREACHABLE_DATABASE_URL, make_test_settings

SENTINEL = "s3cr3t-sentinel-7731"


async def request(method: str, path: str) -> httpx.Response:
    async with app_client(create_app(make_test_settings(UNREACHABLE_DATABASE_URL))) as client:
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


@pytest.mark.parametrize(
    ("status_code", "code", "message"),
    [
        (400, "invalid_request", "bad request"),
        # Not in http.HTTPStatus (nginx's "client closed request"), so there is no phrase.
        (499, "invalid_request", "client error"),
        # Same code as a database outage, so clients treat both as "try again later".
        (503, "service_unavailable", "service unavailable"),
    ],
)
async def test_other_http_exceptions_use_the_envelope_without_their_detail(
    status_code: int, code: str, message: str
) -> None:
    # A route added only for this test: other code may raise HTTPException with any detail.
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))

    async def raises() -> None:
        raise HTTPException(status_code=status_code, detail=SENTINEL)

    app.add_api_route("/raises", raises)
    async with app_client(app) as client:
        response = await client.get("/raises")

    assert response.status_code == status_code
    assert response.json() == {"error": {"code": code, "message": message, "details": []}}
    assert SENTINEL not in response.text


async def test_client_disconnect_mid_body_is_not_a_server_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Called as raw ASGI: httpx cannot hang up halfway through a body.
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))
    incoming: list[Message] = [
        {"type": "http.request", "body": b'{"type": ', "more_body": True},
        {"type": "http.disconnect"},
    ]
    sent: list[Message] = []

    async def receive() -> Message:
        return incoming.pop(0)

    async def send(message: Message) -> None:
        sent.append(message)

    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/jobs",
        "raw_path": b"/jobs",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
        "server": ("test", 80),
        "client": ("127.0.0.1", 50000),
    }
    caplog.set_level(logging.INFO)
    try:
        # The generic 500 path re-raises to the server, which would fail the test right here.
        await app(scope, receive, send)
    finally:
        await app.state.engine.dispose()

    assert sent[0]["status"] == 400
    assert "client disconnected before the request body was read" in caplog.text
