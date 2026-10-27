"""`POST /jobs` rejects bad requests before touching the database.

The app here points at a closed port, so any database access would fail the request with a
different error: every expected 4xx also shows that validation ran first.
"""

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from starlette.requests import Request
from starlette.types import Message

from task_platform.api.errors import ApiError, ApiErrorCode
from task_platform.api.jobs import read_capped_body
from task_platform.config import Settings
from tests.app_client import app_client
from tests.settings import make_test_settings

UNREACHABLE_DATABASE_URL = "postgresql+asyncpg://task_platform:unused@127.0.0.1:1/unused_test"
SENTINEL = "s3cr3t-sentinel-7731"


def settings(**overrides: object) -> Settings:
    return make_test_settings(UNREACHABLE_DATABASE_URL, **overrides)


def envelope(**overrides: Any) -> dict[str, Any]:
    return {
        "type": "notification.send",
        "payload_version": 1,
        "payload": {"recipient": "max@example.com", "subject": "Hi", "body": SENTINEL},
    } | overrides


def encode(body: object) -> bytes:
    return json.dumps(body).encode()


async def post(content: bytes, **overrides: object) -> httpx.Response:
    async with app_client(settings(**overrides)) as client:
        return await client.post(
            "/jobs", content=content, headers={"content-type": "application/json"}
        )


def assert_error(response: httpx.Response, status_code: int, code: str) -> list[str]:
    assert response.status_code == status_code
    assert SENTINEL not in response.text
    body = response.json()
    assert set(body) == {"error"}
    assert set(body["error"]) == {"code", "message", "details"}
    assert body["error"]["code"] == code
    details: list[str] = body["error"]["details"]
    return details


# Invalid input


@pytest.mark.parametrize(
    ("content", "status_code", "code", "details"),
    [
        pytest.param(
            encode(envelope(type=SENTINEL)), 422, "unknown_job_type", [], id="unknown type"
        ),
        pytest.param(
            encode(envelope(payload_version=2)),
            422,
            "unsupported_payload_version",
            [],
            id="unsupported version",
        ),
        pytest.param(
            encode(envelope(payload={"recipient": "max@example.com", "subject": SENTINEL * 20})),
            422,
            "invalid_payload",
            ["subject: string_too_long", "body: missing"],
            id="schema violation",
        ),
        pytest.param(
            encode(envelope(**{SENTINEL: 1})),
            422,
            "invalid_request",
            ["unexpected field"],
            id="extra envelope field",
        ),
        pytest.param(
            encode({"type": SENTINEL, "payload": {}}),
            422,
            "invalid_request",
            ["payload_version: missing"],
            id="missing envelope field",
        ),
        pytest.param(
            encode(envelope(payload=SENTINEL)),
            422,
            "invalid_request",
            ["payload: dict_type"],
            id="payload not an object",
        ),
        pytest.param(
            f'{{"type": "{SENTINEL}"'.encode(),
            422,
            "invalid_request",
            ["body: json_invalid"],
            id="malformed JSON",
        ),
        pytest.param(b"", 422, "invalid_request", ["body: json_invalid"], id="empty body"),
        pytest.param(
            encode([SENTINEL]), 422, "invalid_request", ["body: model_type"], id="not an object"
        ),
    ],
)
async def test_invalid_requests_are_rejected_without_echoing_input(
    content: bytes, status_code: int, code: str, details: list[str]
) -> None:
    response = await post(content)
    assert assert_error(response, status_code, code) == details


@pytest.mark.parametrize("version", [True, 1.0, "1"], ids=["bool", "float", "string"])
async def test_version_must_be_a_json_integer(version: object) -> None:
    # Each of these would select version 1 under lax validation (True == 1 == 1.0).
    response = await post(encode(envelope(payload_version=version)))
    [detail] = assert_error(response, 422, "invalid_request")
    assert detail.startswith("payload_version: ")


@pytest.mark.parametrize(
    ("depth", "code", "detail"),
    [
        # Under pydantic-core's JSON nesting limit: parsed, measured by the domain's json.dumps,
        # then refused by the payload schema.
        (150, "invalid_payload", "unexpected field"),
        # Over it: refused by the parser, never handed to the domain.
        (5_000, "invalid_request", "body: json_invalid"),
    ],
)
async def test_deeply_nested_json_is_rejected_not_a_server_error(
    depth: int, code: str, detail: str
) -> None:
    nested = "[" * depth + "]" * depth
    content = f'{{"type": "notification.send", "payload_version": 1, "payload": {{"a": {nested}}}}}'
    response = await post(content.encode())
    assert detail in assert_error(response, 422, code)


# Size limits (small values so the bodies stay readable)

LIMITS = {"max_payload_bytes": 1_000, "max_request_body_bytes": 2_000}


async def test_body_over_limit_with_content_length_is_rejected() -> None:
    response = await post(b"x" * 2_001, **LIMITS)
    details = assert_error(response, 413, "request_too_large")
    assert details == []


def raw_request(content_length: str, body: bytes | None) -> Request:
    """A request with the given Content-Length; `body=None` fails the test if it is read."""

    async def receive() -> Message:
        if body is None:
            raise AssertionError("the body was read")
        return {"type": "http.request", "body": body, "more_body": False}

    headers = [(b"content-length", content_length.encode())]
    return Request({"type": "http", "method": "POST", "path": "/jobs", "headers": headers}, receive)


async def test_oversized_content_length_is_rejected_without_reading_the_body() -> None:
    with pytest.raises(ApiError) as exc_info:
        await read_capped_body(raw_request("2001", body=None), max_bytes=2_000)
    assert exc_info.value.code is ApiErrorCode.REQUEST_TOO_LARGE


@pytest.mark.parametrize("content_length", ["abc", "", "-1"])
async def test_unusable_content_length_falls_back_to_counting(content_length: str) -> None:
    # The header is only a shortcut: a value that says nothing must not skip the counted read.
    assert await read_capped_body(raw_request(content_length, b"ok"), max_bytes=2_000) == b"ok"
    with pytest.raises(ApiError):
        await read_capped_body(raw_request(content_length, b"x" * 2_001), max_bytes=2_000)


async def test_body_over_limit_is_rejected_without_content_length() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(3):
            yield b" " * 1_000

    async with app_client(settings(**LIMITS)) as client:
        request = client.build_request("POST", "/jobs", content=chunks())
        # A streamed body is sent chunked, so only the counted read can catch it.
        assert "content-length" not in request.headers
        response = await client.send(request)
    assert_error(response, 413, "request_too_large")


async def test_body_at_limit_is_read() -> None:
    content = encode(envelope(type="unknown"))
    padded = content + b" " * (LIMITS["max_request_body_bytes"] - len(content))
    response = await post(padded, **LIMITS)
    # Past the size check: the type is what fails.
    assert_error(response, 422, "unknown_job_type")


async def test_payload_over_limit_is_rejected_even_if_body_is_under_its_limit() -> None:
    payload = {"recipient": "max@example.com", "subject": "Hi", "body": SENTINEL * 60}
    content = encode(envelope(payload=payload))
    assert len(content) < LIMITS["max_request_body_bytes"]
    response = await post(content, **LIMITS)
    assert_error(response, 413, "payload_too_large")
