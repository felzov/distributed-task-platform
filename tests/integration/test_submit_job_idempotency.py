"""`POST /jobs` with an Idempotency-Key against a real PostgreSQL (ADR 0007, decisions 2-7)."""

import asyncio
import json
import logging
import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.config import Settings
from task_platform.domain.idempotency import request_fingerprint
from task_platform.main import create_app
from tests.app_client import app_client
from tests.settings import make_test_settings

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"
# The key holds the sentinel too: it must never come back in a response or a log line.
KEY = f"key-{SENTINEL}"
PAYLOAD = {"recipient": "max@example.com", "subject": "Hi", "body": "Hello"}
JOB = {"type": "notification.send", "payload_version": 1, "payload": PAYLOAD}


async def post(
    client: httpx.AsyncClient, body: object = JOB, key: str | None = KEY
) -> httpx.Response:
    headers = {} if key is None else {"Idempotency-Key": key}
    return await client.post("/jobs", json=body, headers=headers)


async def job_rows(engine: AsyncEngine) -> list[dict[str, Any]]:
    async with engine.connect() as connection:
        result = await connection.execute(
            text("SELECT id, status, payload, idempotency_key, request_hash FROM jobs")
        )
        return [dict(row) for row in result.mappings()]


# Normal path


async def test_a_new_key_saves_the_job_with_its_key_and_fingerprint(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    async with app_client(create_app(test_settings)) as client:
        response = await post(client)

    assert response.status_code == 202
    job_id = uuid.UUID(response.json()["id"])
    assert response.json() == {"id": str(job_id), "status": "queued"}
    assert response.headers["location"] == f"/jobs/{job_id}"
    assert await job_rows(engine) == [
        {
            "id": job_id,
            "status": "queued",
            "payload": PAYLOAD,
            "idempotency_key": KEY,
            "request_hash": request_fingerprint("notification.send", 1, PAYLOAD),
        }
    ]


async def test_a_retry_with_the_same_key_returns_the_first_job(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    async with app_client(create_app(test_settings)) as client:
        first = await post(client)
        retry = await post(client)

    # 200, not a second 202: nothing new was saved.
    assert (first.status_code, retry.status_code) == (202, 200)
    assert retry.json() == first.json()
    assert retry.headers["location"] == first.headers["location"]
    assert len(await job_rows(engine)) == 1


async def test_a_retry_returns_the_jobs_current_status(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    async with app_client(create_app(test_settings)) as client:
        first = await post(client)
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE jobs SET status = 'succeeded', result = '{}' WHERE id = :id"),
                {"id": first.json()["id"]},
            )
        retry = await post(client)

    assert retry.status_code == 200
    assert retry.json() == {"id": first.json()["id"], "status": "succeeded"}


async def test_whitespace_and_key_order_in_the_body_do_not_matter(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    reordered = (
        '{ "payload": {"body": "Hello", "subject": "Hi", "recipient": "max@example.com"},\n'
        '  "payload_version": 1,   "type": "notification.send" }'
    )
    async with app_client(create_app(test_settings)) as client:
        first = await post(client)
        retry = await client.post(
            "/jobs",
            content=reordered.encode(),
            headers={"Idempotency-Key": KEY, "content-type": "application/json"},
        )

    assert retry.status_code == 200
    assert retry.json()["id"] == first.json()["id"]
    assert len(await job_rows(engine)) == 1


async def test_requests_without_a_key_and_with_other_keys_get_jobs_of_their_own(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    async with app_client(create_app(test_settings)) as client:
        responses = [
            await post(client, key=None),
            await post(client, key=None),
            await post(client, key="key-1"),
            await post(client, key="key-2"),
        ]

    assert [response.status_code for response in responses] == [202] * 4
    assert len({response.json()["id"] for response in responses}) == 4
    assert len(await job_rows(engine)) == 4


# Invalid input


@pytest.mark.parametrize(
    "changed",
    [
        pytest.param({"payload": PAYLOAD | {"body": SENTINEL}}, id="payload value"),
        pytest.param({"payload": PAYLOAD | {"recipient": "other@example.com"}}, id="recipient"),
    ],
)
async def test_a_different_request_under_a_used_key_is_refused(
    engine: AsyncEngine, test_settings: Settings, changed: dict[str, Any]
) -> None:
    # Another type or payload version would fail validation first: notification.send v1 is the
    # only registered pair, so they are covered by the fingerprint's unit tests.
    async with app_client(create_app(test_settings)) as client:
        first = await post(client)
        reused = await post(client, body=JOB | changed)

    assert reused.status_code == 422
    assert reused.json() == {
        "error": {
            "code": "idempotency_key_reused",
            "message": "idempotency key was already used for a different request",
            "details": [],
        }
    }
    assert SENTINEL not in reused.text
    rows = await job_rows(engine)
    assert [(row["id"], row["payload"]) for row in rows] == [
        (uuid.UUID(first.json()["id"]), PAYLOAD)
    ]


async def test_a_refused_request_does_not_use_its_key(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    # ADR 0007 decision 7: every check runs before the database, so only a valid request is saved
    # under a key, and the corrected request then creates the job.
    invalid = JOB | {"payload": {"recipient": "max@example.com"}}
    async with app_client(create_app(test_settings)) as client:
        refused = await post(client, body=invalid)
        rows_after_refusal = await job_rows(engine)
        corrected = await post(client)

    assert refused.status_code == 422
    assert refused.json()["error"]["code"] == "invalid_payload"
    assert rows_after_refusal == []
    assert corrected.status_code == 202


async def test_keys_are_never_logged(
    engine: AsyncEngine, test_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    # DEBUG also turns on SQLAlchemy's own logging of statements and result rows; the engine hides
    # bound parameters, and the key is never a selected column.
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="sqlalchemy")
    async with app_client(create_app(test_settings)) as client:
        statuses = [
            (await post(client)).status_code,
            (await post(client)).status_code,
            (await post(client, body=JOB | {"payload": PAYLOAD | {"body": "Other"}})).status_code,
        ]

    assert statuses == [202, 200, 422]
    assert "INSERT INTO jobs" in caplog.text
    assert SENTINEL not in caplog.text


# Failure path: concurrent requests with one key


async def test_concurrent_requests_with_one_key_save_one_job(
    engine: AsyncEngine, test_database_url: str
) -> None:
    # A pool as large as the number of requests, so all of them can reach the database at once.
    requests = 10
    settings = make_test_settings(test_database_url, db_pool_size=requests)
    async with app_client(create_app(settings)) as client:
        responses = await asyncio.gather(*(post(client) for _ in range(requests)))

    assert sorted(response.status_code for response in responses) == [200] * 9 + [202]
    assert len({json.dumps(response.json()) for response in responses}) == 1
    assert len(await job_rows(engine)) == 1


async def test_concurrent_requests_with_one_key_and_two_bodies_save_only_the_first(
    engine: AsyncEngine, test_database_url: str
) -> None:
    # A request that waited for another one's COMMIT still compares fingerprints: whichever body
    # was saved first, the other body's requests get 422, never that job.
    other = JOB | {"payload": PAYLOAD | {"recipient": "other@example.com"}}
    bodies = [JOB, other] * 5
    settings = make_test_settings(test_database_url, db_pool_size=len(bodies))
    async with app_client(create_app(settings)) as client:
        responses = await asyncio.gather(*(post(client, body=body) for body in bodies))

    [saved] = await job_rows(engine)
    winner = JOB if saved["payload"] == PAYLOAD else other
    for body, response in zip(bodies, responses, strict=True):
        if body is winner:
            assert response.status_code in (200, 202)
            assert response.json()["id"] == str(saved["id"])
        else:
            assert response.status_code == 422
            assert response.json()["error"]["code"] == "idempotency_key_reused"
    assert sorted(response.status_code for response in responses).count(202) == 1
