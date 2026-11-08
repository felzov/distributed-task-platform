"""`GET /jobs?status=failed` against a real PostgreSQL: failed jobs, newest first, page by page,
each shown as `GET /jobs/{id}` shows it, never with its payload."""

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.config import Settings
from task_platform.domain.failures import FAILURE_MESSAGES, FailureCode
from task_platform.main import create_app
from tests.app_client import app_client
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"
PAYLOAD = json.dumps({"recipient": "max@example.com", "subject": "Hi", "body": SENTINEL})
FAILED_AT = datetime(2026, 1, 1, tzinfo=UTC)
MAX_PAGES = 10


async def seed_failed(
    engine: AsyncEngine, failed_at: datetime, code: str = FailureCode.HANDLER_ERROR
) -> str:
    row = await insert_job(
        engine,
        payload=PAYLOAD,
        status="failed",
        attempts=5,
        error_code=code,
        updated_at=failed_at,
    )
    return str(row["id"])


async def get_pages(client: httpx.AsyncClient, limit: int) -> list[dict[str, Any]]:
    """Every page, following next_cursor until it is null."""
    pages: list[dict[str, Any]] = []
    query = f"/jobs?status=failed&limit={limit}"
    # Bounded: a cursor that is ignored would return the same page forever.
    for _ in range(MAX_PAGES):
        response = await client.get(query)
        assert response.status_code == 200
        assert SENTINEL not in response.text
        body: dict[str, Any] = response.json()
        pages.append(body)
        if body["next_cursor"] is None:
            return pages
        query = f"/jobs?status=failed&limit={limit}&cursor={body['next_cursor']}"
    pytest.fail(f"still a next_cursor after {MAX_PAGES} pages")


async def test_failed_jobs_are_listed_newest_first_as_get_job_shows_them(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    older = await seed_failed(engine, FAILED_AT, FailureCode.HANDLER_TIMEOUT)
    newer = await seed_failed(engine, FAILED_AT + timedelta(minutes=1))
    await insert_job(engine, payload=PAYLOAD)
    await insert_job(engine, payload=PAYLOAD, status="succeeded", attempts=1, result="{}")

    async with app_client(create_app(test_settings)) as client:
        response = await client.get("/jobs?status=failed")
        singles = [(await client.get(f"/jobs/{job_id}")).json() for job_id in (newer, older)]

    assert response.status_code == 200
    assert SENTINEL not in response.text
    body = response.json()
    assert body["next_cursor"] is None
    assert body["jobs"] == singles
    assert [job["failure"] for job in body["jobs"]] == [
        {"code": "handler_error", "message": FAILURE_MESSAGES[FailureCode.HANDLER_ERROR]},
        {"code": "handler_timeout", "message": FAILURE_MESSAGES[FailureCode.HANDLER_TIMEOUT]},
    ]


async def test_pages_cover_every_failed_job_once(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    # Three jobs failed in the same microsecond, so page boundaries fall between equal times.
    same_time = [await seed_failed(engine, FAILED_AT) for _ in range(3)]
    newest = await seed_failed(engine, FAILED_AT + timedelta(seconds=1))
    oldest = await seed_failed(engine, FAILED_AT - timedelta(seconds=1))

    async with app_client(create_app(test_settings)) as client:
        pages = await get_pages(client, limit=2)

    assert [len(page["jobs"]) for page in pages] == [2, 2, 1]
    listed = [job["id"] for page in pages for job in page["jobs"]]
    by_id = sorted(same_time, key=uuid.UUID, reverse=True)
    assert listed == [newest, *by_id, oldest]


async def test_a_full_last_page_has_no_cursor(engine: AsyncEngine, test_settings: Settings) -> None:
    # Exactly one page of jobs: the API must not send a cursor that leads to an empty page.
    for minute in range(2):
        await seed_failed(engine, FAILED_AT + timedelta(minutes=minute))

    async with app_client(create_app(test_settings)) as client:
        pages = await get_pages(client, limit=2)

    assert [(len(page["jobs"]), page["next_cursor"]) for page in pages] == [(2, None)]


async def test_the_default_page_holds_50_jobs(engine: AsyncEngine, test_settings: Settings) -> None:
    for second in range(51):
        await seed_failed(engine, FAILED_AT + timedelta(seconds=second))

    async with app_client(create_app(test_settings)) as client:
        first = (await client.get("/jobs?status=failed")).json()
        rest = (await client.get(f"/jobs?status=failed&cursor={first['next_cursor']}")).json()

    assert len(first["jobs"]) == 50
    assert first["next_cursor"] is not None
    assert len(rest["jobs"]) == 1
    assert rest["next_cursor"] is None


async def test_no_failed_jobs_is_an_empty_page(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    await insert_job(engine, payload=PAYLOAD)

    async with app_client(create_app(test_settings)) as client:
        response = await client.get("/jobs?status=failed")

    assert response.status_code == 200
    assert response.json() == {"jobs": [], "next_cursor": None}
