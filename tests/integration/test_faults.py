"""The fault helper itself (phase 9 criterion 6): a matching update pauses, a killed client's
paused update rolls back once released, and nothing of the helper is left afterwards."""

import asyncio
import os
import signal
import sys

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.faults import KEY, TRIGGER, paused_update
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

PAUSE_WHEN = "NEW.status = 'running'"
# A client of its own, to be killed while paused. Not the application: only the database matters.
CLIENT = """
import asyncio, os, asyncpg

async def main() -> None:
    connection = await asyncpg.connect(os.environ["CLIENT_DSN"])
    async with connection.transaction():
        await connection.execute(
            "UPDATE jobs SET status = 'running', attempts = 1,"
            " lease_expires_at = now() + interval '1 minute' WHERE id = $1",
            os.environ["CLIENT_JOB_ID"],
        )

asyncio.run(main())
"""


async def status(engine: AsyncEngine, job_id: object) -> str:
    async with engine.connect() as connection:
        value: str = await connection.scalar(
            text("SELECT status FROM jobs WHERE id = :id"), {"id": job_id}
        )
        return value


async def lockable(engine: AsyncEngine, job_id: object) -> bool:
    async with engine.begin() as connection:
        found = await connection.scalar(
            text("SELECT id FROM jobs WHERE id = :id FOR UPDATE SKIP LOCKED"), {"id": job_id}
        )
        return found is not None


async def assert_nothing_left(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        triggers = await connection.scalar(
            text("SELECT count(*) FROM pg_trigger WHERE tgname = :name"), {"name": TRIGGER}
        )
        functions = await connection.scalar(
            text("SELECT count(*) FROM pg_proc WHERE proname = :name"), {"name": TRIGGER}
        )
        locks = await connection.scalar(
            text(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'"
                " AND classid = 0 AND objid = :key"
            ),
            {"key": KEY},
        )
    assert (triggers, functions, locks) == (0, 0, 0)


async def run_update(engine: AsyncEngine, job_id: object) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "UPDATE jobs SET status = 'running', attempts = 1,"
                " lease_expires_at = now() + interval '1 minute' WHERE id = :id"
            ),
            {"id": job_id},
        )


async def test_a_matching_update_waits_until_released_and_then_commits(
    engine: AsyncEngine,
) -> None:
    paused_id = (await insert_job(engine))["id"]
    other_id = (await insert_job(engine))["id"]
    update: asyncio.Task[None] | None = None
    try:
        async with paused_update(engine, PAUSE_WHEN) as pause:
            update = asyncio.create_task(run_update(engine, paused_id))
            await pause.wait_for_pause()
            if update.done():
                update.result()  # its own error, if it failed instead of pausing
            assert not update.done()
            assert not await lockable(engine, paused_id)
            # An update the condition does not match goes through at once.
            async with asyncio.timeout(2), engine.begin() as connection:
                await connection.execute(
                    text("UPDATE jobs SET max_attempts = 3 WHERE id = :id"), {"id": other_id}
                )
            await pause.release()
            async with asyncio.timeout(5):
                await update
    finally:
        # A failure before the release leaves it pending: never let it outlive the test.
        if update is not None and not update.done():
            update.cancel()
            await asyncio.gather(update, return_exceptions=True)

    assert await status(engine, paused_id) == "running"
    async with engine.connect() as connection:
        other_max = await connection.scalar(
            text("SELECT max_attempts FROM jobs WHERE id = :id"), {"id": other_id}
        )
    assert other_max == 3
    await assert_nothing_left(engine)


async def test_a_killed_clients_paused_update_rolls_back_and_frees_the_row(
    engine: AsyncEngine,
) -> None:
    job_id = (await insert_job(engine))["id"]
    # From the engine, not the test_database_url fixture: pytest prints a failing test's
    # arguments, and the URL holds the password.
    url = engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    env = os.environ | {"CLIENT_DSN": url, "CLIENT_JOB_ID": str(job_id)}
    async with paused_update(engine, PAUSE_WHEN) as pause:
        client = await asyncio.create_subprocess_exec(
            sys.executable, "-c", CLIENT, env=env, stderr=asyncio.subprocess.DEVNULL
        )
        try:
            pid = await pause.wait_for_pause(child=client)
            client.send_signal(signal.SIGKILL)
            await client.wait()
            # The backend outlives its client while it waits, and keeps the row locked.
            async with engine.connect() as connection:
                alive = await connection.scalar(
                    text("SELECT count(*) FROM pg_stat_activity WHERE pid = :pid"), {"pid": pid}
                )
            assert alive == 1
            assert not await lockable(engine, job_id)
        finally:
            if client.returncode is None:
                client.kill()
                await client.wait()
    # Leaving the block released the lock and waited for the backend to end.
    async with engine.connect() as connection:
        alive = await connection.scalar(
            text("SELECT count(*) FROM pg_stat_activity WHERE pid = :pid"), {"pid": pid}
        )
    assert alive == 0
    assert await status(engine, job_id) == "queued"
    assert await lockable(engine, job_id)
    await assert_nothing_left(engine)


async def test_no_pause_times_out_and_still_cleans_up(engine: AsyncEngine) -> None:
    job_id = (await insert_job(engine))["id"]
    with pytest.raises(AssertionError, match="no backend paused in a matching update"):
        async with paused_update(engine, PAUSE_WHEN) as pause:
            await pause.wait_for_pause(timeout_s=0.2)

    await assert_nothing_left(engine)
    # Without the trigger, the same update no longer waits.
    async with asyncio.timeout(5):
        await run_update(engine, job_id)
    assert await status(engine, job_id) == "running"
