"""Pausing a process inside one of its `jobs` updates, to crash it at an exact point.

A test-only trigger makes a matching UPDATE on `jobs` wait for an advisory lock the test holds:
the process stops inside its transaction, after everything it committed before. The test can then
kill it there, with no timing involved. PostgreSQL notices the dead client only when the backend
next reads from its socket (with `client_connection_check_interval` at its default, 0), so the
paused backend outlives the kill, holding the row lock (lease recovery's SKIP LOCKED would skip
the job) until the advisory lock is released: its UPDATE then ends, the backend reads EOF and
rolls back (spike on PostgreSQL 18.6, 2026-11-15).
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

# The advisory lock the trigger waits for. Below 2**32, so pg_locks shows it as classid 0, objid
# KEY, objsubid 1 (PostgreSQL documentation, pg_locks).
KEY = 914_200_001
TRIGGER = "test_paused_update"
_WAITERS = text(
    "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"
    " AND classid = 0 AND objid = :key AND objsubid = 1 ORDER BY pid"
)
# A paused backend is done once it is gone (its client was killed) or idle (its client went on).
# Pid and state only: the query text could quote a job's row.
_BUSY = text(
    "SELECT pid, state, wait_event_type FROM pg_stat_activity"
    " WHERE pid = ANY(:pids) AND state IS DISTINCT FROM 'idle' ORDER BY pid"
)


class PausedUpdate:
    def __init__(self, engine: AsyncEngine, holder: AsyncConnection) -> None:
        self._engine = engine
        self._holder = holder
        self._paused: set[int] = set()
        # Nothing to release until paused_update has taken the lock.
        self._released = True

    async def wait_for_pause(
        self, timeout_s: float = 10.0, child: asyncio.subprocess.Process | None = None
    ) -> int:
        """The pid of the one backend paused in a matching update, once there is one. Fails at
        once if `child`, the process expected to pause, has exited."""
        __tracebackhide__ = True
        try:
            async with asyncio.timeout(timeout_s):
                while True:
                    async with self._engine.connect() as connection:
                        pids: list[int] = list(await connection.scalars(_WAITERS, {"key": KEY}))
                    if pids:
                        # Recorded first: cleanup waits for every paused backend, even if the
                        # check below fails.
                        self._paused.update(pids)
                        assert len(pids) == 1, f"{len(pids)} backends paused, expected 1"
                        return pids[0]
                    if child is not None and child.returncode is not None:
                        raise AssertionError(
                            f"the child exited with status {child.returncode} before pausing"
                        )
                    await asyncio.sleep(0.01)
        except TimeoutError:
            raise AssertionError(
                f"no backend paused in a matching update within {timeout_s} s"
            ) from None

    async def release(self) -> None:
        """Let the paused updates go on: a live client's commits, a killed one's rolls back."""
        if not self._released:
            unlocked = await self._holder.scalar(
                text("SELECT pg_advisory_unlock(:key)"), {"key": KEY}
            )
            await self._holder.commit()
            assert unlocked is True, "the advisory lock was not held"
            self._released = True

    async def wait_until_done(self, timeout_s: float = 5.0) -> None:
        """Every paused backend gone or idle, so no row lock of theirs is left."""
        busy: list[tuple[int, str | None, str | None]] = []
        try:
            async with asyncio.timeout(timeout_s):
                while self._paused:
                    async with self._engine.connect() as connection:
                        result = await connection.execute(_BUSY, {"pids": list(self._paused)})
                        busy = [(row.pid, row.state, row.wait_event_type) for row in result]
                    if not busy:
                        return
                    await asyncio.sleep(0.01)
        except TimeoutError:
            raise AssertionError(
                f"paused backends still busy after {timeout_s} s (pid, state, wait): {busy}"
            ) from None


async def _drop_trigger(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP TRIGGER IF EXISTS {TRIGGER} ON jobs"))
        await connection.execute(text(f"DROP FUNCTION IF EXISTS {TRIGGER}()"))


async def _clean_up(engine: AsyncEngine, holder: AsyncConnection, pause: PausedUpdate) -> None:
    """Each step runs even if the one before it failed; the first error is raised."""
    try:
        try:
            await pause.release()
        finally:
            # Never back to the pool, and before the wait: closing its backend frees the lock
            # even if the release failed, so no paused backend waits on it any longer.
            await holder.invalidate()
    finally:
        try:
            await pause.wait_until_done()
        finally:
            await _drop_trigger(engine)


@asynccontextmanager
async def paused_update(engine: AsyncEngine, when: str) -> AsyncIterator[PausedUpdate]:
    """Within the block, an UPDATE on `jobs` whose row matches `when` (a trigger WHEN condition
    on OLD and NEW, written by the test) waits. On leaving: the lock is released, every paused
    backend has ended or gone idle, and the trigger is dropped, also when the block failed. A
    cleanup error after a failed block is added to that failure as a note, not raised over it."""
    async with engine.begin() as connection:
        # Left behind only by a run killed inside the block; the engine fixture drops it too.
        await connection.execute(text(f"DROP TRIGGER IF EXISTS {TRIGGER} ON jobs"))
        await connection.execute(
            text(
                f"CREATE OR REPLACE FUNCTION {TRIGGER}() RETURNS trigger LANGUAGE plpgsql AS"
                f" $$ BEGIN PERFORM pg_advisory_xact_lock({KEY}); RETURN NEW; END $$"
            )
        )
        await connection.execute(
            text(
                f"CREATE TRIGGER {TRIGGER} BEFORE UPDATE ON jobs FOR EACH ROW"
                f" WHEN ({when}) EXECUTE FUNCTION {TRIGGER}()"
            )
        )
    try:
        holder = await engine.connect()
    except BaseException:
        await _drop_trigger(engine)
        raise
    pause = PausedUpdate(engine, holder)
    try:
        # A session lock: it outlives the commit, which returns the connection to idle.
        await holder.execute(text("SELECT pg_advisory_lock(:key)"), {"key": KEY})
        await holder.commit()
        pause._released = False
        yield pause
    except BaseException as exc:
        try:
            await _clean_up(engine, holder, pause)
        except Exception as cleanup:
            exc.add_note(f"paused_update cleanup also failed: {type(cleanup).__name__}: {cleanup}")
        raise
    await _clean_up(engine, holder, pause)
