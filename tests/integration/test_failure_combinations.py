"""Failures in combination, with the dispatcher and the workers as real processes (phase 9).

A process killed with SIGKILL runs none of its own code on the way out: no `finally`, no nack, no
rollback, no connection close. The broker returns its unacknowledged messages when the socket
closes, and the database keeps whatever the process had committed. Each crash is placed exactly
with `tests.integration.faults.paused_update`, never with a sleep.
"""

import asyncio
import json
import re
import signal
from typing import Any

import httpx
import pytest
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.broker.topology import dead_letter_queue_name
from task_platform.config import BrokerSettings
from task_platform.main import create_app
from tests.app_client import app_client
from tests.integration.faults import paused_update
from tests.integration.processes import assert_logged_times, assert_not_logged, child_process
from tests.integration.rows import fake_notification_rows, fake_result
from tests.settings import make_test_settings

pytestmark = pytest.mark.integration

WORKER = "task_platform.worker"
DISPATCHER = "task_platform.dispatcher"
SENTINEL = "s3cr3t-sentinel-4187"
JOB = {
    "type": "notification.send",
    "payload_version": 1,
    "payload": {"recipient": f"{SENTINEL}@example.com", "subject": "Hi", "body": SENTINEL},
}
# The worker's final update for a success: it pauses after the notification has committed in its
# own transaction (services/notifications.py) and before the job and its attempt row are written.
FINISH_SUCCEEDED = "OLD.status = 'running' AND NEW.status = 'succeeded'"
# The dispatcher's mark_dispatched: it pauses after the broker confirmed the publish and before
# the poll's transaction commits (services/dispatch.py). Retries and recovery set dispatched_at
# back to NULL, so nothing else matches.
MARK_DISPATCHED = "OLD.dispatched_at IS NULL AND NEW.dispatched_at IS NOT NULL"
NOT_CLAIMED = b"not claimed (duplicate or stale delivery)"
DISPATCHED = b"dispatched 1 of 1 due jobs"


class ChildEnv(dict[str, str]):
    """A child's environment that never shows its values: pytest prints a failing test's
    arguments, and these hold the test URLs with their passwords."""

    def __repr__(self) -> str:
        return f"<child environment: {', '.join(sorted(self))}; values hidden>"


@pytest.fixture
def env(broker_settings: BrokerSettings, test_database_url: str, test_broker_url: str) -> ChildEnv:
    """The children's settings: this test's queue (deleted afterwards by the broker_connection
    fixture), a 2 s lease so recovery comes soon after the crash, and short loops. LOG_LEVEL is
    pinned: the test waits for INFO lines, which a developer's .env could hide."""
    return ChildEnv(
        DATABASE_URL=test_database_url,
        BROKER_URL=test_broker_url,
        JOBS_QUEUE_NAME=broker_settings.jobs_queue_name,
        LEASE_DURATION_S="2",
        HANDLER_TIMEOUT_S="1",
        DISPATCH_POLL_INTERVAL_S="0.05",
        RECOVERY_INTERVAL_S="0.1",
        LOG_LEVEL="INFO",
    )


async def job_row(engine: AsyncEngine, job_id: str) -> dict[str, Any]:
    """The columns the tests read, never the payload: `--showlocals` would print a whole row."""
    async with engine.connect() as connection:
        result = await connection.execute(
            text("SELECT status, dispatched_at FROM jobs WHERE id = :id"), {"id": job_id}
        )
        return dict(result.mappings().one())


async def queue_counts(connection: AbstractRobustConnection, queue: str) -> tuple[int, int]:
    """Ready messages and consumers, from a passive declare."""
    async with connection.channel() as channel:
        result = (await channel.declare_queue(queue, passive=True)).declaration_result
        assert result.message_count is not None and result.consumer_count is not None
        return result.message_count, result.consumer_count


async def ready_messages(connection: AbstractRobustConnection, queue: str) -> int:
    return (await queue_counts(connection, queue))[0]


async def wait_for_ready(connection: AbstractRobustConnection, queue: str, count: int) -> None:
    """Until the queue holds exactly `count` ready messages. Bounded: a message given back by a
    closed connection is counted ~0.1 s later (phase 8, R9)."""
    __tracebackhide__ = True
    found = None
    try:
        async with asyncio.timeout(5):
            while (found := await ready_messages(connection, queue)) != count:
                await asyncio.sleep(0.02)
    except TimeoutError:
        raise AssertionError(f"{found} ready messages in the queue, expected {count}") from None


async def assert_drained(connection: AbstractRobustConnection, queue: str) -> None:
    """Every message answered: no consumer left, and the jobs queue and its dead-letter queue
    stay empty. An empty read alone proves nothing: a message nobody answered comes back only when
    its consumer's connection closes, and is counted ~0.1 s later (R9); a quorum queue keeps
    counting a consumer until its held message is answered (phase 8). So first no consumer, then
    0.5 s of empty reads. Fails naming counts only."""
    __tracebackhide__ = True
    dead_letters = dead_letter_queue_name(queue)
    counts = None
    try:
        async with asyncio.timeout(5):
            while (counts := await queue_counts(connection, queue))[1] != 0:
                await asyncio.sleep(0.02)
    except TimeoutError:
        raise AssertionError(f"(ready, consumers) {counts}: a consumer is left") from None
    loop = asyncio.get_running_loop()
    until = loop.time() + 0.5
    while loop.time() < until:
        ready, dead = (
            await ready_messages(connection, queue),
            await ready_messages(connection, dead_letters),
        )
        assert (ready, dead) == (0, 0), f"{ready} ready, {dead} dead-lettered, expected none"
        await asyncio.sleep(0.05)


async def wait_for_output(
    child: asyncio.subprocess.Process, stderr: bytearray, line: bytes, count: int = 1
) -> None:
    """Until `child` has logged `line` `count` times; fails at once if it exited. Fails without
    showing its output."""
    __tracebackhide__ = True
    try:
        async with asyncio.timeout(15):
            while bytes(stderr).count(line) < count:
                if child.returncode is not None:
                    raise AssertionError(
                        f"the child exited with status {child.returncode} before logging {line!r}"
                    )
                await asyncio.sleep(0.02)
    except TimeoutError:
        found = bytes(stderr).count(line)
        raise AssertionError(f"{line!r} logged {found} times, expected {count}") from None


async def wait_until(client: httpx.AsyncClient, job_id: str, status: str) -> dict[str, Any]:
    """Poll `GET /jobs/{id}` until the job is `status`; fails naming the last status seen."""
    __tracebackhide__ = True
    last: tuple[str, str | None] | None = None
    try:
        async with asyncio.timeout(15):
            while True:
                body: dict[str, Any] = (await client.get(f"/jobs/{job_id}")).json()
                if body["status"] == status:
                    return body
                last = (body["status"], body.get("failure") and body["failure"]["code"])
                await asyncio.sleep(0.05)
    except TimeoutError:
        raise AssertionError(f"job stayed (status, failure) {last}, expected {status}") from None


async def stop(child: asyncio.subprocess.Process) -> int:
    __tracebackhide__ = True
    child.send_signal(signal.SIGTERM)
    try:
        async with asyncio.timeout(10):
            return await child.wait()
    except TimeoutError:
        raise AssertionError("the child did not exit within 10 s of SIGTERM") from None


# One log record: its header line (level and logger) and any lines after it, such as a
# traceback. The dispatcher's and worker's format: "<date> <time,ms> <LEVEL> <logger>: <message>"
# (log_config.py).
_RECORD_HEADER = re.compile(
    rb"^\S+ \S+,\d{3} (DEBUG|INFO|WARNING|ERROR|CRITICAL) (\S+): ", re.MULTILINE
)
# Python's own output for an error nothing handled; it bypasses logging.
_UNHANDLED = (b"Exception ignored in", b"Task was destroyed", b"was never retrieved")


def _records(stderr: bytes) -> list[tuple[bytes, bytes, bytes]]:
    """(level, logger, text) per record; text before the first header has level and logger b""."""
    headers = list(_RECORD_HEADER.finditer(stderr))
    first = headers[0].start() if headers else len(stderr)
    records = [(b"", b"", stderr[:first])] if first else []
    for index, header in enumerate(headers):
        end = headers[index + 1].start() if index + 1 < len(headers) else len(stderr)
        records.append((header.group(1), header.group(2), stderr[header.end() : end]))
    return records


def assert_clean(stderr: bytes) -> None:
    """No payload, no ERROR or CRITICAL record and no traceback in a child's log, and no
    unhandled-error output. Fails closed: an " ERROR " the header pattern does not parse fails
    too. Names levels and loggers only, never a line."""
    __tracebackhide__ = True
    assert_not_logged(stderr, SENTINEL.encode())
    for unhandled in _UNHANDLED:
        assert_not_logged(stderr, unhandled)
    records = _records(stderr)
    severe = [r for r in records if r[0] in {b"ERROR", b"CRITICAL"}]
    parsed = stderr.count(b" ERROR ") + stderr.count(b" CRITICAL ")
    assert parsed == len(severe), f"{parsed - len(severe)} ERROR or CRITICAL lines not parsed"
    unexpected = sorted(
        {
            (level.decode(), logger.decode())
            for level, logger, body in records
            if level in {b"ERROR", b"CRITICAL"} or b"Traceback" in body
        }
    )
    assert not unexpected, f"(level, logger) records with an error in the child's log: {unexpected}"


# A worker crash after its side effect (criteria 1 and 2)


@pytest.mark.parametrize(
    ("max_attempts", "final"),
    # 3, not 2: with an attempt still left after attempt 2, only the claim's status check keeps
    # the duplicate message from running the job again (with 2, its budget check would too).
    [(3, "succeeded"), (1, "failed")],
    ids=["attempts-left", "last-attempt"],
)
async def test_a_worker_killed_after_its_notification_is_recovered_and_sends_no_second_one(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    broker_settings: BrokerSettings,
    env: ChildEnv,
    max_attempts: int,
    final: str,
) -> None:
    queue = broker_settings.jobs_queue_name
    app = create_app(make_test_settings(env["DATABASE_URL"], default_max_attempts=max_attempts))
    async with app_client(app) as client:
        job_id = (await client.post("/jobs", json=JOB)).json()["id"]
        async with child_process(DISPATCHER, env) as (dispatcher, dispatcher_log):
            # The order matters: the worker leaves first, killed, then the pause releases its
            # backend, which rolls back. The other way round, a failing run would let a live
            # worker's final update commit.
            async with (
                paused_update(engine, FINISH_SUCCEEDED) as pause,
                child_process(WORKER, env) as (first, first_log),
            ):
                await pause.wait_for_pause(child=first)
                # The side effect is out: committed before the final update began.
                sent = await fake_notification_rows(engine, job_id)
                assert [n["attempt"] for n in sent] == [1]
                first.send_signal(signal.SIGKILL)
                await first.wait()
            # Leaving the block let the killed worker's backend end: its final update rolled back.
            # (Running, or already recovered on a slow machine: the lease is only 2 s.)
            assert (await job_row(engine, job_id))["status"] != "succeeded"

            # Lease recovery, then (attempts left) a second publish. The message the crash
            # returned is still in the queue: the job now has two, or one if it failed.
            if final == "succeeded":
                await wait_for_output(
                    dispatcher, dispatcher_log, b"lease recovery: 1 expired, 1 requeued, 0 failed"
                )
                await wait_for_output(dispatcher, dispatcher_log, DISPATCHED, 2)
                assert await ready_messages(broker_connection, queue) == 2
            else:
                await wait_for_output(
                    dispatcher, dispatcher_log, b"lease recovery: 1 expired, 0 requeued, 1 failed"
                )
                assert await ready_messages(broker_connection, queue) == 1

            async with child_process(WORKER, env) as (second, second_log):
                status = await wait_until(client, job_id, final)
                # The one message that loses: the claim refuses it, and it is acked.
                await wait_for_output(second, second_log, NOT_CLAIMED)
                assert await stop(second) == 0
            assert await stop(dispatcher) == 0
        attempts_response = await client.get(f"/jobs/{job_id}/attempts")
        failed_response = await client.get("/jobs?status=failed")

    attempts = [
        (a["attempt"], a["outcome"], a["failure"] and a["failure"]["code"])
        for a in attempts_response.json()["attempts"]
    ]
    # The handler ran on attempt 2 too, but the provider kept one notification per key (ADR 0007):
    # the crashed attempt's, which a success reports as its result.
    [notification] = await fake_notification_rows(engine, job_id)
    assert notification["attempt"] == 1
    if final == "succeeded":
        assert attempts == [(1, "failed", "lease_expired"), (2, "succeeded", None)]
        assert status["attempts"] == 2
        assert status["result"] == fake_result(notification)
        assert failed_response.json()["jobs"] == []
    else:
        # Out of attempts: failed, though its notification was sent (recorded as a known
        # consequence in docs/failure-modes.md, phase 9 S3).
        assert attempts == [(1, "failed", "lease_expired")]
        assert status["attempts"] == 1
        assert status["failure"]["code"] == "lease_expired"
        assert failed_response.json()["jobs"] == [status]
    # Published once more only with an attempt left; nothing left in either queue.
    assert_logged_times(bytes(dispatcher_log), DISPATCHED, 2 if final == "succeeded" else 1)
    await assert_drained(broker_connection, queue)
    assert_logged_times(bytes(second_log), NOT_CLAIMED, 1)
    for log in (dispatcher_log, first_log, second_log):
        assert_clean(bytes(log))
    for body in (json.dumps(status).encode(), attempts_response.content):
        assert_not_logged(body, SENTINEL.encode())


# A dispatcher crash between the broker's confirm and its commit (criterion 3)


async def test_a_dispatcher_killed_before_its_commit_publishes_again_and_the_job_runs_once(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    broker_settings: BrokerSettings,
    env: ChildEnv,
) -> None:
    queue = broker_settings.jobs_queue_name
    async with app_client(create_app(make_test_settings(env["DATABASE_URL"]))) as client:
        job_id = (await client.post("/jobs", json=JOB)).json()["id"]
        # The order matters, as for the worker crash: the dispatcher is killed first, then its
        # paused backend is released and rolls back.
        async with (
            paused_update(engine, MARK_DISPATCHED) as pause,
            child_process(DISPATCHER, env) as (first, first_log),
        ):
            await pause.wait_for_pause(child=first)
            # The broker confirmed the publish before mark_dispatched began: the message is out.
            await wait_for_ready(broker_connection, queue, 1)
            first.send_signal(signal.SIGKILL)
            await first.wait()
        # The poll's transaction rolled back: to the database the job was never published.
        row = await job_row(engine, job_id)
        assert (row["status"], row["dispatched_at"]) == ("queued", None)

        async with child_process(DISPATCHER, env) as (second, second_log):
            # Published again: one job, two messages.
            await wait_for_output(second, second_log, DISPATCHED)
            await wait_for_ready(broker_connection, queue, 2)
            async with child_process(WORKER, env) as (worker, worker_log):
                status = await wait_until(client, job_id, "succeeded")
                # The second delivery loses: the claim refuses it, and it is acked.
                await wait_for_output(worker, worker_log, NOT_CLAIMED)
                assert await stop(worker) == 0
            assert await stop(second) == 0
        attempts_response = await client.get(f"/jobs/{job_id}/attempts")

    attempts = [
        (a["attempt"], a["outcome"], a["failure"]) for a in attempts_response.json()["attempts"]
    ]
    assert attempts == [(1, "succeeded", None)]
    assert status["attempts"] == 1
    [notification] = await fake_notification_rows(engine, job_id)
    assert notification["attempt"] == 1
    assert status["result"] == fake_result(notification)
    # The killed dispatcher never committed, so never logged its publish; the second did, once.
    assert_not_logged(bytes(first_log), DISPATCHED)
    assert_logged_times(bytes(second_log), DISPATCHED, 1)
    assert_logged_times(bytes(worker_log), NOT_CLAIMED, 1)
    # Both messages acked: neither left unanswered nor rejected into the dead-letter queue.
    await assert_drained(broker_connection, queue)
    for log in (first_log, second_log, worker_log):
        assert_clean(bytes(log))
    for body in (json.dumps(status).encode(), attempts_response.content):
        assert_not_logged(body, SENTINEL.encode())
