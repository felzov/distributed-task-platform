"""Failures in combination, with the dispatcher and the workers as real processes (phase 9).

A worker killed with SIGKILL runs none of its own code on the way out: no `finally`, no nack, no
connection close. The broker returns its unacknowledged message when the socket closes, and the
database keeps whatever the worker had committed. Each crash is placed exactly with
`tests.integration.faults.paused_update`, never with a sleep.
"""

import asyncio
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
from tests.integration.processes import assert_not_logged, child_process
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
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


async def ready_messages(connection: AbstractRobustConnection, queue: str) -> int:
    async with connection.channel() as channel:
        count = (await channel.declare_queue(queue, passive=True)).declaration_result.message_count
        assert count is not None
        return count


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


def assert_clean(stderr: bytes) -> None:
    """No error, traceback or payload in a child's log; fails without showing it."""
    __tracebackhide__ = True
    for unwanted in (b" ERROR ", b"Traceback", SENTINEL.encode()):
        assert_not_logged(stderr, unwanted)


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
    assert bytes(dispatcher_log).count(DISPATCHED) == (2 if final == "succeeded" else 1)
    assert await ready_messages(broker_connection, queue) == 0
    assert await ready_messages(broker_connection, dead_letter_queue_name(queue)) == 0
    assert bytes(second_log).count(NOT_CLAIMED) == 1
    for log in (dispatcher_log, first_log, second_log):
        assert_clean(bytes(log))
    assert_not_logged(attempts_response.content, SENTINEL.encode())
