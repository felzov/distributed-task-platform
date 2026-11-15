"""Failures in combination, with the dispatcher and the workers as real processes (phase 9).

A process killed with SIGKILL runs none of its own code on the way out: no `finally`, no nack, no
rollback, no connection close. The broker returns its unacknowledged messages when the socket
closes, and the database keeps whatever the process had committed. Each crash is placed exactly
with `tests.integration.faults.paused_update`, never with a sleep; a lost ack, with the TCP proxy.
"""

import asyncio
import json
import re
import signal
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx
import pytest
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.broker.topology import dead_letter_queue_name
from task_platform.config import BrokerSettings
from task_platform.domain.messages import encode_job_message
from task_platform.main import create_app
from tests.app_client import app_client
from tests.integration.faults import paused_update
from tests.integration.processes import assert_logged_times, assert_not_logged, child_process
from tests.integration.rows import fake_notification_rows, fake_result, insert_job
from tests.proxy import TcpProxy, running_proxy
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


async def wait_for_ready(
    connection: AbstractRobustConnection, queue: str, count: int, what: str = "the queue"
) -> None:
    """Until `queue` holds exactly `count` ready messages; `what` names it in a failure. Bounded:
    a message given back by a closed connection is counted ~0.1 s later (phase 8, R9)."""
    __tracebackhide__ = True
    found = None
    try:
        async with asyncio.timeout(5):
            while (found := await ready_messages(connection, queue)) != count:
                await asyncio.sleep(0.02)
    except TimeoutError:
        raise AssertionError(f"{found} ready messages in {what}, expected {count}") from None


async def assert_drained(
    connection: AbstractRobustConnection, queue: str, dead_lettered: int = 0
) -> None:
    """Every message answered: no consumer left, the jobs queue empty and exactly `dead_lettered`
    messages in its dead-letter queue, for 0.5 s. An empty read alone proves nothing: a message
    nobody answered comes back only when its consumer's connection closes, and is counted ~0.1 s
    later (R9); a quorum queue keeps counting a consumer until its held message is answered
    (phase 8). So first no consumer, then the dead letters, then 0.5 s of steady reads. Fails
    naming counts only."""
    __tracebackhide__ = True
    dead_letters = dead_letter_queue_name(queue)
    counts = None
    try:
        async with asyncio.timeout(5):
            while (counts := await queue_counts(connection, queue))[1] != 0:
                await asyncio.sleep(0.02)
    except TimeoutError:
        raise AssertionError(f"(ready, consumers) {counts}: a consumer is left") from None
    await wait_for_ready(connection, dead_letters, dead_lettered, "the dead-letter queue")
    loop = asyncio.get_running_loop()
    until = loop.time() + 0.5
    while loop.time() < until:
        ready, dead = (
            await ready_messages(connection, queue),
            await ready_messages(connection, dead_letters),
        )
        assert (ready, dead) == (0, dead_lettered), (
            f"{ready} ready, {dead} dead-lettered, expected 0 and {dead_lettered}"
        )
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


@dataclass(frozen=True)
class KnownRecord:
    """A library's log record a test expects: allowed even as an ERROR or with a traceback."""

    level: bytes
    logger: bytes
    # Every one must be in the record's text: an allowance for one record, not a whole logger.
    markers: tuple[bytes, ...]

    def matches(self, level: bytes, logger: bytes, body: bytes) -> bool:
        return (level, logger) == (self.level, self.logger) and all(
            marker in body for marker in self.markers
        )


def assert_clean(stderr: bytes, *, known: Sequence[KnownRecord] = ()) -> None:
    """No payload, no ERROR or CRITICAL record and no traceback in a child's log, except the
    `known` records, and no unhandled-error output. Fails closed: an " ERROR " the header pattern
    does not parse fails too. Names levels and loggers only, never a line."""
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
            if (level in {b"ERROR", b"CRITICAL"} or b"Traceback" in body)
            and not any(record.matches(level, logger, body) for record in known)
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


# An ack lost on a broker cut, after the job's final update committed (criterion 4)

# What a worker logs when its broker link is cut (aio-pika 10.2.1, aiormq 7.2.4): aiormq a WARNING
# with the dropped socket's traceback, and aio-pika an ERROR from RobustChannel._on_close, whose
# restore finds the connection closed ("RuntimeError: <Connection ...> closed", the password
# masked). The robust connection then reconnects and restores the channel itself.
BROKER_CUT_RECORDS = (
    KnownRecord(b"WARNING", b"aiormq.connection", (b"Cancelling cause reader exited abnormally",)),
    KnownRecord(
        b"ERROR", b"aio_pika.tools", (b"RobustChannel._on_close", b"error: RuntimeError: ")
    ),
)


async def wait_for_held_ack(proxy: TcpProxy) -> None:
    """Until the proxy holds a chunk from the worker: its ack, the only thing it sends between
    the final update and the cut (aiormq sends a heartbeat every half of the 60 s timeout, 30 s)."""
    __tracebackhide__ = True
    try:
        async with asyncio.timeout(5):
            while proxy.held_upstream < 1:
                await asyncio.sleep(0.01)
    except TimeoutError:
        raise AssertionError("the worker sent nothing into the frozen link") from None


async def test_an_ack_lost_on_a_broker_cut_brings_the_message_back_and_the_claim_refuses_it(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    broker_settings: BrokerSettings,
    env: ChildEnv,
) -> None:
    queue = broker_settings.jobs_queue_name
    async with (
        app_client(create_app(make_test_settings(env["DATABASE_URL"]))) as client,
        running_proxy(env["BROKER_URL"]) as proxy,
    ):
        job_id = (await client.post("/jobs", json=JOB)).json()["id"]
        # Only the worker reaches the broker through the proxy. If aio-pika's own channel restore
        # failed, the worker would consume again by itself, after STARTUP_RETRY_INTERVAL_S (ADR
        # 0008 decision 8): short, so either path ends well within the test's waits.
        worker_env = ChildEnv(
            {
                **env,
                "BROKER_URL": proxy.url(env["BROKER_URL"]),
                "STARTUP_RETRY_INTERVAL_S": "0.2",
            }
        )
        async with child_process(DISPATCHER, env) as (dispatcher, dispatcher_log):
            async with child_process(WORKER, worker_env) as (worker, worker_log):
                async with paused_update(engine, FINISH_SUCCEEDED) as pause:
                    await pause.wait_for_pause(child=worker)
                    # Frozen before the final update goes on: the ack that follows its commit
                    # is held by the proxy, and the cut drops it. It never reaches the broker.
                    proxy.freeze()
                    await pause.release()
                    await wait_until(client, job_id, "succeeded")
                    await wait_for_held_ack(proxy)
                # The broker sees the connection drop and returns the unacked message, counted.
                # The worker's robust connection comes back through the restored proxy (aio-pika
                # waits 5 s before reconnecting) and is given the message again.
                proxy.cut()
                proxy.restore()
                await wait_for_output(worker, worker_log, NOT_CLAIMED)
                assert await stop(worker) == 0
            assert await stop(dispatcher) == 0
        status_response = await client.get(f"/jobs/{job_id}")
        attempts_response = await client.get(f"/jobs/{job_id}/attempts")

    status = status_response.json()
    attempts = [
        (a["attempt"], a["outcome"], a["failure"]) for a in attempts_response.json()["attempts"]
    ]
    assert attempts == [(1, "succeeded", None)]
    assert (status["status"], status["attempts"]) == ("succeeded", 1)
    [notification] = await fake_notification_rows(engine, job_id)
    assert status["result"] == fake_result(notification)
    # Published once: the redelivery came from the broker, not from a second publish.
    assert_logged_times(bytes(dispatcher_log), DISPATCHED, 1)
    assert_logged_times(bytes(worker_log), NOT_CLAIMED, 1)
    # The ack itself went out and was lost on the way: the worker never saw it fail.
    assert_not_logged(bytes(worker_log), b"could not ack")
    await assert_drained(broker_connection, queue)
    assert_clean(bytes(dispatcher_log))
    # Nothing of the worker's own is an ERROR; only the cut's two known library records.
    assert_clean(bytes(worker_log), known=BROKER_CUT_RECORDS)
    # Those lines print the connection: user, host and port, the password masked.
    for url in (env["DATABASE_URL"], env["BROKER_URL"]):
        password = urlsplit(url).password
        assert password, "a test URL without a password to look for"
        assert_not_logged(bytes(worker_log), unquote(password).encode())
    for body in (status_response.content, attempts_response.content):
        assert_not_logged(body, SENTINEL.encode())


# Poison and valid messages mixed, with two workers consuming (criterion 5)

UNPARSABLE = f'{{"message_version": 1, "job_id": "{SENTINEL}"'.encode()
REJECTED = b"rejected an unparsable message"
UNKNOWN_JOB = b": unknown job "


async def publish(connection: AbstractRobustConnection, queue: str, body: bytes) -> None:
    """A persistent message to the queue by the default exchange, as the dispatcher sends one but
    without its publisher confirms or `mandatory` flag: the queue exists, so nothing is lost."""
    async with connection.channel() as channel:
        await channel.default_exchange.publish(
            Message(body, delivery_mode=DeliveryMode.PERSISTENT), routing_key=queue
        )


async def dead_letter_bodies(connection: AbstractRobustConnection, queue: str) -> list[bytes]:
    """The dead-letter queue's messages, taken off it. Call it after `assert_drained`: before,
    a message on its way to the dead-letter queue could still be missing."""
    bodies = []
    async with connection.channel() as channel:
        dead_letters = await channel.declare_queue(dead_letter_queue_name(queue), passive=True)
        while (message := await dead_letters.get(no_ack=True, fail=False)) is not None:
            bodies.append(message.body)
    return bodies


async def wait_for_total(
    workers: Sequence[tuple[asyncio.subprocess.Process, bytearray]], line: bytes, count: int
) -> None:
    """Until the workers together have logged `line` `count` times; fails at once if one exited.
    Fails without showing their output."""
    __tracebackhide__ = True
    found = 0
    try:
        async with asyncio.timeout(15):
            while (found := sum(bytes(log).count(line) for _, log in workers)) < count:
                exited = [child.returncode for child, _ in workers if child.returncode is not None]
                assert not exited, f"a worker exited with status {exited[0]} before {line!r}"
                await asyncio.sleep(0.02)
    except TimeoutError:
        raise AssertionError(f"{line!r} logged {found} times in all, expected {count}") from None


async def wait_for_consumers(connection: AbstractRobustConnection, queue: str, count: int) -> None:
    """Until `queue` has exactly `count` consumers; it must exist already."""
    __tracebackhide__ = True
    found = None
    try:
        async with asyncio.timeout(5):
            while (found := (await queue_counts(connection, queue))[1]) != count:
                await asyncio.sleep(0.02)
    except TimeoutError:
        raise AssertionError(f"{found} consumers on the queue, expected {count}") from None


POISON_ANSWERS = (REJECTED, UNKNOWN_JOB, NOT_CLAIMED)


async def test_poison_messages_mixed_with_valid_ones_leave_two_workers_running(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    broker_settings: BrokerSettings,
    env: ChildEnv,
) -> None:
    queue = broker_settings.jobs_queue_name
    async with app_client(create_app(make_test_settings(env["DATABASE_URL"]))) as client:
        valid = [(await client.post("/jobs", json=JOB)).json()["id"] for _ in range(3)]
        # A payload the API would refuse, stored anyway (written before a schema change, say):
        # no subject or body. Attempts left, so only the failure's kind keeps it from a retry.
        invalid = str(
            (
                await insert_job(
                    engine,
                    payload=json.dumps({"recipient": f"{SENTINEL}@example.com"}),
                    max_attempts=3,
                )
            )["id"]
        )
        unknown = uuid.uuid4()
        poison = {
            # Cut off mid-object, quoting the sentinel: the worker must not echo it.
            "unparsable": UNPARSABLE,
            # A version the worker does not know, for a real job: it must not run that job.
            "wrong-version": json.dumps({"message_version": 2, "job_id": valid[1]}).encode(),
            "unknown-job": encode_job_message(unknown),
            # A second message for valid[0], published after the dispatcher's: a claim takes any
            # queued job, so published first it would run the job before the dispatcher's poll.
            "duplicate": encode_job_message(uuid.UUID(valid[0])),
        }
        async with (
            child_process(WORKER, env) as first,
            child_process(WORKER, env) as second,
        ):
            workers = (first, second)
            # Both consume before any message exists. With a prefetch of 1 each, the quorum
            # queue hands messages to its consumers in turn, so each worker gets some of the mix.
            for child, log in workers:
                await wait_for_output(child, log, b"worker started: queue")
            await wait_for_consumers(broker_connection, queue, 2)
            for label in ("unparsable", "wrong-version", "unknown-job"):
                await publish(broker_connection, queue, poison[label])
            async with child_process(DISPATCHER, env) as (dispatcher, dispatcher_log):
                # All four jobs were due before the dispatcher started: one poll publishes them.
                await wait_for_output(dispatcher, dispatcher_log, b"dispatched 4 of 4 due jobs")
                await publish(broker_connection, queue, poison["duplicate"])
                statuses = [await wait_until(client, job_id, "succeeded") for job_id in valid]
                failed = await wait_until(client, invalid, "failed")
                await wait_for_total(workers, REJECTED, 2)
                await wait_for_total(workers, UNKNOWN_JOB, 1)
                await wait_for_total(workers, NOT_CLAIMED, 1)
                assert await stop(dispatcher) == 0
            # Both still running after the whole mix, and both stop cleanly.
            assert [child.returncode for child, _ in workers] == [None, None]
            assert [await stop(child) for child, _ in workers] == [0, 0]
        attempts_responses = {
            job_id: await client.get(f"/jobs/{job_id}/attempts") for job_id in [*valid, invalid]
        }

    attempts = {
        job_id: [
            (a["attempt"], a["outcome"], a["failure"] and a["failure"]["code"])
            for a in response.json()["attempts"]
        ]
        for job_id, response in attempts_responses.items()
    }
    # Each valid job ran once, with one notification, whatever the duplicate and the wrong-version
    # message pointed at.
    for job_id, status in zip(valid, statuses, strict=True):
        assert attempts[job_id] == [(1, "succeeded", None)]
        [notification] = await fake_notification_rows(engine, job_id)
        assert status["result"] == fake_result(notification)
    # The stored invalid payload failed on its first attempt, not retried, nothing sent.
    assert (failed["attempts"], failed["failure"]["code"]) == (1, "invalid_payload")
    assert attempts[invalid] == [(1, "failed", "invalid_payload")]
    assert await fake_notification_rows(engine, invalid) == []
    # The two messages the worker cannot read are dead-lettered, untouched; the unknown job and
    # the duplicate were acked. Compared by label: a failing compare of the bodies would print
    # the sentinel.
    await assert_drained(broker_connection, queue, dead_lettered=2)
    dead_lettered = sorted(
        next((label for label, body in poison.items() if body == dead), "something else")
        for dead in await dead_letter_bodies(broker_connection, queue)
    )
    assert dead_lettered == ["unparsable", "wrong-version"]
    # The dispatcher published only the four jobs.
    assert_logged_times(bytes(dispatcher_log), b"dispatched", 1)
    # Each answered once, in all: a poison message requeued instead would be dead-lettered too,
    # but only after the queue's delivery limit, logged once per delivery.
    both = bytes(first[1]) + bytes(second[1])
    assert_logged_times(both, REJECTED, 2)
    assert_logged_times(both, f"unknown job {unknown}".encode(), 1)
    assert_logged_times(both, UNKNOWN_JOB, 1)
    assert_logged_times(both, f"job {valid[0]} not claimed".encode(), 1)
    assert_logged_times(both, NOT_CLAIMED, 1)
    # Each worker answered part of the mix itself, so "still running" holds for both.
    for index, (_, log) in enumerate(workers):
        answered = sum(bytes(log).count(line) for line in POISON_ANSWERS)
        assert answered >= 1, f"worker {index} answered none of the poison messages"
    for log in (dispatcher_log, first[1], second[1]):
        assert_clean(bytes(log))
    for body in [
        *(json.dumps(status).encode() for status in [*statuses, failed]),
        *(response.content for response in attempts_responses.values()),
    ]:
        assert_not_logged(body, SENTINEL.encode())
