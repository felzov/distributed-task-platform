"""The dispatcher's and the worker's logs as processes (ADR 0008, criterion 12): each line names
its time, level and logger, and at DEBUG neither a job's payload nor a password reaches them."""

import asyncio
import json
import re
import signal
from urllib.parse import unquote, urlsplit

import pytest
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.config import BrokerSettings
from tests.integration.processes import child_process
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-9034"
TIMESTAMP = r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}"


async def job_status(engine: AsyncEngine, job_id: object) -> str:
    async with engine.connect() as connection:
        status: str = await connection.scalar(
            text("SELECT status FROM jobs WHERE id = :id"), {"id": job_id}
        )
        return status


async def test_at_debug_the_logs_name_each_line_and_hold_no_payload_or_password(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    broker_settings: BrokerSettings,
    test_database_url: str,
    test_broker_url: str,
) -> None:
    # The worker's claim returns the payload: at DEBUG, SQLAlchemy's engine would log it as a row.
    payload = {"recipient": f"{SENTINEL}@example.com", "subject": "Hi", "body": SENTINEL}
    job_id = (await insert_job(engine, payload=json.dumps(payload)))["id"]
    env = {
        "DATABASE_URL": test_database_url,
        "BROKER_URL": test_broker_url,
        "JOBS_QUEUE_NAME": broker_settings.jobs_queue_name,
        "LOG_LEVEL": "DEBUG",
    }
    async with (
        child_process("task_platform.worker", env) as (worker, worker_stderr),
        child_process("task_platform.dispatcher", env) as (dispatcher, dispatcher_stderr),
    ):
        async with asyncio.timeout(15):
            while await job_status(engine, job_id) != "succeeded":
                await asyncio.sleep(0.1)
        for child in (worker, dispatcher):
            child.send_signal(signal.SIGTERM)
        async with asyncio.timeout(10):
            statuses = await asyncio.gather(worker.wait(), dispatcher.wait())

    assert list(statuses) == [0, 0]
    # Each check is a bool with a fixed message: pytest prints both sides of a failing `in`, and
    # these logs could hold the children's settings.
    worker_log = bytes(worker_stderr).decode(errors="replace")
    dispatcher_log = bytes(dispatcher_stderr).decode(errors="replace")
    started = re.search(
        rf"^{TIMESTAMP} INFO task_platform\.worker\.process: worker started: queue ",
        worker_log,
        re.MULTILINE,
    )
    assert started is not None, "no 'worker started' INFO line with time, level and logger"
    dispatched = re.search(
        rf"^{TIMESTAMP} INFO task_platform\.dispatcher\.process: dispatched 1 of 1 due jobs$",
        dispatcher_log,
        re.MULTILINE,
    )
    assert dispatched is not None, "no 'dispatched 1 of 1' INFO line with time, level and logger"
    # As a log line would hold them: decoded from the URL's percent-encoding.
    passwords = [
        unquote(password)
        for url in (test_database_url, test_broker_url)
        if (password := urlsplit(url).password)
    ]
    assert passwords, "the test URLs hold no password to look for"
    for name, log in (("worker", worker_log), ("dispatcher", dispatcher_log)):
        # DEBUG is in effect: the libraries' DEBUG lines are there.
        at_debug = re.search(rf"^{TIMESTAMP} DEBUG ", log, re.MULTILINE) is not None
        assert at_debug, f"no DEBUG line in the {name} log"
        payload_logged = SENTINEL in log
        assert not payload_logged, f"the job's payload reached the {name} log"
        password_logged = any(password in log for password in passwords)
        assert not password_logged, f"a password reached the {name} log"
