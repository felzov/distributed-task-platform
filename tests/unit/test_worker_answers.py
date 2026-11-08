"""How the worker answers the broker for each outcome (ADR 0004 decision 6), with a fake message.

The real broker's side of each answer (requeued, dead-lettered, counted) is in
tests/integration/test_worker.py.
"""

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Mapping
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import BrokerSettings
from task_platform.domain.messages import encode_job_message
from task_platform.domain.payloads import PAYLOAD_MODELS
from task_platform.services.execution import Handler, Outcome
from task_platform.worker import process
from task_platform.worker.handlers import HANDLERS
from task_platform.worker.process import handle_message
from tests.settings import (
    UNREACHABLE_BROKER_URL,
    UNREACHABLE_DATABASE_URL,
    make_test_broker_settings,
)

SENTINEL = "s3cr3t-sentinel-7731"
SESSIONS: async_sessionmaker[AsyncSession] = async_sessionmaker()


def settings(backoff_s: float = 60.0) -> BrokerSettings:
    return make_test_broker_settings(
        UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL, worker_db_error_backoff_s=backoff_s
    )


class FakeMessage:
    def __init__(self, body: bytes, *, fails: bool = False) -> None:
        self.body = body
        self.fails = fails
        self.answers: list[tuple[str, bool | None]] = []

    async def _answer(self, action: str, requeue: bool | None) -> None:
        self.answers.append((action, requeue))
        if self.fails:
            raise ConnectionResetError(f"channel gone {SENTINEL}")

    async def ack(self, multiple: bool = False) -> None:
        await self._answer("ack", None)

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        await self._answer("nack", requeue)

    async def reject(self, requeue: bool = False) -> None:
        await self._answer("reject", requeue)


def returning(outcome: Outcome) -> Any:
    async def execute_job(
        session_factory: Any, job_id: uuid.UUID, handlers: Mapping[str, Handler], settings: Any
    ) -> Outcome:
        return outcome

    return execute_job


@pytest.mark.parametrize(
    ("outcome", "answer"),
    [
        (Outcome.UNKNOWN_JOB, ("ack", None)),
        (Outcome.NOT_CLAIMED, ("ack", None)),
        (Outcome.CLAIM_REJECTED, ("ack", None)),
        (Outcome.SUCCEEDED, ("ack", None)),
        # The job is queued again in the database; the dispatcher publishes a new message.
        (Outcome.RETRY_SCHEDULED, ("ack", None)),
        (Outcome.FAILED, ("ack", None)),
        (Outcome.STALE, ("ack", None)),
        (Outcome.FINISH_UNAVAILABLE, ("nack", True)),
    ],
)
async def test_each_outcome_gets_its_answer_at_once(
    outcome: Outcome, answer: tuple[str, bool | None], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(process, "execute_job", returning(outcome))
    message = FakeMessage(encode_job_message(uuid.uuid4()))

    # A 60 s backoff: only the claim's database error may wait for it.
    async with asyncio.timeout(1):
        await handle_message(message, SESSIONS, HANDLERS, settings())

    assert message.answers == [answer]


def test_every_outcome_has_an_answer_in_the_table() -> None:
    # A new Outcome must be added to the parametrization above, not acked by default unnoticed.
    tested = {
        Outcome.UNKNOWN_JOB,
        Outcome.NOT_CLAIMED,
        Outcome.CLAIM_REJECTED,
        Outcome.CLAIM_UNAVAILABLE,
        Outcome.SUCCEEDED,
        Outcome.RETRY_SCHEDULED,
        Outcome.FAILED,
        Outcome.STALE,
        Outcome.FINISH_UNAVAILABLE,
    }
    assert set(Outcome) == tested


async def test_a_database_error_at_the_claim_waits_then_nacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(process, "execute_job", returning(Outcome.CLAIM_UNAVAILABLE))
    message = FakeMessage(encode_job_message(uuid.uuid4()))

    started = time.monotonic()
    await handle_message(message, SESSIONS, HANDLERS, settings(backoff_s=0.2))

    assert time.monotonic() - started >= 0.2
    assert message.answers == [("nack", True)]


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"not json", id="not json"),
        pytest.param(json.dumps({"message_version": 2, "job_id": SENTINEL}).encode(), id="v2"),
        pytest.param(b"{" * 2000, id="too large"),
    ],
)
async def test_an_unparsable_message_is_rejected_without_requeue(
    body: bytes, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def must_not_run(*args: Any) -> Outcome:
        raise AssertionError("no job may be touched")

    monkeypatch.setattr(process, "execute_job", must_not_run)
    message = FakeMessage(body)

    await handle_message(message, SESSIONS, HANDLERS, settings())

    assert message.answers == [("reject", False)]
    assert "rejected an unparsable message: message" in caplog.text
    assert SENTINEL not in caplog.text


async def test_an_unexpected_error_requeues_with_reject(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def raises(*args: Any) -> Outcome:
        raise RuntimeError(f"bug quoting {SENTINEL}")

    monkeypatch.setattr(process, "execute_job", raises)
    job_id = uuid.uuid4()
    message = FakeMessage(encode_job_message(job_id))

    await handle_message(message, SESSIONS, HANDLERS, settings())

    # reject, not nack: counted towards the delivery limit, so it cannot loop forever.
    assert message.answers == [("reject", True)]
    assert f"job {job_id}: unexpected builtins.RuntimeError" in caplog.text
    assert [r.levelno for r in caplog.records] == [logging.ERROR]
    assert SENTINEL not in caplog.text


async def test_a_cancelled_worker_does_not_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    # Shutdown: the message stays unacknowledged and the broker requeues it with the channel.
    async def hangs(*args: Any) -> Outcome:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(process, "execute_job", hangs)
    message = FakeMessage(encode_job_message(uuid.uuid4()))

    task = asyncio.create_task(handle_message(message, SESSIONS, HANDLERS, settings()))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert message.answers == []


@pytest.mark.parametrize("outcome", [Outcome.SUCCEEDED, Outcome.FINISH_UNAVAILABLE])
async def test_a_failed_answer_is_logged_and_ignored(
    outcome: Outcome, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(process, "execute_job", returning(outcome))
    message = FakeMessage(encode_job_message(uuid.uuid4()), fails=True)

    await handle_message(message, SESSIONS, HANDLERS, settings())

    assert len(message.answers) == 1
    assert "a job message: builtins.ConnectionResetError" in caplog.text
    assert SENTINEL not in caplog.text


def test_every_job_type_has_a_handler() -> None:
    # Otherwise its jobs would fail as unknown_job_type, like jobs with bad data.
    assert set(HANDLERS) == {job_type for job_type, _ in PAYLOAD_MODELS}
