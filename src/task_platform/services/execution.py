"""One delivery of a job: claim, run its handler, record the outcome (ADR 0001, ADR 0004).

PostgreSQL decides everything here; the message only named the job. The returned `Outcome` tells
the consumer how to answer the broker (ADR 0004 decision 6). Logs carry ids, attempts, codes and
error classes, never a payload or an exception's text.
"""

import asyncio
import logging
import random
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform import safe_logging
from task_platform.config import BrokerSettings
from task_platform.data.attempts import close_attempt, open_attempt
from task_platform.data.jobs import (
    ClaimedJobRow,
    claim_job,
    mark_failed,
    mark_retry,
    mark_succeeded,
    select_job,
)
from task_platform.domain.backoff import retry_delay_s
from task_platform.domain.errors import DomainError, ErrorCode
from task_platform.domain.failures import FailureCode
from task_platform.domain.job_status import JobStatus, ensure_transition
from task_platform.domain.payloads import PayloadModel, parse_payload
from task_platform.domain.retries import AfterFailure, after_failure

logger = logging.getLogger(__name__)


class Outcome(StrEnum):
    """What happened to one delivery. The consumer acks every outcome except the two marked."""

    UNKNOWN_JOB = "unknown_job"
    # A duplicate or stale delivery: not due, already claimed, finished, or out of attempts.
    NOT_CLAIMED = "not_claimed"
    # The database refused the claim for this job's data (a bug): retrying cannot help.
    CLAIM_REJECTED = "claim_rejected"
    # The database could not be reached or timed out: back off, then nack.
    CLAIM_UNAVAILABLE = "claim_unavailable"
    SUCCEEDED = "succeeded"
    # A retryable failure with attempts left: the job is queued again, due after its backoff, and
    # the dispatcher publishes a new message then (ADR 0005 decision 4). This message is done.
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED = "failed"
    # A newer attempt owns the job; this attempt's result was discarded.
    STALE = "stale"
    # The final update could not be written: nack; the job stays running until lease recovery
    # takes it on (ADR 0008 decision 1).
    FINISH_UNAVAILABLE = "finish_unavailable"


@dataclass(frozen=True)
class ClaimedJob:
    """What a handler knows about the attempt it runs."""

    id: uuid.UUID
    attempt: int


# A handler returns the job's result, a small JSON object. It may open its own transactions. The
# result is shown to clients by `GET /jobs/{id}`, so it must never copy the payload.
Handler = Callable[
    [async_sessionmaker[AsyncSession], ClaimedJob, PayloadModel], Awaitable[dict[str, Any]]
]

# SQLSTATE classes 22 (data exception) and 23 (integrity constraint violation): this job's own data
# or a bug in the statement, so the same claim fails the same way every time and a nack would only
# repeat it (nack is not counted, ADR 0004 decision 7). By SQLSTATE, not exception class: the
# asyncpg adapter raises a plain DBAPIError for class 22. Every other database error, a schema
# this code does not match (class 42) included, is treated as an outage that ends: backoff, nack.
# So is an error without a SQLSTATE, raised by the driver itself; the claim's parameters (a UUID
# and an interval) give it nothing to reject.
_DETERMINISTIC_SQLSTATE_CLASSES: Final = frozenset({"22", "23"})

# The stored payload is checked again, with the same rules as at submission.
_STORED_PAYLOAD_FAILURES: Final[Mapping[ErrorCode, FailureCode]] = {
    ErrorCode.UNKNOWN_JOB_TYPE: FailureCode.UNKNOWN_JOB_TYPE,
    ErrorCode.UNSUPPORTED_PAYLOAD_VERSION: FailureCode.UNSUPPORTED_PAYLOAD_VERSION,
    ErrorCode.PAYLOAD_TOO_LARGE: FailureCode.PAYLOAD_TOO_LARGE,
    ErrorCode.INVALID_PAYLOAD: FailureCode.INVALID_PAYLOAD,
}


async def execute_job(
    session_factory: async_sessionmaker[AsyncSession],
    job_id: uuid.UUID,
    handlers: Mapping[str, Handler],
    settings: BrokerSettings,
    *,
    jitter: Callable[[], float] = random.random,
) -> Outcome:
    """Claim the job, run its handler within `handler_timeout_s`, and record the result.

    A retryable failure is retried after a backoff scaled by `jitter()`, a number in [0, 1)
    (ADR 0005 decision 3); tests pass a fixed one. Not for security, so `random` will do.
    """
    # The SQL conditions apply each change atomically; this keeps the transition table the one
    # place that allows it (ADR 0001).
    ensure_transition(JobStatus.QUEUED, JobStatus.RUNNING)
    try:
        async with session_factory.begin() as session:
            row = await claim_job(session, job_id, lease_duration_s=settings.lease_duration_s)
            if row is not None:
                # In the claim's transaction, so the attempt has a row exactly when the claim
                # committed (ADR 0005 decision 5). A row already there for this attempt number is
                # a bug: its unique violation (23505, class 23) rolls the claim back and the claim
                # is rejected below; the job stays queued with dispatched_at set until
                # lost-message recovery publishes it again (ADR 0008 decision 2).
                await open_attempt(session, job_id, attempt=row.attempts)
            # claim_job cannot tell an unknown job from a lost claim; ADR 0004 answers both with
            # an ack, but an unknown job is worth a warning.
            known = row is not None or await select_job(session, job_id) is not None
    # TimeoutError (asyncpg's, a lock wait under the statement timeout) is an OSError subclass.
    except (SQLAlchemyError, OSError) as exc:
        code = safe_logging.sqlstate(exc)
        if code is not None and code[:2] in _DETERMINISTIC_SQLSTATE_CLASSES:
            logger.error(
                "claim rejected by the database: job %s, %s (sqlstate %s)",
                job_id,
                safe_logging.class_name(exc),
                code,
            )
            return Outcome.CLAIM_REJECTED
        logger.warning(
            "claim failed, database unavailable: job %s, %s (sqlstate %s)",
            job_id,
            safe_logging.class_name(exc),
            code,
        )
        return Outcome.CLAIM_UNAVAILABLE
    if row is None:
        if not known:
            logger.warning("unknown job %s", job_id)
            return Outcome.UNKNOWN_JOB
        logger.info("job %s not claimed (duplicate or stale delivery)", job_id)
        return Outcome.NOT_CLAIMED

    job = ClaimedJob(id=job_id, attempt=row.attempts)
    failure, result = await _run_handler(session_factory, job, row, handlers, settings)
    # Decided before the final transaction, so a bug in the decision cannot roll back the finish.
    retry_in_s = (
        None
        if failure is None
        else _retry_delay(
            job, failure, max_attempts=row.max_attempts, settings=settings, jitter=jitter
        )
    )
    return await _finish(session_factory, job, failure, result, retry_in_s=retry_in_s)


def _retry_delay(
    job: ClaimedJob,
    failure: FailureCode,
    *,
    max_attempts: int,
    settings: BrokerSettings,
    jitter: Callable[[], float],
) -> float | None:
    """How long to wait before retrying the failed attempt, or None if the job fails for good.

    `max_attempts` is the job's own budget, from its claim: the setting may have changed since the
    job was submitted.
    """
    try:
        if (
            after_failure(failure, attempt=job.attempt, max_attempts=max_attempts)
            is AfterFailure.FAIL
        ):
            return None
        return retry_delay_s(
            job.attempt,
            base_s=settings.retry_base_delay_s,
            cap_s=settings.retry_max_delay_s,
            jitter=jitter(),
        )
    # A bug: an attempt outside the budget, a code missing from RETRYABLE, a jitter outside [0, 1).
    # Raising would leave the job running and reject the message; failing it records the attempt
    # and its real code, and stops a job the code cannot reason about.
    except (ValueError, KeyError) as exc:
        logger.error(
            "job %s attempt %d of %d: no retry decision for %s (%s); the job fails",
            job.id,
            job.attempt,
            max_attempts,
            failure,
            safe_logging.class_name(exc),
        )
        return None


async def _run_handler(
    session_factory: async_sessionmaker[AsyncSession],
    job: ClaimedJob,
    row: ClaimedJobRow,
    handlers: Mapping[str, Handler],
    settings: BrokerSettings,
) -> tuple[FailureCode | None, dict[str, Any]]:
    try:
        payload = parse_payload(
            row.type, row.payload_version, row.payload, max_bytes=settings.max_payload_bytes
        )
    except DomainError as exc:
        failure = _STORED_PAYLOAD_FAILURES[exc.code]
        logger.warning("job %s attempt %d failed: %s", job.id, job.attempt, failure)
        return failure, {}
    handler = handlers.get(row.type)
    if handler is None:
        # A known type without a handler in this worker: a deployment error, not the job's.
        logger.warning("job %s attempt %d failed: no handler for its type", job.id, job.attempt)
        return FailureCode.UNKNOWN_JOB_TYPE, {}

    deadline = asyncio.timeout(settings.handler_timeout_s)
    try:
        async with deadline:
            result = await handler(session_factory, job, payload)
    # Not CancelledError (a BaseException): shutdown must still stop the worker.
    except Exception as exc:
        # A TimeoutError from inside the handler is the handler's own error, not our deadline.
        timed_out = isinstance(exc, TimeoutError) and deadline.expired()
        failure = FailureCode.HANDLER_TIMEOUT if timed_out else FailureCode.HANDLER_ERROR
        logger.warning(
            "job %s attempt %d failed: %s (%s)",
            job.id,
            job.attempt,
            failure,
            safe_logging.class_name(exc),
        )
        return failure, {}
    if deadline.expired():
        # The handler caught the cancellation and returned anyway: too late all the same.
        logger.warning(
            "job %s attempt %d failed: %s (returned after cancellation)",
            job.id,
            job.attempt,
            FailureCode.HANDLER_TIMEOUT,
        )
        return FailureCode.HANDLER_TIMEOUT, {}
    return None, result


async def _finish(
    session_factory: async_sessionmaker[AsyncSession],
    job: ClaimedJob,
    failure: FailureCode | None,
    result: dict[str, Any],
    *,
    retry_in_s: float | None,
) -> Outcome:
    if failure is None:
        target, outcome = JobStatus.SUCCEEDED, Outcome.SUCCEEDED
    elif retry_in_s is None:
        target, outcome = JobStatus.FAILED, Outcome.FAILED
    else:
        target, outcome = JobStatus.QUEUED, Outcome.RETRY_SCHEDULED
    ensure_transition(JobStatus.RUNNING, target)
    closed = budget_spent = False
    try:
        async with session_factory.begin() as session:
            if failure is None:
                current = await mark_succeeded(
                    session, job.id, claimed_attempt=job.attempt, result=result
                )
            elif retry_in_s is None:
                current = await mark_failed(
                    session, job.id, claimed_attempt=job.attempt, error_code=failure
                )
            else:
                current = await mark_retry(
                    session,
                    job.id,
                    claimed_attempt=job.attempt,
                    error_code=failure,
                    delay_s=retry_in_s,
                )
                if not current:
                    # mark_retry also refuses a job whose budget is spent, which only a change to
                    # max_attempts after the claim can cause. Such a job fails (ADR 0005
                    # decision 2) rather than stay running; a stale attempt is refused here too.
                    ensure_transition(JobStatus.RUNNING, JobStatus.FAILED)
                    current = budget_spent = await mark_failed(
                        session, job.id, claimed_attempt=job.attempt, error_code=failure
                    )
            # Only the attempt that still owns the job records its outcome, in the same
            # transaction (ADR 0005 decision 5): a stale attempt writes neither table, and a
            # database error rolls both back.
            if current:
                closed = await close_attempt(session, job.id, attempt=job.attempt, failure=failure)
    except (SQLAlchemyError, OSError) as exc:
        logger.warning(
            "final update failed: job %s attempt %d, %s (sqlstate %s)",
            job.id,
            job.attempt,
            safe_logging.class_name(exc),
            safe_logging.sqlstate(exc),
        )
        return Outcome.FINISH_UNAVAILABLE
    if not current:
        logger.warning(
            "job %s attempt %d is no longer current; its outcome was discarded", job.id, job.attempt
        )
        return Outcome.STALE
    if budget_spent:
        outcome = Outcome.FAILED
        logger.error(
            "job %s attempt %d was not requeued: its max_attempts was lowered after the claim;"
            " the job fails with %s",
            job.id,
            job.attempt,
            failure,
        )
    if not closed:
        # Should not happen: the claim opened this row in its own transaction, and only the
        # current attempt closes it. Not even for a job claimed before migration 0003: the old
        # worker that claimed it also finishes it. The job's outcome is kept all the same: raising
        # would roll the finish back and leave the job running over a gap in its history.
        logger.error(
            "job %s attempt %d had no open attempt row to close; its outcome %s is on the job only",
            job.id,
            job.attempt,
            failure or "succeeded",
        )
    if outcome is Outcome.RETRY_SCHEDULED:
        logger.info(
            "job %s attempt %d failed with %s; retry in %.3f s",
            job.id,
            job.attempt,
            failure,
            retry_in_s,
        )
    return outcome
