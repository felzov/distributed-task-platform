"""Recovery sweeps for jobs that would otherwise wait forever (ADR 0008 decisions 1 and 2).

Lease recovery (ADR 0001 decision 8): a job stays `running` with its lease expired when its worker
crashed, was stopped past its grace period, or could not write its final update. Each sweep
requeues such jobs while they have attempts left, fails them otherwise, and closes the attempt's
history row.

Lost-message recovery (ADR 0002 decision 7): a job stays `queued` with `dispatched_at` set when its
message died or was lost before any claim. Each sweep, run only while the jobs queue has no ready
messages, marks such jobs unpublished again, so the dispatcher publishes them anew.

Logs carry ids, attempts and codes, never anything else from the rows.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform import safe_logging
from task_platform.config import BrokerSettings
from task_platform.data.attempts import close_attempt
from task_platform.data.jobs import (
    ExpiredLeaseRow,
    clear_lost_dispatches,
    fail_expired_lease,
    requeue_expired_lease,
    select_expired_leases,
    select_lost_message_job_ids,
)
from task_platform.domain.failures import FailureCode
from task_platform.domain.job_status import JobStatus, ensure_transition
from task_platform.domain.retries import AfterFailure, after_failure

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeaseSweep:
    """`expired`: jobs the sweep locked; `requeued` and `failed`: those it recovered each way.

    `expired` is their sum unless a recovery matched no row, which the sweep logs as an ERROR.
    """

    expired: int
    requeued: int
    failed: int


async def recover_expired_leases(
    session_factory: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> LeaseSweep:
    """Recover one batch of up to `dispatch_batch_size` expired leases, then COMMIT.

    One transaction holds the batch's row locks from the select to the commit, so a concurrent
    sweep skips these jobs and no job is recovered twice (ADR 0008 decision 1). A database error
    is raised and nothing of the batch is recovered; the next sweep finds the same jobs again.
    A requeued job is due now and unpublished, so the dispatcher's next poll publishes it.
    """
    async with session_factory.begin() as session:
        rows = await select_expired_leases(session, limit=settings.dispatch_batch_size)
        recoveries = [await _recover(session, row) for row in rows]
    # Only once committed: a log line written inside the transaction would claim recoveries that a
    # later error in the batch, or the COMMIT itself, rolled back.
    for recovery in recoveries:
        _log(recovery)
    targets = [recovery.target for recovery in recoveries]
    return LeaseSweep(
        expired=len(rows),
        requeued=targets.count(JobStatus.QUEUED),
        failed=targets.count(JobStatus.FAILED),
    )


@dataclass(frozen=True)
class _Recovery:
    """What happened to one job of the batch, logged after the COMMIT."""

    row: ExpiredLeaseRow
    # The status the job moved to; None if the update matched no row.
    target: JobStatus | None
    # Whether its open attempt row was closed.
    closed: bool


async def _recover(session: AsyncSession, row: ExpiredLeaseRow) -> _Recovery:
    """Requeue or fail one locked job and close its attempt row."""
    requeue = _decide(row) is AfterFailure.RETRY
    target = JobStatus.QUEUED if requeue else JobStatus.FAILED
    # The SQL conditions apply the change atomically; this keeps the transition table the one
    # place that allows it (ADR 0001).
    ensure_transition(JobStatus.RUNNING, target)
    if requeue:
        recovered = await requeue_expired_lease(session, row.id, attempt=row.attempts)
    else:
        recovered = await fail_expired_lease(session, row.id, attempt=row.attempts)
    if not recovered:
        return _Recovery(row, target=None, closed=False)
    # In the recovery's transaction, as for a worker's finish (ADR 0005 decision 5).
    closed = await close_attempt(
        session, row.id, attempt=row.attempts, failure=FailureCode.LEASE_EXPIRED
    )
    return _Recovery(row, target=target, closed=closed)


def _log(recovery: _Recovery) -> None:
    row = recovery.row
    if recovery.target is None:
        # The row lock from the select makes this impossible: the job's status, attempt and
        # lease cannot have changed since. Left as it is, it is found again by the next sweep.
        logger.error(
            "job %s attempt %d: lease recovery matched no row; the job is left as it is",
            row.id,
            row.attempts,
        )
        return
    logger.warning(
        "job %s attempt %d of %d: lease expired; %s",
        row.id,
        row.attempts,
        row.max_attempts,
        "requeued" if recovery.target is JobStatus.QUEUED else "the job fails",
    )
    if not recovery.closed:
        # Expected only for an attempt claimed before migration 0003, which never had a row (ADR
        # 0005 decision 7); otherwise a bug. The recovery is kept all the same, as a worker's
        # finish keeps its outcome (ADR 0004 update 2026-11-07): raising would roll back the whole
        # batch and repeat on every sweep.
        logger.error(
            "job %s attempt %d had no open attempt row to close; lease_expired is on the job only",
            row.id,
            row.attempts,
        )


def _decide(row: ExpiredLeaseRow) -> AfterFailure:
    """Retry while the job has attempts left (ADR 0005 decision 2); `lease_expired` is retryable."""
    try:
        return after_failure(
            FailureCode.LEASE_EXPIRED, attempt=row.attempts, max_attempts=row.max_attempts
        )
    # A bug or a hand-edited row: a running job with an attempt outside its budget. Failing it
    # records the expiry and stops a job the code cannot reason about, as a worker would.
    except ValueError as exc:
        logger.error(
            "job %s attempt %d of %d: no retry decision for %s (%s); the job fails",
            row.id,
            row.attempts,
            row.max_attempts,
            FailureCode.LEASE_EXPIRED,
            safe_logging.class_name(exc),
        )
        return AfterFailure.FAIL


class QueueDepth(Protocol):
    """Reads the jobs queue's ready messages (broker.queue_depth.ReadyCount)."""

    async def ready_messages(self) -> int: ...


@dataclass(frozen=True)
class LostMessageSweep:
    """`ready`: the jobs queue's ready messages, None if not read; the sweep runs only at 0.

    `found`: jobs the sweep locked; `cleared`: those it marked unpublished. Both 0 if skipped.
    """

    ready: int | None
    found: int
    cleared: int


async def recover_lost_messages(
    session_factory: async_sessionmaker[AsyncSession],
    queue: QueueDepth,
    settings: BrokerSettings,
) -> LostMessageSweep:
    """Mark up to `dispatch_batch_size` jobs whose message is overdue unpublished, then COMMIT.

    Only while the jobs queue has no ready messages: then no copy of such a message waits in a
    backlog, so publishing it again repeats nothing that is only delayed (ADR 0008 decision 2). A
    count that cannot be read within `publish_timeout_s` skips the sweep; so does a database
    error, which is raised. Either way, the next pass tries again.
    """
    try:
        # A robust channel waits for its connection during a broker outage: without this bound
        # one read would hold the recovery loop, the lease sweep included, until the broker is back.
        async with asyncio.timeout(settings.publish_timeout_s):
            ready = await queue.ready_messages()
    # A broker outage, a missing queue or a bug. The class only, as for every broker error.
    # CancelledError too: aiormq fails a pending request with it when its heartbeat finds the
    # connection stuck (observed on aiormq 7.2.4), though nothing cancelled this task. Only a
    # cancellation that was asked for, such as shutdown, ends the sweep.
    except (Exception, asyncio.CancelledError) as exc:
        task = asyncio.current_task()
        if isinstance(exc, asyncio.CancelledError) and task is not None and task.cancelling():
            raise
        logger.warning(
            "lost-message sweep skipped: the jobs queue's ready count was not read (%s)",
            safe_logging.class_name(exc),
        )
        return LostMessageSweep(ready=None, found=0, cleared=0)
    if ready:
        logger.info("lost-message sweep skipped: %d messages ready in the jobs queue", ready)
        return LostMessageSweep(ready=ready, found=0, cleared=0)
    async with session_factory.begin() as session:
        found = await select_lost_message_job_ids(
            session, lost_after_s=settings.lost_message_after_s, limit=settings.dispatch_batch_size
        )
        cleared = await clear_lost_dispatches(
            session, found, lost_after_s=settings.lost_message_after_s
        )
    # Only once committed, as for lease recovery. An ERROR: something lost the message or let it
    # die before its claim (a rejected claim, a dead letter, the broker), and a message that dies
    # that way every time comes back here once per threshold (ADR 0008 consequences).
    for job_id in cleared:
        logger.error(
            "job %s: no message ready %g s after its publish; marked to be published again",
            job_id,
            settings.lost_message_after_s,
        )
    return LostMessageSweep(ready=0, found=len(found), cleared=len(cleared))
