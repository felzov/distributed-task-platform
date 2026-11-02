"""One dispatcher poll: the jobs table as outbox, published over RabbitMQ (ADR 0002, ADR 0004)."""

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import BrokerSettings
from task_platform.data.jobs import mark_dispatched, select_due_job_ids

logger = logging.getLogger(__name__)


class Publisher(Protocol):
    """Publishes one message per job and returns the ids the broker confirmed (JobPublisher)."""

    async def publish(self, job_ids: Sequence[uuid.UUID]) -> list[uuid.UUID]: ...


@dataclass(frozen=True)
class DispatchPoll:
    """`due`: jobs this poll locked and handed to the publisher; `dispatched`: those marked."""

    due: int
    dispatched: int


async def dispatch_due_jobs(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: Publisher,
    settings: BrokerSettings,
) -> DispatchPoll:
    """Publish the due jobs of one batch and mark the confirmed ones dispatched, then COMMIT.

    One transaction holds the batch's row locks from the select through the publishes to the
    commit, so a concurrent dispatcher skips these jobs (ADR 0002 decision 2). A job the broker did
    not confirm stays unmarked and is published again on a later poll. A database error, even after
    confirmed publishes, is raised. Unless the COMMIT itself went through before the connection
    was lost, nothing is marked and the jobs are published again: a duplicate the worker's claim
    absorbs (ADR 0002 decision 5), never a lost job.
    """
    async with session_factory.begin() as session:
        job_ids = await select_due_job_ids(session, limit=settings.dispatch_batch_size)
        if not job_ids:
            return DispatchPoll(due=0, dispatched=0)
        confirmed = await publisher.publish(job_ids)
        # Only this batch: an id from anywhere else was not published by this poll, and marking it
        # would report a dispatch nobody observed.
        confirmed_ids = set(confirmed)
        marked = await mark_dispatched(
            session, [job_id for job_id in job_ids if job_id in confirmed_ids]
        )
        # The row locks make a shortfall impossible for the batch's own jobs, so it means a bug,
        # such as a publisher confirming an id outside the batch. A batch job left unmarked is
        # published again on a later poll. Ids only, never anything from the rows.
        marked_ids = set(marked)
        unmarked = [job_id for job_id in confirmed if job_id not in marked_ids]
        if unmarked:
            logger.warning(
                "confirmed jobs not marked dispatched: %s", ", ".join(map(str, unmarked))
            )
    return DispatchPoll(due=len(job_ids), dispatched=len(marked))
