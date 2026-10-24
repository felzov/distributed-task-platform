"""Job statuses and the allowed transitions between them (ADR 0001).

This module answers only "is this status change possible at all". The conditions on a change
(attempts left, error retryable, lease expired) are decided by the callers, and the data layer
applies every change atomically with a conditional update (ADR 0001 decision 7).
"""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final

from task_platform.domain.errors import InvalidTransitionError


class JobStatus(StrEnum):
    # The values are stored in the `status` column and its CHECK constraint.
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


ALLOWED_TRANSITIONS: Final[Mapping[JobStatus, frozenset[JobStatus]]] = MappingProxyType(
    {
        # A worker claims the job.
        JobStatus.QUEUED: frozenset({JobStatus.RUNNING}),
        # Success; retry or lease recovery; final failure.
        JobStatus.RUNNING: frozenset({JobStatus.SUCCEEDED, JobStatus.QUEUED, JobStatus.FAILED}),
        JobStatus.SUCCEEDED: frozenset(),
        JobStatus.FAILED: frozenset(),
    }
)


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[current]


def ensure_transition(current: JobStatus, target: JobStatus) -> None:
    """Raise `InvalidTransitionError` unless `current -> target` is allowed."""
    if not can_transition(current, target):
        raise InvalidTransitionError(current, target)


def is_terminal(status: JobStatus) -> bool:
    return not ALLOWED_TRANSITIONS[status]
