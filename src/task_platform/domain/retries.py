"""What happens to a job after one of its attempts failed (ADR 0005 decision 2).

Pure: the worker records the answer with a fenced update (ADR 0005 decision 4).
"""

from enum import StrEnum

from task_platform.domain.failures import RETRYABLE, FailureCode


class AfterFailure(StrEnum):
    # running → queued, tried again once the backoff has passed.
    RETRY = "retry"
    # running → failed, for good.
    FAIL = "fail"


def after_failure(code: FailureCode, *, attempt: int, max_attempts: int) -> AfterFailure:
    """Retry a retryable failure while the job has attempts left; fail it otherwise.

    `attempt` is the attempt that failed, counted at claim time (ADR 0001 decision 4), so it is
    between 1 and `max_attempts`; anything else is a caller's bug and raises `ValueError`.
    """
    if not 1 <= attempt <= max_attempts:
        raise ValueError("attempt must be between 1 and max_attempts")
    if RETRYABLE[code] and attempt < max_attempts:
        return AfterFailure.RETRY
    return AfterFailure.FAIL
