"""Retry or fail after a failed attempt (ADR 0005 decision 2)."""

import pytest

from task_platform.domain.failures import FailureCode
from task_platform.domain.retries import AfterFailure, after_failure

# An expired lease is retried like the handler's own failures (ADR 0008 decision 1): lease
# recovery requeues the job while it has attempts left and fails it on the last one.
RETRYABLE_CODES = [
    FailureCode.HANDLER_ERROR,
    FailureCode.HANDLER_TIMEOUT,
    FailureCode.LEASE_EXPIRED,
]
PERMANENT_CODES = [code for code in FailureCode if code not in RETRYABLE_CODES]


@pytest.mark.parametrize("code", RETRYABLE_CODES)
@pytest.mark.parametrize(("attempt", "max_attempts"), [(1, 2), (1, 5), (4, 5), (19, 20)])
def test_a_retryable_failure_with_attempts_left_is_retried(
    code: FailureCode, attempt: int, max_attempts: int
) -> None:
    assert after_failure(code, attempt=attempt, max_attempts=max_attempts) is AfterFailure.RETRY


@pytest.mark.parametrize("code", RETRYABLE_CODES)
@pytest.mark.parametrize("max_attempts", [1, 5, 20])
def test_a_retryable_failure_on_the_last_attempt_fails_the_job(
    code: FailureCode, max_attempts: int
) -> None:
    assert after_failure(code, attempt=max_attempts, max_attempts=max_attempts) is AfterFailure.FAIL


@pytest.mark.parametrize("code", PERMANENT_CODES)
@pytest.mark.parametrize(("attempt", "max_attempts"), [(1, 1), (1, 5), (5, 5)])
def test_a_permanent_failure_fails_the_job_even_with_attempts_left(
    code: FailureCode, attempt: int, max_attempts: int
) -> None:
    assert after_failure(code, attempt=attempt, max_attempts=max_attempts) is AfterFailure.FAIL


@pytest.mark.parametrize(("attempt", "max_attempts"), [(0, 5), (-1, 5), (6, 5)])
def test_an_attempt_outside_the_budget_is_a_bug(attempt: int, max_attempts: int) -> None:
    # The claim counts attempts from 1 and never past max_attempts (ck_jobs_attempts_within_max).
    with pytest.raises(ValueError, match="attempt must be between 1 and max_attempts"):
        after_failure(FailureCode.HANDLER_ERROR, attempt=attempt, max_attempts=max_attempts)
