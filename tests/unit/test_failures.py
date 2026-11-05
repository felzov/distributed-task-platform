"""Every failure code has a fixed message for `GET /jobs/{id}` and a retry classification."""

import pytest

from task_platform.domain.failures import (
    FAILURE_MESSAGES,
    RETRYABLE,
    UNKNOWN_FAILURE_MESSAGE,
    FailureCode,
)


def test_every_failure_code_has_a_message() -> None:
    assert set(FAILURE_MESSAGES) == set(FailureCode)


def test_every_failure_code_is_classified_for_retries() -> None:
    assert set(RETRYABLE) == set(FailureCode)


def test_only_the_handlers_own_failures_are_retryable() -> None:
    # Written out from ADR 0005 decision 1, independently of RETRYABLE.
    retryable = {code for code, retry in RETRYABLE.items() if retry}
    assert retryable == {FailureCode.HANDLER_ERROR, FailureCode.HANDLER_TIMEOUT}


@pytest.mark.parametrize("message", [*FAILURE_MESSAGES.values(), UNKNOWN_FAILURE_MESSAGE])
def test_messages_are_fixed_sentences(message: str) -> None:
    assert message.strip() == message != ""
    # No format placeholders: nothing from a job can be put into a message.
    assert "{" not in message
    assert "%" not in message
