"""Every failure code has a fixed message for `GET /jobs/{id}`."""

import pytest

from task_platform.domain.failures import FAILURE_MESSAGES, UNKNOWN_FAILURE_MESSAGE, FailureCode


def test_every_failure_code_has_a_message() -> None:
    assert set(FAILURE_MESSAGES) == set(FailureCode)


@pytest.mark.parametrize("message", [*FAILURE_MESSAGES.values(), UNKNOWN_FAILURE_MESSAGE])
def test_messages_are_fixed_sentences(message: str) -> None:
    assert message.strip() == message != ""
    # No format placeholders: nothing from a job can be put into a message.
    assert "{" not in message
    assert "%" not in message
