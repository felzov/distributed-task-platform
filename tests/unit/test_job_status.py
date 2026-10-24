from itertools import product

import pytest

from task_platform.domain.errors import ErrorCode, InvalidTransitionError
from task_platform.domain.job_status import (
    ALLOWED_TRANSITIONS,
    JobStatus,
    can_transition,
    ensure_transition,
    is_terminal,
)

Q, R, S, F = JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCEEDED, JobStatus.FAILED

# Written out from ADR 0001 decision 2, independently of ALLOWED_TRANSITIONS.
EXPECTED_ALLOWED = {(Q, R), (R, S), (R, Q), (R, F)}
ALL_PAIRS = list(product(JobStatus, repeat=2))


def test_status_values_match_adr_0001() -> None:
    # These strings end up in the database CHECK constraint; renaming one is a migration.
    assert {status.value for status in JobStatus} == {"queued", "running", "succeeded", "failed"}


def test_every_status_has_an_entry_in_the_table() -> None:
    assert set(ALLOWED_TRANSITIONS) == set(JobStatus)


@pytest.mark.parametrize(("current", "target"), sorted(EXPECTED_ALLOWED))
def test_allowed_transitions_pass(current: JobStatus, target: JobStatus) -> None:
    assert can_transition(current, target)
    ensure_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"), [pair for pair in ALL_PAIRS if pair not in EXPECTED_ALLOWED]
)
def test_every_other_transition_is_rejected(current: JobStatus, target: JobStatus) -> None:
    assert not can_transition(current, target)
    with pytest.raises(InvalidTransitionError) as exc_info:
        ensure_transition(current, target)
    assert exc_info.value.code is ErrorCode.INVALID_TRANSITION
    assert exc_info.value.current == current
    assert exc_info.value.target == target


def test_all_sixteen_pairs_are_covered() -> None:
    assert len(ALL_PAIRS) == 16
    assert len(EXPECTED_ALLOWED) == 4


@pytest.mark.parametrize("status", list(JobStatus))
def test_staying_in_the_same_status_is_rejected(status: JobStatus) -> None:
    # A duplicate delivery must not "re-claim" a running job (ADR 0001 decision 7).
    with pytest.raises(InvalidTransitionError):
        ensure_transition(status, status)


@pytest.mark.parametrize(("status", "terminal"), [(Q, False), (R, False), (S, True), (F, True)])
def test_only_succeeded_and_failed_are_terminal(status: JobStatus, terminal: bool) -> None:
    assert is_terminal(status) is terminal


def test_error_message_names_both_statuses() -> None:
    with pytest.raises(InvalidTransitionError, match="'succeeded' to 'running'"):
        ensure_transition(S, R)


def test_unknown_status_value_is_rejected() -> None:
    # A value outside the CHECK constraint, e.g. read from a corrupted row, never becomes a status.
    with pytest.raises(ValueError, match="cancelled"):
        JobStatus("cancelled")


def test_transition_table_is_read_only() -> None:
    with pytest.raises(TypeError):
        ALLOWED_TRANSITIONS[S] = frozenset({R})  # type: ignore[index]
