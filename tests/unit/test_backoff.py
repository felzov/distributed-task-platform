"""Exponential backoff with full jitter (ADR 0005 decision 3)."""

import math

import pytest

from task_platform.domain.backoff import backoff_ceiling_s, retry_delay_s

BASE = 2.0
CAP = 300.0
JUST_BELOW_ONE = math.nextafter(1.0, 0.0)


def expected_ceiling(failed_attempt: int) -> float:
    # Written out from ADR 0005 decision 3, independently of backoff_ceiling_s.
    return min(CAP, BASE * 2.0 ** (failed_attempt - 1))


@pytest.mark.parametrize("failed_attempt", range(1, 21))
def test_the_ceiling_doubles_per_attempt_up_to_the_cap(failed_attempt: int) -> None:
    ceiling = backoff_ceiling_s(failed_attempt, base_s=BASE, cap_s=CAP)
    assert ceiling == expected_ceiling(failed_attempt)


def test_the_ceiling_starts_at_the_base_and_stays_at_the_cap() -> None:
    ceilings = [backoff_ceiling_s(n, base_s=BASE, cap_s=CAP) for n in range(1, 21)]
    assert ceilings[:5] == [2.0, 4.0, 8.0, 16.0, 32.0]
    # 2 * 2^8 = 512 > 300: from attempt 9 on, the cap.
    assert ceilings[8:] == [CAP] * 12
    assert ceilings == sorted(ceilings)


@pytest.mark.parametrize(
    ("failed_attempt", "ceiling"),
    [(1, 1.0), (9, 256.0), (12, 2048.0), (17, 65536.0), (18, 86400.0), (20, 86400.0)],
)
def test_the_ceiling_doubles_up_to_a_one_day_cap(failed_attempt: int, ceiling: float) -> None:
    # With BASE and CAP the cap is reached at attempt 9; base 1 s with the one-day cap reaches it
    # only at attempt 18, so a clamp or a shortcut that stops doubling early cannot hide here.
    assert backoff_ceiling_s(failed_attempt, base_s=1.0, cap_s=86400.0) == ceiling


def test_an_overflowing_product_gives_the_cap() -> None:
    # 1e308 * 2^2 is inf (float multiplication does not raise); min() must still return the cap.
    assert backoff_ceiling_s(3, base_s=1e308, cap_s=1.5e308) == 1.5e308


@pytest.mark.parametrize("failed_attempt", [1024, 1025, 10**6])
def test_a_huge_attempt_number_gives_the_cap_instead_of_overflowing(failed_attempt: int) -> None:
    assert backoff_ceiling_s(failed_attempt, base_s=BASE, cap_s=CAP) == CAP


@pytest.mark.parametrize("failed_attempt", range(1, 21))
@pytest.mark.parametrize("jitter", [0.0, 0.5, JUST_BELOW_ONE])
def test_the_delay_is_the_jittered_share_of_the_ceiling(failed_attempt: int, jitter: float) -> None:
    delay = retry_delay_s(failed_attempt, base_s=BASE, cap_s=CAP, jitter=jitter)
    ceiling = expected_ceiling(failed_attempt)
    assert delay == jitter * ceiling
    assert 0.0 <= delay < ceiling


def test_no_jitter_means_no_wait() -> None:
    assert retry_delay_s(5, base_s=BASE, cap_s=CAP, jitter=0.0) == 0.0


def test_base_equal_to_the_cap_gives_a_constant_ceiling() -> None:
    assert {backoff_ceiling_s(n, base_s=CAP, cap_s=CAP) for n in range(1, 21)} == {CAP}


@pytest.mark.parametrize("jitter", [-0.1, 1.0, 1.5, math.nan, math.inf])
def test_jitter_outside_zero_to_one_is_rejected(jitter: float) -> None:
    with pytest.raises(ValueError, match=r"jitter must be in \[0, 1\)"):
        retry_delay_s(1, base_s=BASE, cap_s=CAP, jitter=jitter)


@pytest.mark.parametrize("failed_attempt", [0, -1])
def test_an_attempt_below_one_is_rejected(failed_attempt: int) -> None:
    with pytest.raises(ValueError, match="failed_attempt must be at least 1"):
        retry_delay_s(failed_attempt, base_s=BASE, cap_s=CAP, jitter=0.5)


@pytest.mark.parametrize(
    ("base_s", "cap_s"),
    [
        pytest.param(0.0, CAP, id="zero base"),
        pytest.param(-1.0, CAP, id="negative base"),
        pytest.param(CAP + 1, CAP, id="base above cap"),
        pytest.param(BASE, math.inf, id="infinite cap"),
        pytest.param(math.nan, CAP, id="nan base"),
        pytest.param(BASE, math.nan, id="nan cap"),
    ],
)
def test_invalid_delays_are_rejected(base_s: float, cap_s: float) -> None:
    with pytest.raises(ValueError, match="delays must satisfy"):
        backoff_ceiling_s(1, base_s=base_s, cap_s=cap_s)
