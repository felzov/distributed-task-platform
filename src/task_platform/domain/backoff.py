"""How long a job waits before it is retried: exponential backoff with full jitter (ADR 0005).

Pure and deterministic: the caller passes the random `jitter`, and PostgreSQL turns the delay into
`available_at` (ADR 0001 decision 9).
"""

import math
from typing import Final

# 2.0 ** 1024 raises OverflowError. Clamping the exponent keeps the ceiling computable; for any base
# of at least 1e-300 s, 2^1023 times it already exceeds a cap of one day, so the clamp changes
# nothing in practice.
_MAX_EXPONENT: Final = 1023


def backoff_ceiling_s(failed_attempt: int, *, base_s: float, cap_s: float) -> float:
    """The longest wait after attempt `failed_attempt` failed: `min(cap_s, base_s * 2^(n-1))`."""
    if failed_attempt < 1:
        raise ValueError("failed_attempt must be at least 1")
    if not (0 < base_s <= cap_s and math.isfinite(cap_s)):
        raise ValueError("delays must satisfy 0 < base_s <= cap_s < inf")
    exponent = min(failed_attempt - 1, _MAX_EXPONENT)
    # An overflowing product is inf (float multiplication does not raise), and min() keeps the cap.
    return min(cap_s, base_s * 2.0**exponent)


def retry_delay_s(failed_attempt: int, *, base_s: float, cap_s: float, jitter: float) -> float:
    """The wait before the next attempt: `jitter * backoff_ceiling_s(...)`, `jitter` in `[0, 1)`.

    Full jitter: the delay is drawn uniformly between 0 and the ceiling, so jobs that failed
    together do not retry together.
    """
    if not 0.0 <= jitter < 1.0:
        raise ValueError("jitter must be in [0, 1)")
    return jitter * backoff_ceiling_s(failed_attempt, base_s=base_s, cap_s=cap_s)
