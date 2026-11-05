"""How one attempt of a job ended (ADR 0005 decision 5)."""

from enum import StrEnum


class AttemptOutcome(StrEnum):
    # The values are stored in job_attempts.outcome and its CHECK constraint (migration 0003). An
    # attempt still running, or whose worker crashed, has no outcome yet (NULL).
    SUCCEEDED = "succeeded"
    # The failure's code is stored with it. Whether the job was then retried is the job's
    # business: a failed attempt looks the same either way.
    FAILED = "failed"
