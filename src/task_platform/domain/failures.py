"""Why a job failed: the fixed codes stored in `jobs.error_code` (ADR 0004 decision 6).

Separate from `ErrorCode`, which names errors a client causes and gets an HTTP status for. These
codes describe an attempt that already ran. Never exception text: it can quote the payload.
"""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final


class FailureCode(StrEnum):
    # The stored payload no longer passes validation (same names as the API's errors).
    UNKNOWN_JOB_TYPE = "unknown_job_type"
    UNSUPPORTED_PAYLOAD_VERSION = "unsupported_payload_version"
    PAYLOAD_TOO_LARGE = "payload_too_large"
    INVALID_PAYLOAD = "invalid_payload"
    # The handler raised, or did not finish within handler_timeout_s.
    HANDLER_ERROR = "handler_error"
    HANDLER_TIMEOUT = "handler_timeout"
    # The attempt's lease ran out before any finish was recorded: its worker crashed, was stopped
    # past its grace period, or lost the database (ADR 0008 decision 1). Written by lease recovery.
    LEASE_EXPIRED = "lease_expired"


# What `GET /jobs/{id}` says about each code: fixed sentences, nothing taken from the job.
FAILURE_MESSAGES: Final[Mapping[FailureCode, str]] = MappingProxyType(
    {
        FailureCode.UNKNOWN_JOB_TYPE: "no handler exists for the job's type",
        FailureCode.UNSUPPORTED_PAYLOAD_VERSION: "the job's payload version is not supported",
        FailureCode.PAYLOAD_TOO_LARGE: "the job's payload exceeds the size limit",
        FailureCode.INVALID_PAYLOAD: "the job's payload is not valid for its type",
        FailureCode.HANDLER_ERROR: "the job's handler raised an error",
        FailureCode.HANDLER_TIMEOUT: "the job's handler did not finish in time",
        FailureCode.LEASE_EXPIRED: "the job's attempt did not finish before its lease expired",
    }
)

# For a stored code this code does not know, e.g. one written by a newer worker.
UNKNOWN_FAILURE_MESSAGE: Final = "the job failed"

# Whether a failed attempt is worth another one (ADR 0005 decision 1, with the exception of ADR
# 0008 decision 1). Every code is listed, so a new one cannot become retryable or permanent by
# omission (a unit test checks it).
RETRYABLE: Final[Mapping[FailureCode, bool]] = MappingProxyType(
    {
        # The stored payload fails validation the same way on every attempt. A missing handler
        # (unknown_job_type) is a deployment error, but it is not retried either.
        FailureCode.UNKNOWN_JOB_TYPE: False,
        FailureCode.UNSUPPORTED_PAYLOAD_VERSION: False,
        FailureCode.PAYLOAD_TOO_LARGE: False,
        FailureCode.INVALID_PAYLOAD: False,
        # The handler's own error or timeout may not happen on the next attempt.
        FailureCode.HANDLER_ERROR: True,
        FailureCode.HANDLER_TIMEOUT: True,
        # Not the handler's own failure, but like a timeout the next attempt may not fail the same
        # way (ADR 0008 decision 1). Lease recovery requeues it without backoff.
        FailureCode.LEASE_EXPIRED: True,
    }
)
