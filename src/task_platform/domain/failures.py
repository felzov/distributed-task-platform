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


# What `GET /jobs/{id}` says about each code: fixed sentences, nothing taken from the job.
FAILURE_MESSAGES: Final[Mapping[FailureCode, str]] = MappingProxyType(
    {
        FailureCode.UNKNOWN_JOB_TYPE: "no handler exists for the job's type",
        FailureCode.UNSUPPORTED_PAYLOAD_VERSION: "the job's payload version is not supported",
        FailureCode.PAYLOAD_TOO_LARGE: "the job's payload exceeds the size limit",
        FailureCode.INVALID_PAYLOAD: "the job's payload is not valid for its type",
        FailureCode.HANDLER_ERROR: "the job's handler raised an error",
        FailureCode.HANDLER_TIMEOUT: "the job's handler did not finish in time",
    }
)

# For a stored code this code does not know, e.g. one written by a newer worker.
UNKNOWN_FAILURE_MESSAGE: Final = "the job failed"
