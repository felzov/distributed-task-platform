"""Why a job failed: the fixed codes stored in `jobs.error_code` (ADR 0004 decision 6).

Separate from `ErrorCode`, which names errors a client causes and gets an HTTP status for. These
codes describe an attempt that already ran. Never exception text: it can quote the payload.
"""

from enum import StrEnum


class FailureCode(StrEnum):
    # The stored payload no longer passes validation (same names as the API's errors).
    UNKNOWN_JOB_TYPE = "unknown_job_type"
    UNSUPPORTED_PAYLOAD_VERSION = "unsupported_payload_version"
    PAYLOAD_TOO_LARGE = "payload_too_large"
    INVALID_PAYLOAD = "invalid_payload"
    # The handler raised, or did not finish within handler_timeout_s.
    HANDLER_ERROR = "handler_error"
    HANDLER_TIMEOUT = "handler_timeout"
