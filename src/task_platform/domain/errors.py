"""Domain errors with stable, safe error codes.

Messages may name domain values (such as job statuses) but never echo client input or payloads.
"""

from enum import StrEnum
from typing import ClassVar


class ErrorCode(StrEnum):
    INVALID_TRANSITION = "invalid_transition"
    UNKNOWN_JOB_TYPE = "unknown_job_type"
    UNSUPPORTED_PAYLOAD_VERSION = "unsupported_payload_version"
    PAYLOAD_TOO_LARGE = "payload_too_large"
    INVALID_PAYLOAD = "invalid_payload"
    JOB_NOT_FOUND = "job_not_found"


class DomainError(Exception):
    """Base class for domain errors; `code` is stable and safe to expose."""

    code: ClassVar[ErrorCode]

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class InvalidTransitionError(DomainError):
    code = ErrorCode.INVALID_TRANSITION

    def __init__(self, current: str, target: str) -> None:
        # str(), not repr(): a StrEnum's repr would leak "<JobStatus.RUNNING: 'running'>".
        super().__init__(f"transition from '{current}' to '{target}' is not allowed")
        self.current = current
        self.target = target


class UnknownJobTypeError(DomainError):
    code = ErrorCode.UNKNOWN_JOB_TYPE

    def __init__(self) -> None:
        # The unknown type is client input, so it is not repeated here.
        super().__init__("unknown job type")


class UnsupportedPayloadVersionError(DomainError):
    code = ErrorCode.UNSUPPORTED_PAYLOAD_VERSION

    def __init__(self, job_type: str) -> None:
        # Only raised for a known job type, so naming it is safe.
        super().__init__(f"payload version is not supported for job type '{job_type}'")
        self.job_type = job_type


class PayloadTooLargeError(DomainError):
    code = ErrorCode.PAYLOAD_TOO_LARGE

    def __init__(self, max_bytes: int) -> None:
        super().__init__(f"payload exceeds the limit of {max_bytes} bytes")
        self.max_bytes = max_bytes


class InvalidPayloadError(DomainError):
    code = ErrorCode.INVALID_PAYLOAD

    def __init__(self, details: tuple[str, ...]) -> None:
        super().__init__("payload does not match the schema for this job type")
        # Each detail is "<schema field>: <error type>"; never an input value.
        self.details = details


class JobNotFoundError(DomainError):
    code = ErrorCode.JOB_NOT_FOUND

    def __init__(self) -> None:
        # The id is client input, so it is not repeated here.
        super().__init__("job not found")
