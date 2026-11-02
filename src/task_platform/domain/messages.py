"""The broker message for a job: its id and nothing else (ADR 0002 decision 3, ADR 0004).

The message proves nothing about the job; the worker reads the job's state from PostgreSQL. Bodies
that do not parse to a version 1 message (no extra fields, no type coercion) raise
`InvalidMessageError`, whose details never contain a value from the body. Spelling is not checked:
whitespace, key order and any UUID form Python accepts all parse to the same message. It is not a
`DomainError`: no client sends or sees a message, and an unparsable message has no job whose
`error_code` it could become.
"""

import uuid
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from task_platform.domain.payloads import safe_error_details

MESSAGE_VERSION: Final = 1
# A version 1 body is 69 bytes. Part of the contract, not a tuning value, so not a setting: the
# cap only stops the worker from parsing an arbitrarily large body that did not come from us.
MAX_MESSAGE_BYTES: Final = 1024


class InvalidMessageError(Exception):
    def __init__(self, details: tuple[str, ...]) -> None:
        super().__init__("broker message is not a valid job message")
        # Each detail is "<field>: <error type>"; never a value from the message body.
        self.details = details


class JobMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    message_version: Literal[1]
    job_id: uuid.UUID

    @field_validator("message_version", mode="before")
    @classmethod
    def _exactly_int(cls, value: object) -> object:
        # Literal compares by equality even in strict mode, so JSON `true` and `1.0` would pass.
        if type(value) is not int:
            raise ValueError("must be an integer")
        return value


def encode_job_message(job_id: uuid.UUID) -> bytes:
    """The message body: compact UTF-8 JSON, `message_version` first."""
    return JobMessage(message_version=MESSAGE_VERSION, job_id=job_id).model_dump_json().encode()


def parse_job_message(body: bytes) -> JobMessage:
    """Parse a message body strictly: no extra fields, no coercion, version 1 only."""
    if len(body) > MAX_MESSAGE_BYTES:
        raise InvalidMessageError(("message: too_large",))
    # Raised outside the `except`, so the error carries no __context__ with the body in it (as in
    # parse_payload).
    try:
        return JobMessage.model_validate_json(body)
    except ValidationError as exc:
        details = safe_error_details(
            exc.errors(include_url=False, include_input=False), root="message"
        )
    raise InvalidMessageError(details)
