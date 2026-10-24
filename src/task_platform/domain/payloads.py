"""Typed, versioned job payloads and their validation.

Every (job type, payload version) pair maps to one Pydantic model. Unknown types or versions,
oversized payloads and schema violations raise domain errors whose messages and details never
contain input values.
"""

import json
from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from task_platform.domain.errors import (
    InvalidPayloadError,
    PayloadTooLargeError,
    UnknownJobTypeError,
    UnsupportedPayloadVersionError,
)


class PayloadModel(BaseModel):
    """Base for all payload schemas: no unknown fields, no type coercion, immutable."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    @field_validator("*")
    @classmethod
    def _reject_nul(cls, value: object) -> object:
        # PostgreSQL text and jsonb cannot store U+0000; reject it here, not with a failed INSERT.
        if isinstance(value, str) and "\x00" in value:
            raise ValueError("contains a NUL character")
        return value


class NotificationSendV1(PayloadModel):
    """A notification for the fake provider, which records it instead of sending it."""

    # 320 is the maximum length of an email address (64 local + "@" + 255 domain).
    recipient: str = Field(min_length=1, max_length=320)
    subject: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=10_000)


NOTIFICATION_SEND: Final = "notification.send"

PAYLOAD_MODELS: Final[Mapping[tuple[str, int], type[PayloadModel]]] = MappingProxyType(
    {
        (NOTIFICATION_SEND, 1): NotificationSendV1,
    }
)


def payload_size_bytes(payload: object) -> int:
    """Size of the payload as compact UTF-8 JSON. Raises TypeError/ValueError if not JSON."""
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return len(encoded.encode())


def parse_payload(job_type: str, version: int, payload: object, *, max_bytes: int) -> PayloadModel:
    """Validate a payload against its schema and return the typed model.

    Checks run from cheapest to most expensive: job type, version, size, schema.
    """
    if not any(known_type == job_type for known_type, _ in PAYLOAD_MODELS):
        raise UnknownJobTypeError()
    # Exactly int: bool (True == 1) and float (1.0 == 1) would otherwise select version 1, and an
    # unhashable value such as a list would raise TypeError from the lookup.
    model = PAYLOAD_MODELS.get((job_type, version)) if type(version) is int else None
    if model is None:
        raise UnsupportedPayloadVersionError(job_type)

    # Domain errors are raised outside the `except` blocks, so they carry no __context__: the
    # original exceptions contain input values, and `from None` only hides them from printed
    # tracebacks, not from tools that walk the exception chain.
    size: int | None
    try:
        size = payload_size_bytes(payload)
    except (TypeError, ValueError):
        size = None
    if size is None:
        raise InvalidPayloadError(("payload: not_json",))
    if size > max_bytes:
        raise PayloadTooLargeError(max_bytes)

    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        details = _safe_details(exc)
    raise InvalidPayloadError(details)


def _safe_details(exc: ValidationError) -> tuple[str, ...]:
    details = []
    for error in exc.errors(include_url=False, include_input=False):
        if error["type"] in {"extra_forbidden", "invalid_key"}:
            # The location is the client's own key, which is input too.
            details.append("unexpected field")
        else:
            # Payload models are flat, so every other location is a schema field name (or empty
            # when the payload itself has the wrong type). Revisit if a model gains dict fields.
            location = ".".join(str(part) for part in error["loc"]) or "payload"
            details.append(f"{location}: {error['type']}")
    return tuple(details)
