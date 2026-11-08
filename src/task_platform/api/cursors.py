"""The opaque cursor of `GET /jobs?status=failed`: a page's last (updated_at, id), base64url."""

import base64
import binascii
import re
import uuid
from datetime import UTC, datetime
from typing import Final

# Longer than any cursor encode_cursor makes (about 90 characters), short enough to bound the work.
MAX_CURSOR_LENGTH: Final = 200

_ALPHABET: Final = re.compile(r"[A-Za-z0-9_-]+")


class InvalidCursorError(ValueError):
    """A cursor this API did not make. Carries no part of it."""


def encode_cursor(updated_at: datetime, job_id: uuid.UUID) -> str:
    raw = f"{updated_at.isoformat()}_{job_id}".encode("ascii")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    """The (updated_at, id) a cursor names. Raises InvalidCursorError for anything else."""
    # The checks below raise outside any `except` block, so the error carries no __context__
    # that holds the input (as in domain.payloads.parse_payload).
    decoded = _decode(cursor)
    if decoded is None or encode_cursor(*decoded) != cursor:
        # The round trip also refuses other spellings of a valid cursor, such as added padding.
        raise InvalidCursorError("cursor is not valid")
    return decoded


def _decode(cursor: str) -> tuple[datetime, uuid.UUID] | None:
    if len(cursor) > MAX_CURSOR_LENGTH or not _ALPHABET.fullmatch(cursor):
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode("ascii")
        timestamp, separator, job_id = raw.partition("_")
        updated_at = datetime.fromisoformat(timestamp)
        parsed = uuid.UUID(job_id)
        # asyncpg converts the time to UTC before sending it; a time outside UTC's range (year 1
        # at +14:00) would fail there as a database error, a 500 instead of this 422.
        updated_at.astimezone(UTC)
    except (binascii.Error, UnicodeDecodeError, ValueError, OverflowError):
        return None
    # A naive time would compare against timestamptz in the session's time zone.
    if not separator or updated_at.tzinfo is None:
        return None
    return updated_at, parsed
