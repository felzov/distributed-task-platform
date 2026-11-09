"""Idempotency keys for job submission and the request fingerprint stored with them (ADR 0007).

Pure functions, no I/O. The key's format is checked here; what a used key means is decided by the
services layer, which compares fingerprints.
"""

import hashlib
import json
import re
from typing import Any, Final

# ADR 0007 decision 2. Migration 0005's CHECK repeats the length; the characters are checked only
# here.
MAX_IDEMPOTENCY_KEY_LENGTH: Final = 255
# Visible ASCII, 0x21-0x7E: no space, no control character, nothing outside ASCII. Starlette
# decodes header bytes as Latin-1, so a non-ASCII byte arrives as a character above 0x7E.
_IDEMPOTENCY_KEY: Final = re.compile(rf"[\x21-\x7e]{{1,{MAX_IDEMPOTENCY_KEY_LENGTH}}}")


def is_valid_idempotency_key(value: str) -> bool:
    """Whether `value` is a usable key: 1 to 255 visible ASCII characters, compared as is."""
    return _IDEMPOTENCY_KEY.fullmatch(value) is not None


def request_fingerprint(job_type: str, payload_version: int, payload: dict[str, Any]) -> bytes:
    """The SHA-256 of a submission as the API parsed it, to tell a retry from a reused key.

    Call it only after the payload passed validation, so `payload` is JSON. Whitespace and key
    order in the request do not change the result (ADR 0007 decision 3). The result is 32 bytes,
    as migration 0005's CHECK requires.

    Fingerprints are stored with their keys, which never expire: changing any argument below
    turns honest retries of keys saved earlier into a reuse. A unit test pins the exact form.
    """
    canonical = json.dumps(
        {"type": job_type, "payload_version": payload_version, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).digest()
