import hashlib
import json

import pytest

from task_platform.domain.idempotency import (
    MAX_IDEMPOTENCY_KEY_LENGTH,
    is_valid_idempotency_key,
    request_fingerprint,
)

PAYLOAD = {"recipient": "max@example.com", "subject": "Hi", "body": "Hello"}

# Normal path: keys


@pytest.mark.parametrize(
    "key",
    [
        "a",
        "x" * MAX_IDEMPOTENCY_KEY_LENGTH,
        "8e03978e-40d5-43e8-bc93-6894a57f9324",
        # A key as the IETF draft writes it (a quoted string): the quotes are part of the key.
        '"8e03978e-40d5-43e8-bc93-6894a57f9324"',
        # The whole allowed range, 0x21 to 0x7E.
        "".join(chr(code) for code in range(0x21, 0x7F)),
    ],
)
def test_visible_ascii_keys_up_to_255_characters_are_valid(key: str) -> None:
    assert is_valid_idempotency_key(key)


# Invalid input: keys


@pytest.mark.parametrize(
    "key",
    [
        "",
        "x" * (MAX_IDEMPOTENCY_KEY_LENGTH + 1),
        "two words",
        " leading",
        "trailing ",
        "tab\there",
        "new\nline",
        # Python's "$" would match before this newline; fullmatch does not.
        "trailing\n",
        "nul\x00",
        "del\x7f",
        # A UTF-8 "é" as Starlette hands it over: its bytes decoded as Latin-1.
        "caf\xc3\xa9",
        "café",
        "ключ",
    ],
)
def test_other_keys_are_invalid(key: str) -> None:
    assert not is_valid_idempotency_key(key)


# Normal path: fingerprints


def test_the_fingerprint_is_a_sha256_of_this_exact_canonical_form() -> None:
    # Fingerprints are stored and never expire (ADR 0007 decision 3), so the form is pinned here
    # by hand, not by calling json.dumps: sorted keys at every level, no spaces.
    canonical = (
        '{"payload":{"body":"Hello","recipient":"max@example.com","subject":"Hi"},'
        '"payload_version":1,"type":"notification.send"}'
    )
    expected = hashlib.sha256(canonical.encode("utf-8")).digest()

    assert request_fingerprint("notification.send", 1, PAYLOAD) == expected
    assert len(expected) == 32


def test_non_ascii_text_is_hashed_as_utf8_not_as_escapes() -> None:
    # ensure_ascii=False: "é" is hashed as its two UTF-8 bytes, never as the escape "\u00e9".
    canonical = '{"payload":{"body":"café ключ"},"payload_version":1,"type":"t"}'

    assert (
        request_fingerprint("t", 1, {"body": "café ключ"})
        == hashlib.sha256(canonical.encode("utf-8")).digest()
    )


def test_key_order_and_whitespace_in_the_request_do_not_matter() -> None:
    first = json.loads('{"recipient":"max@example.com","subject":"Hi","body":"Hello"}')
    second = json.loads('{ "body" : "Hello",\n  "subject": "Hi", "recipient": "max@example.com" }')

    assert request_fingerprint("notification.send", 1, first) == request_fingerprint(
        "notification.send", 1, second
    )


def test_nested_objects_are_sorted_too() -> None:
    assert request_fingerprint("t", 1, {"a": {"y": 1, "x": 2}}) == request_fingerprint(
        "t", 1, {"a": {"x": 2, "y": 1}}
    )


# Invalid input: a different request under the same key


@pytest.mark.parametrize(
    ("job_type", "payload_version", "payload"),
    [
        ("notification.other", 1, PAYLOAD),
        ("notification.send", 2, PAYLOAD),
        ("notification.send", 1, PAYLOAD | {"body": "Hello!"}),
        ("notification.send", 1, PAYLOAD | {"extra": None}),
        ("notification.send", 1, {k: v for k, v in PAYLOAD.items() if k != "body"}),
    ],
)
def test_any_change_to_type_version_or_payload_changes_the_fingerprint(
    job_type: str, payload_version: int, payload: dict[str, object]
) -> None:
    original = request_fingerprint("notification.send", 1, PAYLOAD)

    assert request_fingerprint(job_type, payload_version, payload) != original


def test_a_value_moved_between_fields_changes_the_fingerprint() -> None:
    # The fields are hashed as one JSON object, not concatenated, so values cannot shift between
    # them and collide.
    assert request_fingerprint("t", 1, {"a": "bc"}) != request_fingerprint("t", 1, {"ab": "c"})
