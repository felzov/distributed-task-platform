import traceback
from typing import Any, cast

import pytest

from task_platform.domain.errors import (
    DomainError,
    ErrorCode,
    InvalidPayloadError,
    PayloadTooLargeError,
    UnknownJobTypeError,
    UnsupportedPayloadVersionError,
)
from task_platform.domain.payloads import (
    NOTIFICATION_SEND,
    PAYLOAD_MODELS,
    NotificationSendV1,
    PayloadModel,
    parse_payload,
    payload_size_bytes,
)

MAX_BYTES = 64 * 1024
SENTINEL = "s3cr3t-sentinel-7731"


def valid_payload(**overrides: Any) -> dict[str, Any]:
    return {"recipient": "max@example.com", "subject": "Hi", "body": "Hello"} | overrides


def parse(payload: object, job_type: str = NOTIFICATION_SEND, version: int = 1) -> PayloadModel:
    return parse_payload(job_type, version, payload, max_bytes=MAX_BYTES)


def invalid_details(payload: object) -> tuple[str, ...]:
    with pytest.raises(InvalidPayloadError) as exc_info:
        parse(payload)
    assert exc_info.value.code is ErrorCode.INVALID_PAYLOAD
    return exc_info.value.details


# Normal path


def test_valid_payload_returns_typed_model() -> None:
    result = parse(valid_payload())
    assert result == NotificationSendV1(recipient="max@example.com", subject="Hi", body="Hello")


def test_every_registered_model_uses_the_strict_config() -> None:
    # A future model that drops extra="forbid", strict or frozen (even by overriding
    # model_config in a subclass) fails here.
    for model in PAYLOAD_MODELS.values():
        assert issubclass(model, PayloadModel)
        assert model.model_config.get("extra") == "forbid"
        assert model.model_config.get("strict") is True
        assert model.model_config.get("frozen") is True


# Unknown type and version


def test_unknown_job_type_is_rejected() -> None:
    with pytest.raises(UnknownJobTypeError) as exc_info:
        parse(valid_payload(), job_type="report.generate")
    assert exc_info.value.code is ErrorCode.UNKNOWN_JOB_TYPE


@pytest.mark.parametrize("version", [0, 2, -1])
def test_unknown_version_of_known_type_is_rejected(version: int) -> None:
    with pytest.raises(UnsupportedPayloadVersionError) as exc_info:
        parse(valid_payload(), version=version)
    assert exc_info.value.code is ErrorCode.UNSUPPORTED_PAYLOAD_VERSION
    assert exc_info.value.job_type == NOTIFICATION_SEND


def test_bool_version_is_not_treated_as_version_1() -> None:
    with pytest.raises(UnsupportedPayloadVersionError):
        parse(valid_payload(), version=True)


@pytest.mark.parametrize("version", [1.0, "1", [1], None], ids=["float", "str", "list", "none"])
def test_non_int_version_is_rejected(version: object) -> None:
    # The version comes from untrusted input: 1.0 == 1 must not select version 1, and an
    # unhashable value must give a domain error, not a TypeError.
    with pytest.raises(UnsupportedPayloadVersionError):
        parse_payload(NOTIFICATION_SEND, cast(int, version), valid_payload(), max_bytes=MAX_BYTES)


# Schema violations


def test_missing_field_is_reported_by_name() -> None:
    payload = valid_payload()
    del payload["subject"]
    assert invalid_details(payload) == ("subject: missing",)


def test_unexpected_field_is_rejected_without_naming_it() -> None:
    assert invalid_details(valid_payload(cc="someone@example.com")) == ("unexpected field",)


def test_non_string_key_is_rejected_without_naming_it() -> None:
    payload: dict[object, Any] = {1: "x"}
    for key, value in valid_payload().items():
        payload[key] = value
    assert invalid_details(payload) == ("unexpected field",)


@pytest.mark.parametrize("field", ["recipient", "subject", "body"])
def test_nul_character_is_rejected(field: str) -> None:
    # PostgreSQL text and jsonb cannot store U+0000: reject it here, not with a failed INSERT.
    assert invalid_details(valid_payload(**{field: "a\x00b"})) == (f"{field}: value_error",)


def test_wrong_type_is_not_coerced() -> None:
    assert invalid_details(valid_payload(subject=123)) == ("subject: string_type",)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("recipient", "", "string_too_short"),
        ("recipient", "a" * 321, "string_too_long"),
        ("subject", "a" * 201, "string_too_long"),
        ("body", "a" * 10_001, "string_too_long"),
    ],
)
def test_field_length_limits(field: str, value: str, error: str) -> None:
    assert invalid_details(valid_payload(**{field: value})) == (f"{field}: {error}",)


@pytest.mark.parametrize("payload", [[], "text", 42, None])
def test_payload_must_be_an_object(payload: object) -> None:
    assert invalid_details(payload) == ("payload: model_type",)


@pytest.mark.parametrize("payload", [{"body": float("nan")}, {"body": {1, 2}}])
def test_non_json_payload_is_rejected(payload: object) -> None:
    assert invalid_details(payload) == ("payload: not_json",)


# Size limit


def test_payload_exactly_at_the_limit_is_accepted() -> None:
    payload = valid_payload()
    size = payload_size_bytes(payload)
    assert parse_payload(NOTIFICATION_SEND, 1, payload, max_bytes=size) is not None


def test_payload_one_byte_over_the_limit_is_rejected() -> None:
    payload = valid_payload()
    size = payload_size_bytes(payload)
    with pytest.raises(PayloadTooLargeError) as exc_info:
        parse_payload(NOTIFICATION_SEND, 1, payload, max_bytes=size - 1)
    assert exc_info.value.code is ErrorCode.PAYLOAD_TOO_LARGE
    assert exc_info.value.max_bytes == size - 1


def test_size_is_measured_in_utf8_bytes_not_characters() -> None:
    # '{"a":"é"}' is 9 characters but 10 bytes.
    assert payload_size_bytes({"a": "é"}) == 10


def test_size_check_runs_before_schema_validation() -> None:
    # An oversized payload is rejected for its size even if it also breaks the schema.
    with pytest.raises(PayloadTooLargeError):
        parse_payload(NOTIFICATION_SEND, 1, {"junk": "x" * 100}, max_bytes=50)


# Errors never echo input


@pytest.mark.parametrize(
    ("job_type", "version", "payload", "max_bytes"),
    [
        (SENTINEL, 1, valid_payload(), MAX_BYTES),
        (NOTIFICATION_SEND, 2, valid_payload(body=SENTINEL), MAX_BYTES),
        (NOTIFICATION_SEND, 1, {"body": {SENTINEL}}, MAX_BYTES),
        (NOTIFICATION_SEND, 1, valid_payload(body=SENTINEL), 10),
        (NOTIFICATION_SEND, 1, valid_payload(**{SENTINEL: SENTINEL}), MAX_BYTES),
        (NOTIFICATION_SEND, 1, valid_payload(recipient=SENTINEL * 20), MAX_BYTES),
        (NOTIFICATION_SEND, 1, valid_payload(subject=[SENTINEL]), MAX_BYTES),
        (NOTIFICATION_SEND, 1, valid_payload(body=f"{SENTINEL}\x00"), MAX_BYTES),
        (NOTIFICATION_SEND, 1, [SENTINEL], MAX_BYTES),
    ],
    ids=[
        "job_type",
        "unsupported_version",
        "not_json",
        "too_large",
        "extra_field",
        "too_long",
        "wrong_type",
        "nul",
        "not_object",
    ],
)
def test_errors_never_contain_input_values(
    job_type: str, version: int, payload: object, max_bytes: int
) -> None:
    with pytest.raises(DomainError) as exc_info:
        parse_payload(job_type, version, payload, max_bytes=max_bytes)
    error = exc_info.value
    # The full traceback is what a logger would print, including any chained exception.
    rendered = "".join(traceback.format_exception(error))
    assert SENTINEL not in rendered
    assert SENTINEL not in repr(error)
    assert all(SENTINEL not in detail for detail in getattr(error, "details", ()))
    # No chained exception at all: tools that walk __context__ (Sentry, loguru with
    # diagnose=True) would otherwise reach the original error and its input values.
    assert error.__cause__ is None
    assert error.__context__ is None
