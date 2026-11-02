import traceback
import uuid

import pytest

from task_platform.domain.messages import (
    MAX_MESSAGE_BYTES,
    InvalidMessageError,
    encode_job_message,
    parse_job_message,
)

JOB_ID = uuid.UUID("0b7a5e0c-3f1e-4c8e-9d1a-2b6f4e8c7a10")
VALID = b'{"message_version":1,"job_id":"0b7a5e0c-3f1e-4c8e-9d1a-2b6f4e8c7a10"}'
SENTINEL = "s3cr3t-sentinel-7731"


def invalid_details(body: bytes) -> tuple[str, ...]:
    with pytest.raises(InvalidMessageError) as exc_info:
        parse_job_message(body)
    return exc_info.value.details


# Normal path


def test_encoded_body_is_exactly_the_contract() -> None:
    # The bytes a worker of any later version must still understand (ADR 0004 decision 4).
    assert encode_job_message(JOB_ID) == VALID


def test_an_encoded_message_parses_back() -> None:
    message = parse_job_message(encode_job_message(JOB_ID))

    assert message.message_version == 1
    assert message.job_id == JOB_ID


def test_whitespace_is_allowed_up_to_the_size_cap() -> None:
    body = b'{ "message_version": 1, "job_id": "0b7a5e0c-3f1e-4c8e-9d1a-2b6f4e8c7a10" }'

    assert parse_job_message(body.ljust(MAX_MESSAGE_BYTES)).job_id == JOB_ID


# Invalid input


@pytest.mark.parametrize(
    ("body", "details"),
    [
        pytest.param(b"not json", ("message: json_invalid",), id="not JSON"),
        pytest.param(b"", ("message: json_invalid",), id="empty"),
        pytest.param(b"\xff\xfe", ("message: json_invalid",), id="not UTF-8"),
        pytest.param(b"[1]", ("message: model_type",), id="not an object"),
        pytest.param(b'{"message_version":1}', ("job_id: missing",), id="no job id"),
        pytest.param(
            b'{"job_id":"0b7a5e0c-3f1e-4c8e-9d1a-2b6f4e8c7a10"}',
            ("message_version: missing",),
            id="no version",
        ),
        pytest.param(
            b'{"message_version":2,"job_id":"0b7a5e0c-3f1e-4c8e-9d1a-2b6f4e8c7a10"}',
            ("message_version: literal_error",),
            id="unknown version",
        ),
        pytest.param(
            b'{"message_version":"1","job_id":"0b7a5e0c-3f1e-4c8e-9d1a-2b6f4e8c7a10"}',
            ("message_version: value_error",),
            id="version as a string",
        ),
        # Equal to 1 in Python, so Literal[1] alone would accept both.
        pytest.param(
            b'{"message_version":true,"job_id":"0b7a5e0c-3f1e-4c8e-9d1a-2b6f4e8c7a10"}',
            ("message_version: value_error",),
            id="version as a boolean",
        ),
        pytest.param(
            b'{"message_version":1.0,"job_id":"0b7a5e0c-3f1e-4c8e-9d1a-2b6f4e8c7a10"}',
            ("message_version: value_error",),
            id="version as a float",
        ),
        pytest.param(
            b'{"message_version":1,"job_id":"not-a-uuid"}', ("job_id: uuid_parsing",), id="bad id"
        ),
        pytest.param(
            b'{"message_version":1,"job_id":7}', ("job_id: uuid_type",), id="id as a number"
        ),
        pytest.param(
            b'{"message_version":1,"job_id":"0b7a5e0c-3f1e-4c8e-9d1a-2b6f4e8c7a10","x":1}',
            ("unexpected field",),
            id="extra field",
        ),
    ],
)
def test_invalid_bodies_are_rejected(body: bytes, details: tuple[str, ...]) -> None:
    assert invalid_details(body) == details


def test_a_body_over_the_cap_is_rejected_before_parsing() -> None:
    # Valid JSON otherwise: only the size makes it invalid.
    assert invalid_details(VALID.ljust(MAX_MESSAGE_BYTES + 1)) == ("message: too_large",)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(f'{{"message_version":1,"job_id":"{SENTINEL}"}}'.encode(), id="bad id"),
        pytest.param(
            f'{{"message_version":1,"job_id":"{JOB_ID}","{SENTINEL}":1}}'.encode(), id="extra key"
        ),
        pytest.param(f'{{"message_version":"{SENTINEL}"}}'.encode(), id="bad version"),
        pytest.param(f"{SENTINEL} not json".encode(), id="not JSON"),
        pytest.param(f'["{SENTINEL}"]'.encode(), id="not an object"),
    ],
)
def test_errors_never_contain_body_values(body: bytes) -> None:
    with pytest.raises(InvalidMessageError) as exc_info:
        parse_job_message(body)
    error = exc_info.value
    # What a logger would print, chained exceptions included.
    assert SENTINEL not in "".join(traceback.format_exception(error))
    assert SENTINEL not in repr(error)
    assert all(SENTINEL not in detail for detail in error.details)
    assert error.__cause__ is None
    assert error.__context__ is None
