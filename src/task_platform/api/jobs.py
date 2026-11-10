"""`POST /jobs` (key format, size-capped body, envelope check, then the service),
`GET /jobs?status=failed`, `GET /jobs/{id}` and `GET /jobs/{id}/attempts`."""

import uuid
from datetime import datetime
from typing import Annotated, Any, Final, Literal

from fastapi import APIRouter, Query, Request, Response, status
from pydantic import ValidationError

from task_platform.api.cursors import (
    MAX_CURSOR_LENGTH,
    InvalidCursorError,
    decode_cursor,
    encode_cursor,
)
from task_platform.api.dependencies import AppSettings, SessionFactory
from task_platform.api.errors import DATABASE_ERROR_RESPONSES, ApiError, ApiErrorCode
from task_platform.api.schemas import (
    AttemptBody,
    AttemptsResponse,
    ErrorResponse,
    JobListResponse,
    JobResponse,
    SubmitJobRequest,
    SubmitJobResponse,
)
from task_platform.domain.idempotency import MAX_IDEMPOTENCY_KEY_LENGTH, is_valid_idempotency_key
from task_platform.domain.payloads import safe_error_details
from task_platform.services.jobs import get_attempts, get_job, list_failed_jobs, submit_job

router = APIRouter()

IDEMPOTENCY_KEY_HEADER: Final = "Idempotency-Key"

# The body and the Idempotency-Key header are read by hand (to cap the body's size before parsing,
# and to see a repeated header), so FastAPI cannot infer either.
_REQUEST_DOC: Final[dict[str, Any]] = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": SubmitJobRequest.model_json_schema()}},
    },
    "parameters": [
        {
            "name": IDEMPOTENCY_KEY_HEADER,
            "in": "header",
            "required": False,
            "description": (
                "Makes a retry safe (ADR 0007): a key not used before saves the job (202); the"
                " same key with the same type, version and payload returns that job (200); with"
                " anything else, 422 idempotency_key_reused. Use a random value such as a UUIDv4;"
                " keys never expire and must not carry secrets."
            ),
            "schema": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_IDEMPOTENCY_KEY_LENGTH,
                "pattern": "^[\\x21-\\x7E]+$",
            },
        }
    ],
}


def read_idempotency_key(request: Request) -> str | None:
    """The request's Idempotency-Key, None without one, or a 422 if it is not a usable key.

    Reads only the headers, so a bad key is refused before the body is read (ADR 0007 decision 7).
    """
    values = request.headers.getlist(IDEMPOTENCY_KEY_HEADER)
    if not values:
        return None
    # The detail names only the header and the problem, never the value.
    if len(values) > 1:
        problem = "repeated_header"
    elif not is_valid_idempotency_key(values[0]):
        problem = "invalid_idempotency_key"
    else:
        return values[0]
    raise ApiError(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        ApiErrorCode.INVALID_REQUEST,
        "request is not valid",
        (f"header.{IDEMPOTENCY_KEY_HEADER}: {problem}",),
    )


async def read_capped_body(request: Request, max_bytes: int) -> bytes:
    """Return the request body, or raise 413 as soon as it exceeds `max_bytes`."""
    # Content-Length lets an honest oversized request fail without being read. It is only a
    # shortcut: the count below is what enforces the limit, also for chunked bodies.
    try:
        declared = int(request.headers.get("content-length", "0"))
    except ValueError:
        declared = 0
    if declared > max_bytes:
        raise _request_too_large(max_bytes)

    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > max_bytes:
            raise _request_too_large(max_bytes)
    return bytes(body)


def _request_too_large(max_bytes: int) -> ApiError:
    return ApiError(
        status.HTTP_413_CONTENT_TOO_LARGE,
        ApiErrorCode.REQUEST_TOO_LARGE,
        f"request body exceeds the limit of {max_bytes} bytes",
    )


def parse_envelope(body: bytes) -> SubmitJobRequest:
    # The ApiError is raised outside the `except` block so that it carries no __context__: the
    # ValidationError holds the input (same reasoning as domain.payloads.parse_payload).
    try:
        return SubmitJobRequest.model_validate_json(body)
    except ValidationError as exc:
        details = safe_error_details(
            exc.errors(include_url=False, include_input=False), root="body"
        )
    raise ApiError(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        ApiErrorCode.INVALID_REQUEST,
        "request body is not a valid job submission",
        details,
    )


@router.post(
    "/jobs",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {
            "model": SubmitJobResponse,
            "description": "The Idempotency-Key names a job saved earlier: its current status",
        },
        status.HTTP_413_CONTENT_TOO_LARGE: {"model": ErrorResponse},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
        **DATABASE_ERROR_RESPONSES,
    },
    openapi_extra=_REQUEST_DOC,
)
async def post_job(
    request: Request,
    response: Response,
    settings: AppSettings,
    session_factory: SessionFactory,
) -> SubmitJobResponse:
    """Save a job. 202 means saved as `queued`, not dispatched (ADR 0002).

    200 means nothing new was saved: the Idempotency-Key names an earlier job (ADR 0007).
    """
    # Cheapest check first: the key needs only the headers (ADR 0007 decision 7).
    idempotency_key = read_idempotency_key(request)
    envelope = parse_envelope(await read_capped_body(request, settings.max_request_body_bytes))
    job = await submit_job(
        session_factory,
        settings,
        job_type=envelope.type,
        payload_version=envelope.payload_version,
        payload=envelope.payload,
        idempotency_key=idempotency_key,
    )
    if not job.created:
        response.status_code = status.HTTP_200_OK
    response.headers["Location"] = f"/jobs/{job.id}"
    return SubmitJobResponse(id=job.id, status=job.status)


# Page sizes of the job list; placeholders, not measured.
DEFAULT_PAGE_SIZE: Final = 50
MAX_PAGE_SIZE: Final = 100


@router.get(
    "/jobs",
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
        **DATABASE_ERROR_RESPONSES,
    },
)
async def list_jobs(
    session_factory: SessionFactory,
    # Named apart from fastapi's `status` module; only failed jobs can be listed so far.
    job_status: Annotated[Literal["failed"], Query(alias="status")],
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: Annotated[str | None, Query(max_length=MAX_CURSOR_LENGTH)] = None,
) -> JobListResponse:
    """Failed jobs, newest first, without their payloads. Pass `next_cursor` for the next page."""
    after = None if cursor is None else _decode_cursor(cursor)
    page = await list_failed_jobs(session_factory, limit=limit, after=after)
    return JobListResponse(
        jobs=[JobResponse.model_validate(job) for job in page.jobs],
        next_cursor=None if page.next_after is None else encode_cursor(*page.next_after),
    )


def _decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    # Raised outside the `except` block, so the ApiError carries no __context__ (as in
    # parse_envelope); the detail has the same "<location>: <type>" form as FastAPI's own 422s.
    try:
        return decode_cursor(cursor)
    except InvalidCursorError:
        pass
    raise ApiError(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        ApiErrorCode.INVALID_REQUEST,
        "request is not valid",
        ("query.cursor: invalid_cursor",),
    )


@router.get(
    "/jobs/{job_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
        # Listing 422 here also replaces FastAPI's own HTTPValidationError schema in the docs.
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
        **DATABASE_ERROR_RESPONSES,
    },
)
async def read_job(job_id: uuid.UUID, session_factory: SessionFactory) -> JobResponse:
    """A job's current state, without its payload. An invalid UUID never reaches the database."""
    return JobResponse.model_validate(await get_job(session_factory, job_id))


@router.get(
    "/jobs/{job_id}/attempts",
    responses={
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
        **DATABASE_ERROR_RESPONSES,
    },
)
async def read_attempts(job_id: uuid.UUID, session_factory: SessionFactory) -> AttemptsResponse:
    """A job's recorded attempts, lowest first.

    Empty until the job is first claimed, and for a job that ran before migration 0003.
    """
    attempts = await get_attempts(session_factory, job_id)
    return AttemptsResponse(attempts=[AttemptBody.model_validate(a) for a in attempts])
