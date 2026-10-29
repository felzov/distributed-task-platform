"""`POST /jobs` (size-capped body, envelope check, then the service) and `GET /jobs/{id}`."""

import uuid
from typing import Any, Final

from fastapi import APIRouter, Request, Response, status
from pydantic import ValidationError

from task_platform.api.dependencies import AppSettings, SessionFactory
from task_platform.api.errors import DATABASE_ERROR_RESPONSES, ApiError, ApiErrorCode
from task_platform.api.schemas import (
    ErrorResponse,
    JobResponse,
    SubmitJobRequest,
    SubmitJobResponse,
)
from task_platform.domain.payloads import safe_error_details
from task_platform.services.jobs import get_job, submit_job

router = APIRouter()

# The body is read by hand (to cap its size before parsing), so FastAPI cannot infer its schema.
_REQUEST_BODY_DOC: Final[dict[str, Any]] = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": SubmitJobRequest.model_json_schema()}},
    }
}


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
        status.HTTP_413_CONTENT_TOO_LARGE: {"model": ErrorResponse},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
        **DATABASE_ERROR_RESPONSES,
    },
    openapi_extra=_REQUEST_BODY_DOC,
)
async def post_job(
    request: Request,
    response: Response,
    settings: AppSettings,
    session_factory: SessionFactory,
) -> SubmitJobResponse:
    """Save a job. 202 means saved as `queued`, not dispatched (ADR 0002)."""
    envelope = parse_envelope(await read_capped_body(request, settings.max_request_body_bytes))
    job = await submit_job(
        session_factory,
        settings,
        job_type=envelope.type,
        payload_version=envelope.payload_version,
        payload=envelope.payload,
    )
    response.headers["Location"] = f"/jobs/{job.id}"
    return SubmitJobResponse(id=job.id, status=job.status)


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
