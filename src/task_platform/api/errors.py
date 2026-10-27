"""The error envelope, API-level error codes and the exception handlers that produce them.

Every error response is `{"error": {"code", "message", "details"}}`. Messages and details never
echo client input.
"""

from collections.abc import Mapping
from enum import StrEnum
from http import HTTPStatus
from types import MappingProxyType
from typing import Final, cast

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from task_platform.api.schemas import ErrorBody, ErrorResponse
from task_platform.domain.errors import DomainError, ErrorCode, InvalidPayloadError


class ApiErrorCode(StrEnum):
    """Errors found by the HTTP layer itself, before a request reaches the services."""

    REQUEST_TOO_LARGE = "request_too_large"
    INVALID_REQUEST = "invalid_request"
    NOT_FOUND = "not_found"
    METHOD_NOT_ALLOWED = "method_not_allowed"
    INTERNAL_ERROR = "internal_error"


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        code: ApiErrorCode,
        message: str,
        details: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


DOMAIN_ERROR_STATUS: Final[Mapping[ErrorCode, int]] = MappingProxyType(
    {
        ErrorCode.UNKNOWN_JOB_TYPE: status.HTTP_422_UNPROCESSABLE_CONTENT,
        ErrorCode.UNSUPPORTED_PAYLOAD_VERSION: status.HTTP_422_UNPROCESSABLE_CONTENT,
        ErrorCode.INVALID_PAYLOAD: status.HTTP_422_UNPROCESSABLE_CONTENT,
        ErrorCode.PAYLOAD_TOO_LARGE: status.HTTP_413_CONTENT_TOO_LARGE,
        # Not raised by any endpoint yet; a request that conflicts with the job's state.
        ErrorCode.INVALID_TRANSITION: status.HTTP_409_CONFLICT,
    }
)


# Routing raises these two itself (unknown path, wrong method).
_HTTP_EXCEPTION_CODES: Final[Mapping[int, ApiErrorCode]] = MappingProxyType(
    {
        status.HTTP_404_NOT_FOUND: ApiErrorCode.NOT_FOUND,
        status.HTTP_405_METHOD_NOT_ALLOWED: ApiErrorCode.METHOD_NOT_ALLOWED,
    }
)


def error_response(
    status_code: int,
    code: str,
    message: str,
    details: tuple[str, ...] = (),
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    body = ErrorResponse(error=ErrorBody(code=code, message=message, details=list(details)))
    return JSONResponse(body.model_dump(), status_code=status_code, headers=headers)


# Starlette selects a handler by exception class, so each cast below only narrows the type.


async def _handle_api_error(request: Request, exc: Exception) -> JSONResponse:
    error = cast(ApiError, exc)
    return error_response(error.status_code, error.code, error.message, error.details)


async def _handle_domain_error(request: Request, exc: Exception) -> JSONResponse:
    error = cast(DomainError, exc)
    details = error.details if isinstance(error, InvalidPayloadError) else ()
    return error_response(DOMAIN_ERROR_STATUS[error.code], error.code, error.message, details)


async def _handle_http_exception(request: Request, exc: Exception) -> JSONResponse:
    error = cast(HTTPException, exc)
    fallback = (
        ApiErrorCode.INTERNAL_ERROR if error.status_code >= 500 else ApiErrorCode.INVALID_REQUEST
    )
    code = _HTTP_EXCEPTION_CODES.get(error.status_code, fallback)
    # The standard phrase, not exc.detail: a detail passed by other code could hold input. The
    # headers are kept, e.g. Allow on a 405.
    message = HTTPStatus(error.status_code).phrase.lower()
    return error_response(error.status_code, code, message, headers=error.headers)


async def _handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    # Starlette re-raises the exception after this response is sent, and uvicorn logs its
    # traceback (decision D7). So database errors, whose traceback holds PostgreSQL's DETAIL with
    # the payload in it, need class-specific handlers of their own: those do not re-raise.
    return error_response(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        ApiErrorCode.INTERNAL_ERROR,
        "internal server error",
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, _handle_api_error)
    app.add_exception_handler(DomainError, _handle_domain_error)
    # Replaces FastAPI's default, which answers {"detail": ...}. FastAPI's HTTPException is a
    # subclass, so it is covered too.
    app.add_exception_handler(HTTPException, _handle_http_exception)
    app.add_exception_handler(Exception, _handle_unexpected_error)
