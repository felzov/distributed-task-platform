"""The error envelope, API-level error codes and the exception handlers that produce them.

Every error response is `{"error": {"code", "message", "details"}}`. Messages and details never
echo client input, and database errors are logged as their class, SQLSTATE and constraint name
only.
"""

import logging
from collections.abc import Mapping
from enum import StrEnum
from http import HTTPStatus
from types import MappingProxyType
from typing import Final, cast

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from task_platform.api.schemas import ErrorBody, ErrorResponse
from task_platform.domain.errors import DomainError, ErrorCode, InvalidPayloadError
from task_platform.domain.payloads import safe_error_details

logger = logging.getLogger(__name__)


class ApiErrorCode(StrEnum):
    """Errors found by the HTTP layer itself, before a request reaches the services."""

    REQUEST_TOO_LARGE = "request_too_large"
    INVALID_REQUEST = "invalid_request"
    NOT_FOUND = "not_found"
    METHOD_NOT_ALLOWED = "method_not_allowed"
    SERVICE_UNAVAILABLE = "service_unavailable"
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
        ErrorCode.JOB_NOT_FOUND: status.HTTP_404_NOT_FOUND,
    }
)


# Routing raises the first two itself (unknown path, wrong method).
_HTTP_EXCEPTION_CODES: Final[Mapping[int, ApiErrorCode]] = MappingProxyType(
    {
        status.HTTP_404_NOT_FOUND: ApiErrorCode.NOT_FOUND,
        status.HTTP_405_METHOD_NOT_ALLOWED: ApiErrorCode.METHOD_NOT_ALLOWED,
        # Same code as a database outage: either way the client should try again later.
        status.HTTP_503_SERVICE_UNAVAILABLE: ApiErrorCode.SERVICE_UNAVAILABLE,
    }
)


# SQLSTATE classes that mean the database cannot serve the request right now, so a retry may work:
# 08 connection exception, 53 insufficient resources (e.g. too many connections), 57 operator
# intervention (57014 is a statement_timeout, 57P01 a server shutdown).
_UNAVAILABLE_SQLSTATE_CLASSES: Final = frozenset({"08", "53", "57"})


# OpenAPI entries for the errors every database-backed route can return.
DATABASE_ERROR_RESPONSES: Final[dict[int | str, dict[str, object]]] = {
    status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse},
    status.HTTP_503_SERVICE_UNAVAILABLE: {
        "model": ErrorResponse,
        "description": "Database unavailable; try again later",
    },
}


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


async def _handle_request_validation_error(request: Request, exc: Exception) -> JSONResponse:
    # Replaces FastAPI's default 422, which returns each error's input, msg and ctx: all three can
    # quote the client's value. Raised today only for path parameters (POST /jobs reads its body
    # itself), e.g. "path.job_id: uuid_parsing".
    error = cast(RequestValidationError, exc)
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        ApiErrorCode.INVALID_REQUEST,
        "request is not valid",
        safe_error_details(error.errors(), root="request"),
    )


async def _handle_http_exception(request: Request, exc: Exception) -> JSONResponse:
    error = cast(HTTPException, exc)
    fallback = (
        ApiErrorCode.INTERNAL_ERROR if error.status_code >= 500 else ApiErrorCode.INVALID_REQUEST
    )
    code = _HTTP_EXCEPTION_CODES.get(error.status_code, fallback)
    # The standard phrase, not exc.detail: a detail passed by other code could hold input. The
    # headers are kept, e.g. Allow on a 405.
    return error_response(
        error.status_code, code, _status_phrase(error.status_code), headers=error.headers
    )


def _status_phrase(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase.lower()
    except ValueError:  # Not a registered status, e.g. 499.
        return "server error" if status_code >= 500 else "client error"


def _service_unavailable() -> JSONResponse:
    return error_response(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        ApiErrorCode.SERVICE_UNAVAILABLE,
        "service temporarily unavailable, try again later",
    )


def _sqlstate(error: SQLAlchemyError) -> str | None:
    # SQLAlchemy's asyncpg adapter copies the code onto the driver error it wraps (exc.orig).
    sqlstate = getattr(getattr(error, "orig", None), "sqlstate", None)
    return sqlstate if isinstance(sqlstate, str) else None


def _constraint_name(error: SQLAlchemyError) -> str | None:
    # Only asyncpg's own exception has it, which the adapter chains as orig.__cause__. A schema
    # identifier such as ck_jobs_max_attempts_range, never row data.
    driver_error = getattr(getattr(error, "orig", None), "__cause__", None)
    name = getattr(driver_error, "constraint_name", None)
    return name if isinstance(name, str) else None


def _class_name(exc: Exception) -> str:
    # Qualified, because the bare name is ambiguous: sqlalchemy.exc.TimeoutError is a full pool,
    # builtins.TimeoutError a database that stopped answering.
    return f"{type(exc).__module__}.{type(exc).__qualname__}"


def _is_unavailable(error: SQLAlchemyError, sqlstate: str | None) -> bool:
    if isinstance(error, PoolTimeoutError):
        return True
    # A dropped connection, whatever the exception class (asyncpg reports it as InterfaceError).
    if isinstance(error, DBAPIError) and error.connection_invalidated:
        return True
    return sqlstate is not None and sqlstate[:2] in _UNAVAILABLE_SQLSTATE_CLASSES


# The database handlers below log only the class name, SQLSTATE and constraint name, never
# str(exc) or a traceback: PostgreSQL's DETAIL (in the chained asyncpg error) can quote the whole
# row, payload included. Being class-specific, they also stop Starlette from re-raising to uvicorn.


async def _handle_database_error(request: Request, exc: Exception) -> JSONResponse:
    error = cast(SQLAlchemyError, exc)
    sqlstate = _sqlstate(error)
    if _is_unavailable(error, sqlstate):
        logger.warning("database unavailable: %s (sqlstate %s)", _class_name(error), sqlstate)
        return _service_unavailable()
    logger.error(
        "database error: %s (sqlstate %s, constraint %s)",
        _class_name(error),
        sqlstate,
        _constraint_name(error),
    )
    return error_response(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        ApiErrorCode.INTERNAL_ERROR,
        "internal server error",
    )


async def _handle_connection_error(request: Request, exc: Exception) -> JSONResponse:
    # SQLAlchemy does not wrap these: a refused connection (ConnectionRefusedError) and asyncpg's
    # connect and command timeouts (TimeoutError, an OSError subclass). The API does no other I/O,
    # so any OSError is taken to mean the database is out of reach.
    logger.warning("database unavailable: %s", _class_name(exc))
    return _service_unavailable()


async def _handle_client_disconnect(request: Request, exc: Exception) -> JSONResponse:
    # The client hung up while sending the body, so nobody receives this response. Handled only
    # to keep it out of the server-error tracebacks: it is not a bug here (decision Q1).
    logger.info("client disconnected before the request body was read")
    return error_response(
        status.HTTP_400_BAD_REQUEST, ApiErrorCode.INVALID_REQUEST, "request body incomplete"
    )


async def _handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    # Starlette re-raises the exception after this response is sent, and uvicorn logs its
    # traceback (decision D7). Database and connection errors never get here: their traceback
    # would hold PostgreSQL's DETAIL, so they have the handlers above, which do not re-raise.
    return error_response(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        ApiErrorCode.INTERNAL_ERROR,
        "internal server error",
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, _handle_api_error)
    app.add_exception_handler(DomainError, _handle_domain_error)
    app.add_exception_handler(RequestValidationError, _handle_request_validation_error)
    # Replaces FastAPI's default, which answers {"detail": ...}. FastAPI's HTTPException is a
    # subclass, so it is covered too.
    app.add_exception_handler(HTTPException, _handle_http_exception)
    app.add_exception_handler(SQLAlchemyError, _handle_database_error)
    app.add_exception_handler(OSError, _handle_connection_error)
    app.add_exception_handler(ClientDisconnect, _handle_client_disconnect)
    app.add_exception_handler(Exception, _handle_unexpected_error)
