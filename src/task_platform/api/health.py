"""Probes: `/health` (the process answers) and `/ready` (it can serve requests)."""

from typing import Literal

from fastapi import APIRouter, status
from pydantic import BaseModel

from task_platform.api.dependencies import AppEngine, AppSettings
from task_platform.api.errors import ApiError, ApiErrorCode
from task_platform.api.schemas import ErrorResponse
from task_platform.services.health import check_readiness

router = APIRouter()


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"


class ReadyResponse(BaseModel):
    status: Literal["ready"] = "ready"


@router.get("/health")
async def health() -> HealthResponse:
    """Liveness. Touches no dependency, so a database outage never gets the process restarted."""
    return HealthResponse()


@router.get("/ready", responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse}})
async def ready(settings: AppSettings, engine: AppEngine) -> ReadyResponse:
    """Readiness: the database answers and is at this code's migration revision."""
    failed = await check_readiness(engine, timeout_s=settings.ready_check_timeout_s)
    if failed is not None:
        raise ApiError(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ApiErrorCode.SERVICE_UNAVAILABLE,
            "service not ready",
            (failed,),
        )
    return ReadyResponse()
