"""Request and response bodies of the HTTP API."""

import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict

from task_platform.domain.job_status import JobStatus


class SubmitJobRequest(BaseModel):
    """Envelope of `POST /jobs`. The payload inside is validated by the domain, per job type."""

    # strict: the version must be a JSON integer, not true, 1.0 or "1".
    model_config = ConfigDict(extra="forbid", strict=True)

    type: str
    payload_version: int
    # Every payload schema is a JSON object; its contents are checked by the domain.
    payload: dict[str, Any]


class SubmitJobResponse(BaseModel):
    id: uuid.UUID
    status: JobStatus


class ErrorBody(BaseModel):
    code: str
    # Safe to show: never contains input values.
    message: str
    details: list[str]


class ErrorResponse(BaseModel):
    error: ErrorBody
