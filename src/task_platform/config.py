"""Settings from environment variables, or from a local `.env` file (see `.env.example`).

All limits and timeouts are explicit here. The defaults are placeholders, not measured values.
"""

from typing import Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # `.env` also holds values for docker compose (POSTGRES_PASSWORD), so unknown keys are ignored.
    # Errors would otherwise print every value read, secrets included. exc.errors() still holds
    # them, so never log that.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    # SecretStr keeps the password out of repr(), logs and tracebacks.
    database_url: SecretStr
    db_connect_timeout_s: float = Field(default=5.0, gt=0)
    # Server-side limit per statement, set on every connection.
    db_statement_timeout_ms: int = Field(default=5_000, gt=0)
    # Client-side limit per command (asyncpg command_timeout), COMMIT included: a backstop for a
    # server or network that stops answering, so it must be longer than the statement timeout.
    db_command_timeout_s: float = Field(default=10.0, gt=0)
    db_pool_size: int = Field(default=5, ge=1)
    # How long a request waits for a free pooled connection.
    db_pool_timeout_s: float = Field(default=5.0, gt=0)
    # Upper bound for the whole /ready check, waiting for a pooled connection included.
    ready_check_timeout_s: float = Field(default=2.0, gt=0)

    # Size of the job payload as compact UTF-8 JSON (domain `payload_size_bytes`).
    max_payload_bytes: int = Field(default=64 * 1024, gt=0)
    # Raw request body, read before any parsing. About twice the payload limit, because the
    # payload is measured compact while clients may send pretty-printed JSON.
    max_request_body_bytes: int = Field(default=128 * 1024, gt=0)
    # Attempt budget of every new job. The bounds match ck_jobs_max_attempts_range.
    default_max_attempts: int = Field(default=5, ge=1, le=20)

    @model_validator(mode="after")
    def _body_limit_fits_payload_limit(self) -> Self:
        # Otherwise a payload under its own limit could never reach the payload check.
        if self.max_request_body_bytes < self.max_payload_bytes:
            raise ValueError("max_request_body_bytes must be at least max_payload_bytes")
        return self

    @model_validator(mode="after")
    def _command_timeout_outlasts_statement_timeout(self) -> Self:
        # Otherwise a slow statement is cut off by the client, which tells less than the server's
        # cancellation (SQLSTATE 57014) and leaves the server to notice on its own.
        if self.db_command_timeout_s * 1000 <= self.db_statement_timeout_ms:
            raise ValueError("db_command_timeout_s must be longer than db_statement_timeout_ms")
        return self


class BrokerSettings(Settings):
    """Settings of the processes that talk to RabbitMQ: the dispatcher and the worker.

    The API uses `Settings`: it never talks to the broker (ADR 0002), so broker configuration can
    neither be required by it nor break it.
    """

    broker_url: SecretStr
    # Bounds the first connection attempt, which is not retried (the process stops instead).
    broker_connect_timeout_s: float = Field(default=5.0, gt=0)
    # Sent in Connection.Tune-Ok, replacing the broker's proposal; missed heartbeats close it.
    # The AMQP field is 16 bits, and aiormq sends 0 (heartbeats off) for 65535 or more.
    broker_heartbeat_s: int = Field(default=60, ge=1, le=65534)
    # RabbitMQ caps names at 255 bytes; ASCII only here, so that is 255 characters.
    jobs_queue_name: str = Field(default="jobs", max_length=255, pattern=r"^[A-Za-z0-9._-]+$")

    @field_validator("jobs_queue_name")
    @classmethod
    def _not_reserved(cls, name: str) -> str:
        # RabbitMQ refuses to declare a queue whose name starts with "amq.".
        if name.startswith("amq."):
            raise ValueError("jobs_queue_name must not start with 'amq.'")
        return name
