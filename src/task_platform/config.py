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

    # Jobs one dispatcher poll locks and publishes at once. Capped well below asyncpg's limit of
    # 32767 bind parameters per statement: marking sends one parameter per id.
    dispatch_batch_size: int = Field(default=100, ge=1, le=1000)
    # Bounds all publishes and confirms of one batch together, while the batch's rows stay locked.
    publish_timeout_s: float = Field(default=2.0, gt=0)
    # Pause after every poll, so at most dispatch_batch_size jobs go out per interval.
    dispatch_poll_interval_s: float = Field(default=1.0, gt=0)

    # How long a claimed job belongs to its worker; phase 8 recovers jobs whose lease expired.
    # Below RabbitMQ's default consumer_timeout of 30 minutes (ADR 0004 decision 8); the bound
    # also rejects inf and values past PostgreSQL's timestamp range, which would fail every claim.
    lease_duration_s: float = Field(default=120.0, gt=0, lt=1800)
    # A handler still running after this is cancelled and the job fails as handler_timeout.
    handler_timeout_s: float = Field(default=60.0, gt=0)
    # Wait after a database error at the claim, before the message goes back with nack (ADR 0004
    # decision 6). The worker holds the message unacknowledged meanwhile, for the claim's own
    # timeouts plus this wait, so it is capped below RabbitMQ's default consumer_timeout (30 min).
    worker_db_error_backoff_s: float = Field(default=5.0, gt=0, lt=1800)
    # Backoff before a retryable failure is tried again (ADR 0005 decision 3): after attempt n,
    # a random share of min(retry_max_delay_s, retry_base_delay_s * 2^(n-1)). Placeholders, not
    # measured. ADR 0005 decision 4 acks the message before the wait, so consumer_timeout does not
    # bound it; the one-day cap keeps available_at far inside PostgreSQL's timestamp range.
    retry_base_delay_s: float = Field(default=2.0, gt=0)
    retry_max_delay_s: float = Field(default=300.0, gt=0, le=86400)

    @model_validator(mode="after")
    def _publish_timeout_within_statement_timeout(self) -> Self:
        # A worker's claim waits on the dispatcher's row locks; if the publishes could take longer
        # than the statement timeout, that claim would time out (ADR 0004 decision 8). The claim
        # runs under the worker's statement timeout, so this check holds only if the dispatcher
        # and the worker use the same DB_STATEMENT_TIMEOUT_MS (one .env for both).
        if self.publish_timeout_s * 1000 >= self.db_statement_timeout_ms:
            raise ValueError("publish_timeout_s must be shorter than db_statement_timeout_ms")
        return self

    @model_validator(mode="after")
    def _handler_timeout_within_lease(self) -> Self:
        # Otherwise a handler could still run after its lease expired, when recovery may already
        # have given the job to another worker (ADR 0004 decision 8).
        if self.handler_timeout_s >= self.lease_duration_s:
            raise ValueError("handler_timeout_s must be shorter than lease_duration_s")
        return self

    @model_validator(mode="after")
    def _retry_base_within_cap(self) -> Self:
        # Otherwise the cap would shorten even the first wait. Also rejects an infinite base.
        if self.retry_base_delay_s > self.retry_max_delay_s:
            raise ValueError("retry_base_delay_s must not exceed retry_max_delay_s")
        return self

    @field_validator("jobs_queue_name")
    @classmethod
    def _not_reserved(cls, name: str) -> str:
        # RabbitMQ refuses to declare a queue whose name starts with "amq.".
        if name.startswith("amq."):
            raise ValueError("jobs_queue_name must not start with 'amq.'")
        return name
