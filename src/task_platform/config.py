"""Settings from environment variables, or from a local `.env` file (see `.env.example`).

All limits and timeouts are explicit here. The defaults are placeholders, not measured values.
"""

from pydantic import Field, SecretStr
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
    db_pool_size: int = Field(default=5, ge=1)
    # How long a request waits for a free pooled connection.
    db_pool_timeout_s: float = Field(default=5.0, gt=0)
