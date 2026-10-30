"""Where integration tests find their RabbitMQ.

There is no name guard like the database one: tests only declare and delete queues of their own,
with unique names (ADR 0004 decision 9), so they never touch what a developer's processes use.
"""

from pydantic import SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class TestBrokerError(Exception):
    """The test broker is not configured. Messages never contain the URL."""

    __test__ = False  # Not a test class, despite the name.


class _TestEnvironment(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    test_broker_url: SecretStr


def load_test_broker_url(env_file: str | None = ".env") -> str:
    """Return TEST_BROKER_URL from the environment or `env_file`.

    Integration tests fail (never skip) when this raises, so a missing broker is noticed.
    """
    try:
        url = _TestEnvironment(_env_file=env_file).test_broker_url.get_secret_value()
    except ValidationError:
        url = None
    if url is None:
        raise TestBrokerError("TEST_BROKER_URL is not set (see .env.example)")
    return url
