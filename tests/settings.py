"""Settings for tests: only the values a test passes, never a developer's .env or shell."""

from urllib.parse import urlsplit, urlunsplit

from task_platform.config import BrokerSettings, Settings

# A closed port: any database access fails at once with ConnectionRefusedError.
UNREACHABLE_DATABASE_URL = "postgresql+asyncpg://task_platform:unused@127.0.0.1:1/unused_test"
# The same for RabbitMQ: the first connection attempt fails at once.
UNREACHABLE_BROKER_URL = "amqp://task_platform:unused@127.0.0.1:1/"


def make_test_settings(url: str, **overrides: object) -> Settings:
    # A developer's .env and exported DB_* variables must not change what the tests measure:
    # _env_file=None skips the file, and explicit arguments take precedence over the environment.
    defaults = {
        name: field.default
        for name, field in Settings.model_fields.items()
        if name != "database_url"
    }
    return Settings(_env_file=None, database_url=url, **(defaults | overrides))  # type: ignore[arg-type]


def make_test_broker_settings(
    database_url: str, broker_url: str, **overrides: object
) -> BrokerSettings:
    # Same rule as above: defaults from the class, never from .env or exported variables.
    defaults = {
        name: field.default
        for name, field in BrokerSettings.model_fields.items()
        if name not in {"database_url", "broker_url"}
    }
    values = defaults | {"database_url": database_url, "broker_url": broker_url} | overrides
    return BrokerSettings(_env_file=None, **values)


def with_url_parts(url: str, *, password: str | None = None, path: str | None = None) -> str:
    """`url` with its password or path (database, vhost) replaced; everything else kept."""
    parts = urlsplit(url)
    netloc = parts.netloc
    if password is not None:
        _, _, host = netloc.rpartition("@")
        netloc = f"{parts.username}:{password}@{host}"
    return urlunsplit(parts._replace(netloc=netloc, path=parts.path if path is None else path))
