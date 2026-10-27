"""Where integration tests find their database, and the guard that keeps them off real data."""

from pydantic import SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class TestDatabaseError(Exception):
    """The test database is missing or unsafe to use. Messages never contain the URL."""

    __test__ = False  # Not a test class, despite the name.


class _TestEnvironment(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    test_database_url: SecretStr


def load_test_database_url(env_file: str | None = ".env") -> str:
    """Return TEST_DATABASE_URL from the environment or `env_file`, if it names a `_test` database.

    Integration tests fail (never skip) when this raises, so a missing database is noticed.
    """
    try:
        url = _TestEnvironment(_env_file=env_file).test_database_url.get_secret_value()
    except ValidationError:
        url = None
    if url is None:
        raise TestDatabaseError("TEST_DATABASE_URL is not set (see .env.example)")

    try:
        parsed = make_url(url)
        # Check the database the driver will really connect to: a `?database=` query parameter
        # overrides the one in the path.
        _, connect_kwargs = parsed.get_dialect()().create_connect_args(parsed)
        database = connect_kwargs.get("database")
    except ArgumentError:
        database = None
    # Tests truncate and migrate this database up and down, so it must be a throwaway one.
    if not isinstance(database, str) or not database.endswith("_test"):
        raise TestDatabaseError("TEST_DATABASE_URL must name a database ending in '_test'")
    return url
