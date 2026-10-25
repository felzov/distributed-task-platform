"""The guard that stops integration tests from touching a non-test database, and test settings."""

import pytest

from task_platform.config import Settings
from tests.integration.database import (
    TestDatabaseError,
    load_test_database_url,
    make_test_settings,
)

PASSWORD = "s3cr3t-sentinel-7731"


def url_for(database: str) -> str:
    return f"postgresql+asyncpg://task_platform:{PASSWORD}@127.0.0.1:5432/{database}"


def test_returns_a_url_that_names_a_test_database(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_DATABASE_URL", url_for("task_platform_test"))
    assert load_test_database_url(env_file=None) == url_for("task_platform_test")


def test_fails_when_the_url_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    with pytest.raises(TestDatabaseError, match="not set"):
        load_test_database_url(env_file=None)


@pytest.mark.parametrize(
    "url",
    [
        url_for("task_platform"),
        url_for("task_platform_test_copy"),
        url_for(""),
        "not a url",
        # The path names a test database, but the driver would connect to the query's database.
        url_for("task_platform_test") + "?database=task_platform",
    ],
)
def test_refuses_anything_but_a_test_database(monkeypatch: pytest.MonkeyPatch, url: str) -> None:
    monkeypatch.setenv("TEST_DATABASE_URL", url)
    with pytest.raises(TestDatabaseError, match="_test") as exc_info:
        load_test_database_url(env_file=None)
    assert PASSWORD not in str(exc_info.value)


def test_test_settings_ignore_exported_db_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    # A value exported in the developer's shell must not change what the tests measure.
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", "1")
    monkeypatch.setenv("DB_POOL_SIZE", "99")
    result = make_test_settings(url_for("task_platform_test"))
    fields = Settings.model_fields
    assert result.db_statement_timeout_ms == fields["db_statement_timeout_ms"].default
    assert result.db_pool_size == fields["db_pool_size"].default


def test_test_settings_accept_explicit_overrides() -> None:
    result = make_test_settings(url_for("task_platform_test"), db_statement_timeout_ms=100)
    assert result.db_statement_timeout_ms == 100
