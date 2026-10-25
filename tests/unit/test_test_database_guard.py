"""The guard that stops integration tests from touching a non-test database."""

import pytest

from tests.integration.database import TestDatabaseError, load_test_database_url

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
    ],
)
def test_refuses_anything_but_a_test_database(monkeypatch: pytest.MonkeyPatch, url: str) -> None:
    monkeypatch.setenv("TEST_DATABASE_URL", url)
    with pytest.raises(TestDatabaseError, match="_test") as exc_info:
        load_test_database_url(env_file=None)
    assert PASSWORD not in str(exc_info.value)
