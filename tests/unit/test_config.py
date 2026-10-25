from pathlib import Path

import pytest
from pydantic import ValidationError

from task_platform.config import Settings

PASSWORD = "s3cr3t-sentinel-7731"
URL = f"postgresql+asyncpg://task_platform:{PASSWORD}@127.0.0.1:5432/task_platform"


def settings(**values: object) -> Settings:
    # _env_file=None: the developer's .env must not leak into unit tests.
    return Settings(_env_file=None, **values)  # type: ignore[arg-type]


def test_defaults_are_explicit_and_positive() -> None:
    result = settings(database_url=URL)
    assert result.db_connect_timeout_s > 0
    assert result.db_statement_timeout_ms > 0
    assert result.db_pool_size >= 1
    assert result.db_pool_timeout_s > 0


def test_values_are_read_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", URL)
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", "250")
    result = settings()
    assert result.database_url.get_secret_value() == URL
    assert result.db_statement_timeout_ms == 250


def test_database_password_is_hidden_from_repr_and_str() -> None:
    result = settings(database_url=URL)
    assert PASSWORD not in repr(result)
    assert PASSWORD not in str(result)


def test_database_url_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValidationError, match="database_url"):
        settings()


def test_validation_errors_do_not_echo_env_file_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # .env also holds POSTGRES_PASSWORD; pydantic would print every value it read in `input_value`.
    env_file = tmp_path / ".env"
    env_file.write_text(f"DB_POOL_SIZE=7\nPOSTGRES_PASSWORD={PASSWORD}\n")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=env_file)
    assert "database_url" in str(exc_info.value)
    assert PASSWORD not in str(exc_info.value)


@pytest.mark.parametrize(
    "field",
    ["db_connect_timeout_s", "db_statement_timeout_ms", "db_pool_size", "db_pool_timeout_s"],
)
@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_limits_are_rejected(field: str, value: int) -> None:
    with pytest.raises(ValidationError, match=field):
        settings(database_url=URL, **{field: value})
