"""migrations/env.py chooses its database explicitly and never falls back to it by accident."""

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from pydantic import ValidationError

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"


@pytest.fixture
def no_database_configured(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # No DATABASE_URL and no .env: a fallback to Settings() fails before connecting anywhere.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.chdir(tmp_path)


def config_with(**attributes: object) -> Config:
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_DIR))
    config.attributes.update(attributes)
    return config


@pytest.mark.usefixtures("no_database_configured")
def test_settings_of_the_wrong_type_are_rejected() -> None:
    # Falling back to DATABASE_URL here would point a test's `downgrade base` at the dev database.
    with pytest.raises(TypeError, match="Settings"):
        command.current(config_with(settings="postgresql+asyncpg://u@h/task_platform_test"))


@pytest.mark.usefixtures("no_database_configured")
def test_without_settings_the_cli_reads_database_url() -> None:
    with pytest.raises(ValidationError, match="database_url"):
        command.current(config_with())
