"""The revision `/ready` expects is the newest migration, so a new migration cannot forget it."""

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

from task_platform.data.schema import SCHEMA_REVISION

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"


def test_schema_revision_is_the_alembic_head() -> None:
    # Reads the revision files only; env.py (and with it a database) is not involved.
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_DIR))
    assert ScriptDirectory.from_config(config).get_heads() == [SCHEMA_REVISION]
