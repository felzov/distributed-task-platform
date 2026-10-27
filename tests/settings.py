"""Settings for tests: only the values a test passes, never a developer's .env or shell."""

from task_platform.config import Settings


def make_test_settings(url: str, **overrides: object) -> Settings:
    # A developer's .env and exported DB_* variables must not change what the tests measure:
    # _env_file=None skips the file, and explicit arguments take precedence over the environment.
    defaults = {
        name: field.default
        for name, field in Settings.model_fields.items()
        if name != "database_url"
    }
    return Settings(_env_file=None, database_url=url, **(defaults | overrides))  # type: ignore[arg-type]
