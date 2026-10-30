from pathlib import Path

import pytest
from pydantic import ValidationError

from task_platform.config import BrokerSettings, Settings

PASSWORD = "s3cr3t-sentinel-7731"
URL = f"postgresql+asyncpg://task_platform:{PASSWORD}@127.0.0.1:5432/task_platform"
BROKER_URL = f"amqp://task_platform:{PASSWORD}@127.0.0.1:5672/"


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
    [
        "db_connect_timeout_s",
        "db_statement_timeout_ms",
        "db_command_timeout_s",
        "db_pool_size",
        "db_pool_timeout_s",
        "ready_check_timeout_s",
        "max_payload_bytes",
        "max_request_body_bytes",
        "default_max_attempts",
    ],
)
@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_limits_are_rejected(field: str, value: int) -> None:
    with pytest.raises(ValidationError, match=field):
        settings(database_url=URL, **{field: value})


@pytest.mark.parametrize(("value", "accepted"), [(1, True), (20, True), (21, False)])
def test_default_max_attempts_stays_within_the_database_check(value: int, accepted: bool) -> None:
    # Same bounds as ck_jobs_max_attempts_range: a config typo must fail at startup, not on
    # every INSERT.
    if accepted:
        assert settings(database_url=URL, default_max_attempts=value).default_max_attempts == value
    else:
        with pytest.raises(ValidationError, match="default_max_attempts"):
            settings(database_url=URL, default_max_attempts=value)


def test_body_limit_below_payload_limit_is_rejected() -> None:
    with pytest.raises(ValidationError, match="max_request_body_bytes must be at least"):
        settings(database_url=URL, max_payload_bytes=2_000, max_request_body_bytes=1_999)


def test_body_limit_equal_to_payload_limit_is_accepted() -> None:
    result = settings(database_url=URL, max_payload_bytes=2_000, max_request_body_bytes=2_000)
    assert result.max_request_body_bytes == result.max_payload_bytes


@pytest.mark.parametrize(
    ("command_timeout_s", "accepted"), [(5.001, True), (5.0, False), (1.0, False)]
)
def test_command_timeout_must_outlast_statement_timeout(
    command_timeout_s: float, accepted: bool
) -> None:
    values = {"db_statement_timeout_ms": 5_000, "db_command_timeout_s": command_timeout_s}
    if accepted:
        assert settings(database_url=URL, **values).db_command_timeout_s == command_timeout_s
    else:
        with pytest.raises(ValidationError, match="db_command_timeout_s must be longer"):
            settings(database_url=URL, **values)


def broker_settings(**values: object) -> BrokerSettings:
    return BrokerSettings(_env_file=None, **values)  # type: ignore[arg-type]


def test_api_settings_need_no_broker_url(monkeypatch: pytest.MonkeyPatch) -> None:
    # The API never talks to RabbitMQ (ADR 0002): a missing BROKER_URL must not stop it.
    monkeypatch.delenv("BROKER_URL", raising=False)
    assert not hasattr(settings(database_url=URL), "broker_url")


def test_broker_url_is_required_for_broker_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BROKER_URL", raising=False)
    with pytest.raises(ValidationError, match="broker_url"):
        broker_settings(database_url=URL)


def test_broker_settings_keep_the_database_settings_and_their_checks() -> None:
    result = broker_settings(database_url=URL, broker_url=BROKER_URL)
    assert result.database_url.get_secret_value() == URL
    with pytest.raises(ValidationError, match="db_command_timeout_s must be longer"):
        broker_settings(database_url=URL, broker_url=BROKER_URL, db_command_timeout_s=1.0)


def test_broker_password_is_hidden_from_repr_and_str() -> None:
    result = broker_settings(database_url=URL, broker_url=BROKER_URL)
    assert PASSWORD not in repr(result)
    assert PASSWORD not in str(result)


def test_broker_values_are_read_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_URL", BROKER_URL)
    monkeypatch.setenv("JOBS_QUEUE_NAME", "jobs.dev")
    result = broker_settings(database_url=URL)
    assert result.broker_url.get_secret_value() == BROKER_URL
    assert result.jobs_queue_name == "jobs.dev"


@pytest.mark.parametrize("field", ["broker_connect_timeout_s", "broker_heartbeat_s"])
@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_broker_limits_are_rejected(field: str, value: int) -> None:
    with pytest.raises(ValidationError, match=field):
        broker_settings(database_url=URL, broker_url=BROKER_URL, **{field: value})


def test_heartbeat_above_the_amqp_range_is_rejected() -> None:
    # aiormq sends 0 (no heartbeats at all) for any value of 65535 or more.
    assert broker_settings(database_url=URL, broker_url=BROKER_URL, broker_heartbeat_s=65534)
    with pytest.raises(ValidationError, match="broker_heartbeat_s"):
        broker_settings(database_url=URL, broker_url=BROKER_URL, broker_heartbeat_s=65535)


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("", id="empty"),
        pytest.param("amq.jobs", id="reserved prefix"),
        pytest.param("jobs queue", id="space"),
        pytest.param("jöbs", id="non-ascii"),
        pytest.param("j" * 256, id="256 characters"),
    ],
)
def test_invalid_queue_names_are_rejected(name: str) -> None:
    with pytest.raises(ValidationError, match="jobs_queue_name") as exc_info:
        broker_settings(database_url=URL, broker_url=BROKER_URL, jobs_queue_name=name)
    assert PASSWORD not in str(exc_info.value)


@pytest.mark.parametrize("name", ["jobs", "test-jobs-0f3a", "jobs.v1_x", "j" * 255, "amqp-jobs"])
def test_valid_queue_names_are_accepted(name: str) -> None:
    result = broker_settings(database_url=URL, broker_url=BROKER_URL, jobs_queue_name=name)
    assert result.jobs_queue_name == name
