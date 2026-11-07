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


def test_dispatch_values_are_read_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DISPATCH_BATCH_SIZE", "7")
    monkeypatch.setenv("PUBLISH_TIMEOUT_S", "0.5")
    result = broker_settings(database_url=URL, broker_url=BROKER_URL)
    assert result.dispatch_batch_size == 7
    assert result.publish_timeout_s == 0.5


@pytest.mark.parametrize(("size", "accepted"), [(0, False), (1, True), (1000, True), (1001, False)])
def test_dispatch_batch_size_bounds(size: int, accepted: bool) -> None:
    values = {"database_url": URL, "broker_url": BROKER_URL, "dispatch_batch_size": size}
    if accepted:
        assert broker_settings(**values).dispatch_batch_size == size
    else:
        with pytest.raises(ValidationError, match="dispatch_batch_size"):
            broker_settings(**values)


@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_publish_timeout_is_rejected(value: float) -> None:
    with pytest.raises(ValidationError, match="publish_timeout_s"):
        broker_settings(database_url=URL, broker_url=BROKER_URL, publish_timeout_s=value)


@pytest.mark.parametrize(
    ("publish_timeout_s", "accepted"), [(4.999, True), (5.0, False), (9.0, False)]
)
def test_publish_timeout_must_be_shorter_than_statement_timeout(
    publish_timeout_s: float, accepted: bool
) -> None:
    # A worker's claim waits on the dispatcher's row locks (ADR 0004 decision 8).
    values = {
        "database_url": URL,
        "broker_url": BROKER_URL,
        "db_statement_timeout_ms": 5_000,
        "publish_timeout_s": publish_timeout_s,
    }
    if accepted:
        assert broker_settings(**values).publish_timeout_s == publish_timeout_s
    else:
        with pytest.raises(ValidationError, match="publish_timeout_s must be shorter"):
            broker_settings(**values)


def test_poll_interval_is_read_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DISPATCH_POLL_INTERVAL_S", "0.25")
    result = broker_settings(database_url=URL, broker_url=BROKER_URL)
    assert result.dispatch_poll_interval_s == 0.25


@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_poll_interval_is_rejected(value: float) -> None:
    # Zero would poll the database in a tight loop.
    with pytest.raises(ValidationError, match="dispatch_poll_interval_s"):
        broker_settings(database_url=URL, broker_url=BROKER_URL, dispatch_poll_interval_s=value)


@pytest.mark.parametrize("field", ["lease_duration_s", "handler_timeout_s"])
@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_worker_timeouts_are_rejected(field: str, value: float) -> None:
    with pytest.raises(ValidationError, match=field):
        broker_settings(database_url=URL, broker_url=BROKER_URL, **{field: value})


@pytest.mark.parametrize(
    ("handler_timeout_s", "accepted"), [(29.999, True), (30.0, False), (45.0, False)]
)
def test_handler_timeout_must_be_shorter_than_the_lease(
    handler_timeout_s: float, accepted: bool
) -> None:
    # A handler must not outlive its lease, after which recovery may give the job to another worker.
    values = {
        "database_url": URL,
        "broker_url": BROKER_URL,
        "lease_duration_s": 30.0,
        "handler_timeout_s": handler_timeout_s,
    }
    if accepted:
        assert broker_settings(**values).handler_timeout_s == handler_timeout_s
    else:
        with pytest.raises(ValidationError, match="handler_timeout_s must be shorter"):
            broker_settings(**values)


@pytest.mark.parametrize(
    ("lease_duration_s", "accepted"),
    [(1799.0, True), (1800.0, False), (1e13, False), (float("inf"), False)],
)
def test_lease_stays_below_the_brokers_consumer_timeout(
    lease_duration_s: float, accepted: bool
) -> None:
    # ADR 0004 decision 8; inf or a huge lease would also fail every claim in the database.
    values = {"database_url": URL, "broker_url": BROKER_URL, "lease_duration_s": lease_duration_s}
    if accepted:
        assert broker_settings(**values).lease_duration_s == lease_duration_s
    else:
        with pytest.raises(ValidationError, match="lease_duration_s"):
            broker_settings(**values)


@pytest.mark.parametrize(
    ("backoff_s", "accepted"), [(0.001, True), (1799.0, True), (0, False), (1800.0, False)]
)
def test_db_error_backoff_bounds(backoff_s: float, accepted: bool) -> None:
    # The worker holds the message during the wait, so it stays below the consumer_timeout.
    values = {"database_url": URL, "broker_url": BROKER_URL, "worker_db_error_backoff_s": backoff_s}
    if accepted:
        assert broker_settings(**values).worker_db_error_backoff_s == backoff_s
    else:
        with pytest.raises(ValidationError, match="worker_db_error_backoff_s"):
            broker_settings(**values)


def test_retry_delays_are_read_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RETRY_BASE_DELAY_S", "0.5")
    monkeypatch.setenv("RETRY_MAX_DELAY_S", "60")
    result = broker_settings(database_url=URL, broker_url=BROKER_URL)
    assert (result.retry_base_delay_s, result.retry_max_delay_s) == (0.5, 60.0)


@pytest.mark.parametrize("field", ["retry_base_delay_s", "retry_max_delay_s"])
@pytest.mark.parametrize("value", [0, -1, float("nan")])
def test_non_positive_or_nan_retry_delays_are_rejected(field: str, value: float) -> None:
    # NaN would pass the base <= cap check (every comparison with NaN is False) and then make every
    # retry raise in the worker; today pydantic's gt/le reject it, and this test keeps it that way.
    with pytest.raises(ValidationError, match=field):
        broker_settings(database_url=URL, broker_url=BROKER_URL, **{field: value})


@pytest.mark.parametrize(
    ("cap_s", "accepted"),
    [(86400.0, True), (86400.001, False), (float("inf"), False), (float("nan"), False)],
)
def test_retry_cap_is_at_most_one_day(cap_s: float, accepted: bool) -> None:
    # ADR 0005 decision 3; also keeps now() + delay inside PostgreSQL's timestamp range.
    values = {"database_url": URL, "broker_url": BROKER_URL, "retry_max_delay_s": cap_s}
    if accepted:
        assert broker_settings(**values).retry_max_delay_s == cap_s
    else:
        with pytest.raises(ValidationError, match="retry_max_delay_s"):
            broker_settings(**values)


@pytest.mark.parametrize(
    ("base_s", "accepted"),
    [(300.0, True), (0.001, True), (300.001, False), (float("inf"), False)],
)
def test_retry_base_must_not_exceed_the_cap(base_s: float, accepted: bool) -> None:
    values = {
        "database_url": URL,
        "broker_url": BROKER_URL,
        "retry_base_delay_s": base_s,
        "retry_max_delay_s": 300.0,
    }
    if accepted:
        assert broker_settings(**values).retry_base_delay_s == base_s
    else:
        with pytest.raises(ValidationError, match="retry_base_delay_s must not exceed"):
            broker_settings(**values)
