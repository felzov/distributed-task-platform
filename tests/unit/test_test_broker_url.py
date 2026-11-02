"""Integration tests fail, not skip, when TEST_BROKER_URL is missing."""

import pytest

from tests.integration.broker import TestBrokerError, load_test_broker_url

URL = "amqp://task_platform:s3cr3t-sentinel-7731@127.0.0.1:5672/"


def test_returns_the_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_BROKER_URL", URL)
    assert load_test_broker_url(env_file=None) == URL


def test_fails_when_the_url_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_BROKER_URL", raising=False)
    with pytest.raises(TestBrokerError, match="not set"):
        load_test_broker_url(env_file=None)
