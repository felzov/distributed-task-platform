"""The integration fixtures return hidden URLs (see `tests/unit/test_hidden_url.py`)."""

import pytest

from tests.settings import HiddenUrl

pytestmark = pytest.mark.integration


def test_the_test_urls_are_hidden(test_database_url: str, test_broker_url: str) -> None:
    assert isinstance(test_database_url, HiddenUrl)
    assert isinstance(test_broker_url, HiddenUrl)
