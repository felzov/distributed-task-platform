"""Test URLs hide their passwords in pytest's report of a failing test."""

import pytest

from tests.proxy import running_proxy
from tests.settings import HiddenUrl, with_url_parts

# pytester is part of pytest: it runs an inner pytest session and keeps its output.
pytest_plugins = ["pytester"]

PASSWORD = "s3cr3t-sentinel-5521"  # gitleaks:allow (a test sentinel, not a secret)
URL = f"postgresql+asyncpg://task_platform:{PASSWORD}@127.0.0.1:5432/jobs_test"

# A failing test with the URL as a fixture argument (shown by default) and as a local (shown with
# `--showlocals`). It fails on something else: a failing `==` on the URL itself shows both
# strings' values in pytest's diff, which no `repr` can hide.
FAILING_TEST = f"""
import pytest

from tests.settings import HiddenUrl


@pytest.fixture
def test_database_url():
    return {{wrap}}({URL!r})


def test_fails(test_database_url):
    url = test_database_url
    assert url.startswith("amqp://")
"""


def run_failing_test(pytester: pytest.Pytester, wrap: str, *args: str) -> str:
    """The inner session's output. Without an ini file pytest-asyncio warns about its loop scope,
    and the inner test is synchronous: the plugin stays off."""
    pytester.makepyfile(FAILING_TEST.replace("{wrap}", wrap))
    result = pytester.runpytest_inprocess("-p", "no:asyncio", *args)
    result.assert_outcomes(failed=1)
    return "\n".join(result.outlines + result.errlines)


def test_the_value_is_the_url_and_only_repr_hides_it() -> None:
    url = HiddenUrl(URL)
    assert url == URL
    assert str(url) == URL
    assert f"{url}" == URL
    assert PASSWORD not in repr(url)
    assert PASSWORD not in repr({"DATABASE_URL": url})


def test_derived_urls_are_hidden_too() -> None:
    # A changed path keeps the real password.
    assert PASSWORD not in repr(with_url_parts(URL, path="/other_test"))


async def test_the_proxy_url_is_hidden() -> None:
    async with running_proxy("tcp://127.0.0.1:1") as proxy:
        assert PASSWORD not in repr(proxy.url(URL))


@pytest.mark.parametrize("showlocals", [False, True], ids=["default", "showlocals"])
def test_a_failing_test_prints_no_password(pytester: pytest.Pytester, showlocals: bool) -> None:
    output = run_failing_test(pytester, "HiddenUrl", *(["--showlocals"] if showlocals else []))
    assert "<test URL, hidden>" in output
    # Not `PASSWORD not in output`: a failing `in` would print the output it is checking.
    leaked = PASSWORD in output
    assert not leaked


def test_a_plain_str_url_would_print_its_password(pytester: pytest.Pytester) -> None:
    # The control: without HiddenUrl the same failing test shows the password, so the test above
    # would notice the wrapper being lost.
    leaked = PASSWORD in run_failing_test(pytester, "str")
    assert leaked
