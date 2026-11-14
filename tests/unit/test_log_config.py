"""The dispatcher's and the worker's logging (ADR 0008): time, level and logger on each line, and
SQLAlchemy's engine never below WARNING, where its records can hold payloads."""

import io
import logging
import re
from collections.abc import Iterator

import pytest

from task_platform.log_config import HELD_AT_WARNING, configure_logging

LINE = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3} (\w+) ([\w.]+): (.*)$")


@pytest.fixture
def stream() -> Iterator[io.StringIO]:
    """configure_logging's output, with the root logger, `sqlalchemy` and the held loggers
    restored after."""
    root = logging.getLogger()
    saved = (list(root.handlers), root.level)
    held = {name: logging.getLogger(name).level for name in ("sqlalchemy", *HELD_AT_WARNING)}
    output = io.StringIO()
    try:
        yield output
    finally:
        root.handlers[:] = saved[0]
        root.setLevel(saved[1])
        for name, level in held.items():
            logging.getLogger(name).setLevel(level)


def lines(stream: io.StringIO) -> list[tuple[str, str, str]]:
    found = []
    for line in stream.getvalue().splitlines():
        match = LINE.match(line)
        assert match, line
        found.append((match[1], match[2], match[3]))
    return found


def test_a_line_names_its_time_level_and_logger(stream: io.StringIO) -> None:
    configure_logging("INFO", stream)
    logging.getLogger("task_platform.worker.process").info("worker started: queue %s", "jobs")
    assert lines(stream) == [("INFO", "task_platform.worker.process", "worker started: queue jobs")]


def test_records_below_the_level_are_dropped(stream: io.StringIO) -> None:
    configure_logging("WARNING", stream)
    logger = logging.getLogger("task_platform.dispatcher.process")
    logger.info("dropped")
    logger.warning("kept")
    assert lines(stream) == [("WARNING", "task_platform.dispatcher.process", "kept")]


@pytest.mark.parametrize("name", ["sqlalchemy.engine", "sqlalchemy.engine.Engine"])
def test_sqlalchemy_engine_stays_at_warning_even_at_debug(stream: io.StringIO, name: str) -> None:
    # SQLAlchemy sets its `sqlalchemy` logger to WARNING when imported, which alone would pass
    # this test. Lowered, as a developer might, only the hold keeps the engine's rows out.
    logging.getLogger("sqlalchemy").setLevel(logging.DEBUG)
    configure_logging("DEBUG", stream)
    logger = logging.getLogger(name)
    logger.debug("Row (%r,)", "payload")
    logger.info("SELECT ...")
    logger.warning("kept")
    logging.getLogger("task_platform").debug("own debug")
    assert lines(stream) == [("WARNING", name, "kept"), ("DEBUG", "task_platform", "own debug")]
