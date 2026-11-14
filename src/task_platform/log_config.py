"""Logging for the dispatcher and the worker (ADR 0008): one line per record on stderr, with the
time, the level and the logger's name.

The API keeps uvicorn's logging configuration.
"""

import logging
from typing import Final, TextIO

FORMAT: Final = "%(asctime)s %(levelname)s %(name)s: %(message)s"

# Held at WARNING whatever the configured level: at INFO SQLAlchemy's engine logs every statement,
# at DEBUG also every result row, and the worker's claim returns the job's payload
# (`hide_parameters` hides only the bound parameters). SQLAlchemy sets its own `sqlalchemy` logger
# to WARNING on import (2.1.6), so a DEBUG root level does not reach it either way; the hold keeps
# the engine there even if that default changes or someone lowers `sqlalchemy`.
HELD_AT_WARNING: Final = ("sqlalchemy.engine",)


def configure_logging(level: str, stream: TextIO | None = None) -> None:
    """Send every record at `level` or above to `stream` (stderr by default).

    Called once, at the start of a process: a second call would add a second handler.
    """
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(FORMAT))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(level)
    for name in HELD_AT_WARNING:
        logging.getLogger(name).setLevel(logging.WARNING)
