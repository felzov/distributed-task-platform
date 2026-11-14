"""Start-up of the dispatcher and the worker (ADR 0008 decision 7).

Both wait for their dependencies, schema check first, then the broker, but stop on configuration
errors. A retry logs a WARNING and waits `startup_retry_interval_s`; a stop signal ends that wait.
A signal never cancels a check or a connection attempt in progress: each runs to its own timeout,
so no reconnect task of aio-pika's is left behind (observed on aio-pika 10.2.1).
"""

import asyncio
import logging
from typing import Final, NoReturn

from aio_pika.abc import AbstractRobustConnection
from aiormq.exceptions import AuthenticationError, ProbableAuthenticationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform import safe_logging
from task_platform.broker.connection import connect_broker
from task_platform.config import BrokerSettings
from task_platform.data.schema import SCHEMA_REVISION, current_revisions
from task_platform.shutdown import sleep_unless_stopped

logger = logging.getLogger(__name__)


class StartupError(Exception):
    """A configuration error at start, already logged as an ERROR: the process exits with 1."""


# SQLSTATE classes of a server that does not take connections for now: 08 connection exception,
# 53 insufficient resources (too many connections, 53300), 57 operator intervention (starting up,
# 57P03; shutting down, 57P01).
_RETRIED_SQLSTATE_CLASSES: Final = frozenset({"08", "53", "57"})


def _worth_retrying(sqlstate: str | None) -> bool:
    """No SQLSTATE: the server was not reached or did not answer in time. Any other class, such
    as a wrong password (28P01), an unknown database (3D000) or a missing privilege (42501), is a
    configuration error no retry fixes."""
    return sqlstate is None or sqlstate[:2] in _RETRIED_SQLSTATE_CLASSES


async def wait_for_schema(
    engine: AsyncEngine,
    settings: BrokerSettings,
    stop: asyncio.Event,
    expected_revision: str = SCHEMA_REVISION,
) -> bool:
    """True once the database is reachable and at `expected_revision`; False if `stop` was set
    first. Raises StartupError if it is at another revision, or on an error no retry fixes."""
    while not stop.is_set():
        try:
            # The same check, bound and strictness as /ready.
            async with asyncio.timeout(settings.ready_check_timeout_s), engine.connect() as conn:
                revisions = await current_revisions(conn)
        # TimeoutError (the bound above, asyncpg's connect timeout) is an OSError subclass.
        except (SQLAlchemyError, OSError) as exc:
            code = safe_logging.sqlstate(exc)
            if not _worth_retrying(code):
                logger.error(
                    "the database cannot be used at start: %s (sqlstate %s)",
                    safe_logging.class_name(exc),
                    code,
                )
                raise StartupError from None
            logger.warning(
                "database not available at start: %s (sqlstate %s); retrying in %g s",
                safe_logging.class_name(exc),
                code,
                settings.startup_retry_interval_s,
            )
            await sleep_unless_stopped(settings.startup_retry_interval_s, stop)
            continue
        if revisions != (expected_revision,):
            # Revision ids are not secrets; migrations run before the processes start.
            found = ", ".join(revisions) or "none (never migrated)"
            logger.error(
                "the database schema is at revision %s, this code expects %s: run the migrations"
                " first",
                found,
                expected_revision,
            )
            raise StartupError
        return True
    return False


async def wait_for_broker(
    settings: BrokerSettings, stop: asyncio.Event
) -> AbstractRobustConnection | None:
    """The broker connection once it is established; None if `stop` was set first. Raises
    StartupError on an error no retry fixes: wrong credentials, an unknown vhost, a malformed
    URL."""
    while not stop.is_set():
        try:
            return await connect_broker(settings)
        # Subclasses of OSError, but no retry fixes them: wrong credentials (observed), also
        # raised for a URL scheme other than amqp (observed); an authentication mechanism the
        # broker does not offer.
        except (ProbableAuthenticationError, AuthenticationError) as exc:
            _broker_unusable(exc)
        # An unreachable or silent broker: AMQPConnectionError, TimeoutError (observed).
        except OSError as exc:
            logger.warning(
                "broker not available at start: %s; retrying in %g s",
                safe_logging.class_name(exc),
                settings.startup_retry_interval_s,
            )
            await sleep_unless_stopped(settings.startup_retry_interval_s, stop)
        # An unknown vhost (InvalidFrameError, observed), a malformed URL (ValueError, observed).
        except Exception as exc:
            _broker_unusable(exc)
    return None


def _broker_unusable(exc: Exception) -> NoReturn:
    # The class only: the text of a broker error can hold the connection's URL.
    logger.error("could not connect to the broker at start: %s", safe_logging.class_name(exc))
    raise StartupError from None


async def wait_for_dependencies(
    engine: AsyncEngine, settings: BrokerSettings, stop: asyncio.Event
) -> AbstractRobustConnection | None:
    """The schema check, then the broker connection; None if `stop` was set first."""
    if not await wait_for_schema(engine, settings, stop):
        return None
    return await wait_for_broker(settings, stop)
