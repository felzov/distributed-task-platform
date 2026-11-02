"""The RabbitMQ connection (aio-pika, ADR 0003), with its timeouts taken from settings."""

from aio_pika import connect_robust
from aio_pika.abc import AbstractRobustConnection

from task_platform.config import BrokerSettings


async def connect_broker(settings: BrokerSettings) -> AbstractRobustConnection:
    """Open a connection that reconnects by itself once it has been established.

    The first attempt is not retried: it raises, so a process with a wrong URL, wrong credentials
    or no broker stops at start instead of waiting silently. aio-pika masks the password in the
    connection's str() and repr(), which is what its reconnect warnings log.

    `broker_connect_timeout_s` is the only bound on that first attempt. Cancelling this coroutine
    from outside (asyncio.timeout, shutdown) does not stop it: aio-pika 10.2.1 logs "Connection
    attempt ... cancelled. Reconnecting" and keeps retrying (observed 2026-10-29).
    """
    return await connect_robust(
        settings.broker_url.get_secret_value(),
        timeout=settings.broker_connect_timeout_s,
        # Passed on as the `heartbeat` URL query parameter.
        heartbeat=settings.broker_heartbeat_s,
    )
