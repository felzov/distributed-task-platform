import asyncio
import sys

from task_platform.config import BrokerSettings
from task_platform.log_config import configure_logging
from task_platform.shutdown import StopSignals
from task_platform.startup import StartupError
from task_platform.worker.process import run_worker


async def main() -> int:
    """The exit status: 0 after a stop, 1 on a configuration error at start (ADR 0008)."""
    settings = BrokerSettings()
    configure_logging(settings.log_level)
    signals = StopSignals()
    # Before the first connection: a signal during start-up is then a stop request too.
    with signals.handled():
        try:
            await run_worker(settings, signals)
        except StartupError:
            # Already logged as an ERROR; a traceback would add nothing.
            return 1
    return 0


sys.exit(asyncio.run(main()))
