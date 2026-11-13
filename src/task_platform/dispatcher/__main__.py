import asyncio
import sys

from task_platform.config import BrokerSettings
from task_platform.dispatcher.process import run_dispatcher
from task_platform.shutdown import StopSignals
from task_platform.startup import StartupError


async def main() -> int:
    """The exit status: 0 after a stop, 1 on a configuration error at start (ADR 0008)."""
    settings = BrokerSettings()
    signals = StopSignals()
    # Before the first connection: a signal during start-up is then a stop request too.
    with signals.handled():
        try:
            await run_dispatcher(settings, signals)
        except StartupError:
            # Already logged as an ERROR; a traceback would add nothing.
            return 1
    return 0


# No logging configuration yet (phase 8): warnings reach stderr through Python's last-resort
# handler, INFO lines are not shown.
sys.exit(asyncio.run(main()))
