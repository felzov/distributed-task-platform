import asyncio

from task_platform.config import BrokerSettings
from task_platform.dispatcher.process import run_dispatcher
from task_platform.shutdown import StopSignals


async def main() -> None:
    settings = BrokerSettings()
    signals = StopSignals()
    # Before the first connection: a signal during start-up is then a stop request too.
    with signals.handled():
        await run_dispatcher(settings, signals)


# No logging configuration yet (phase 8): warnings reach stderr through Python's last-resort
# handler, INFO lines are not shown.
asyncio.run(main())
