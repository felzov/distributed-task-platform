import asyncio

from task_platform.config import BrokerSettings
from task_platform.dispatcher.process import run_dispatcher

# No logging configuration yet (phase 8): warnings reach stderr through Python's last-resort
# handler, INFO lines are not shown.
asyncio.run(run_dispatcher(BrokerSettings()))
