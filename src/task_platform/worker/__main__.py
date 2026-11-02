import asyncio

from task_platform.config import BrokerSettings
from task_platform.worker.process import run_worker

# No logging configuration yet (phase 8): warnings reach stderr through Python's last-resort
# handler, INFO lines are not shown.
asyncio.run(run_worker(BrokerSettings()))
