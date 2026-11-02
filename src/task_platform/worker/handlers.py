"""The handler for each job type this worker runs."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from task_platform.domain.payloads import NOTIFICATION_SEND
from task_platform.services.execution import Handler
from task_platform.services.notifications import send_notification

# Every type in domain PAYLOAD_MODELS needs one: a missing handler fails its jobs for good, as
# unknown_job_type (a unit test checks the two match).
HANDLERS: Final[Mapping[str, Handler]] = MappingProxyType({NOTIFICATION_SEND: send_notification})
