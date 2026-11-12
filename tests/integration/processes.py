"""Running `python -m task_platform.<process>` as a child and stopping it with a signal."""

import asyncio
import os
import signal
import sys
from collections.abc import Awaitable, Callable


async def stop_with_signal(
    module: str,
    env: dict[str, str],
    ready: Callable[[], Awaitable[bool]],
    signum: signal.Signals,
) -> tuple[int, bytes]:
    """Start the module, wait until `ready()` is true, send `signum`; return the exit status and
    stderr. The child is killed if it has not exited within 10 s of the signal."""
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        module,
        env=os.environ | env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(15):
            while not await ready():
                await asyncio.sleep(0.05)
        child.send_signal(signum)
        async with asyncio.timeout(10):
            _, stderr = await child.communicate()
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
    assert child.returncode is not None
    return child.returncode, stderr
