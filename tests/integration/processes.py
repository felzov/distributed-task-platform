"""Running `python -m task_platform.<process>` as a child: stopped with a signal, or to its exit."""

import asyncio
import os
import signal
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager


@asynccontextmanager
async def child_process(
    module: str, env: dict[str, str]
) -> AsyncIterator[tuple[asyncio.subprocess.Process, bytearray]]:
    """The running child and its stderr so far, read as it comes; killed if still running at the
    end."""
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        module,
        env=os.environ | env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    stderr = bytearray()

    async def read_stderr() -> None:
        assert child.stderr is not None
        while chunk := await child.stderr.read(4096):
            stderr.extend(chunk)

    reading = asyncio.create_task(read_stderr())
    try:
        yield child, stderr
        async with asyncio.timeout(10):
            await reading
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
        reading.cancel()


async def stop_with_signal(
    module: str,
    env: dict[str, str],
    ready: Callable[[bytes], Awaitable[bool]],
    signum: signal.Signals,
) -> tuple[int, bytes]:
    """Start the module, wait until `ready(stderr so far)` is true, send `signum`; return the exit
    status and stderr. The child is killed if it has not exited within 10 s of the signal."""
    async with child_process(module, env) as (child, stderr):
        async with asyncio.timeout(15):
            while not await ready(bytes(stderr)):
                await asyncio.sleep(0.05)
        child.send_signal(signum)
        async with asyncio.timeout(10):
            status = await child.wait()
    return status, bytes(stderr)


async def run_to_exit(module: str, env: dict[str, str]) -> tuple[int, bytes]:
    """Start the module and return its exit status and stderr; killed after 15 s."""
    async with child_process(module, env) as (child, stderr):
        async with asyncio.timeout(15):
            status = await child.wait()
    return status, bytes(stderr)
