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
    end. This helper and the two below are hidden from tracebacks: pytest would show their
    arguments, and `env` holds the test URLs with their passwords."""
    __tracebackhide__ = True
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
    __tracebackhide__ = True
    async with child_process(module, env) as (child, stderr):
        async with asyncio.timeout(15):
            while not await ready(bytes(stderr)):
                await asyncio.sleep(0.05)
        child.send_signal(signum)
        async with asyncio.timeout(10):
            status = await child.wait()
    return status, bytes(stderr)


def assert_logged(stderr: bytes, text: bytes) -> None:
    """Fails naming `text`, never showing `stderr`: pytest prints both sides of a failing `in`,
    and a child's output could quote its settings. Hidden from the traceback, which would show
    its arguments."""
    __tracebackhide__ = True
    found = text in stderr
    assert found, f"{text!r} is not in the child's stderr"


def assert_not_logged(stderr: bytes, text: bytes) -> None:
    """As `assert_logged`, for text that must not appear. Fails without showing either side."""
    __tracebackhide__ = True
    found = text in stderr
    assert not found, "the child's stderr holds text it must not hold"


async def run_to_exit(module: str, env: dict[str, str]) -> tuple[int, bytes]:
    """Start the module and return its exit status and stderr; killed after 15 s."""
    __tracebackhide__ = True
    async with child_process(module, env) as (child, stderr):
        async with asyncio.timeout(15):
            status = await child.wait()
    return status, bytes(stderr)
