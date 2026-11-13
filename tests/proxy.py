"""An in-process TCP proxy that a test can cut, freeze and restore (ADR 0008 decision 10).

Outage tests connect through it instead of stopping the shared PostgreSQL or RabbitMQ containers.
It simulates two kinds of outage, not a server that is slow or answers wrongly:
- `cut()`: every open connection is reset, and new ones are closed as soon as they are accepted.
- `freeze()`: connections stay open but no byte passes either way, as on a link that stopped
  passing traffic; new connections are accepted and frozen too.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlsplit, urlunsplit

CHUNK_BYTES = 64 * 1024


class TcpProxy:
    """Forwards every connection made to `port` to the upstream address."""

    def __init__(self, upstream_host: str, upstream_port: int) -> None:
        self._upstream = (upstream_host, upstream_port)
        self._server: asyncio.Server | None = None
        self._cut = False
        # Set while bytes may pass.
        self._flowing = asyncio.Event()
        self._flowing.set()
        self._writers: set[asyncio.StreamWriter] = set()
        self._pipes: set[asyncio.Task[None]] = set()

    @property
    def port(self) -> int:
        assert self._server is not None, "the proxy is not started"
        port: int = self._server.sockets[0].getsockname()[1]
        return port

    def url(self, upstream_url: str) -> str:
        """`upstream_url` with its host and port replaced by the proxy's; user, password, path
        and query kept."""
        parts = urlsplit(upstream_url)
        credentials, _, _ = parts.netloc.rpartition("@")
        netloc = f"{credentials}@127.0.0.1:{self.port}" if credentials else f"127.0.0.1:{self.port}"
        return urlunsplit(parts._replace(netloc=netloc))

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)

    def cut(self) -> None:
        """Reset every connection and refuse new ones until `restore()`."""
        self._cut = True
        self._flowing.set()
        for writer in self._writers:
            writer.transport.abort()
        self._writers.clear()

    def freeze(self) -> None:
        """Keep every connection open but pass nothing until `restore()`."""
        self._flowing.clear()

    def restore(self) -> None:
        self._cut = False
        self._flowing.set()

    async def close(self) -> None:
        self.cut()
        if self._server is not None:
            self._server.close()
        for pipe in self._pipes:
            pipe.cancel()
        await asyncio.gather(*self._pipes, return_exceptions=True)
        if self._server is not None:
            await self._server.wait_closed()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._cut:
            writer.transport.abort()
            return
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(*self._upstream)
        except OSError:
            writer.transport.abort()
            return
        if self._cut:
            # Cut while the upstream connection was opening.
            writer.transport.abort()
            upstream_writer.transport.abort()
            return
        self._writers |= {writer, upstream_writer}
        for source, target in ((reader, upstream_writer), (upstream_reader, writer)):
            pipe = asyncio.create_task(self._pipe(source, target))
            self._pipes.add(pipe)
            pipe.add_done_callback(self._pipes.discard)

    async def _pipe(self, source: asyncio.StreamReader, target: asyncio.StreamWriter) -> None:
        # An aborted connection ends the pipe with ConnectionError, or with b"" at the other end.
        with contextlib.suppress(ConnectionError):
            while data := await source.read(CHUNK_BYTES):
                # Frozen: the chunk is held, and TCP backpressure stops the sender after that.
                await self._flowing.wait()
                if target.is_closing():
                    return
                target.write(data)
                await target.drain()
        target.close()
        self._writers.discard(target)


@asynccontextmanager
async def running_proxy(upstream_url: str) -> AsyncIterator[TcpProxy]:
    """A started proxy to the host and port of `upstream_url`, closed afterwards."""
    parts = urlsplit(upstream_url)
    assert parts.hostname is not None and parts.port is not None, "the URL needs a host and port"
    proxy = TcpProxy(parts.hostname, parts.port)
    await proxy.start()
    try:
        yield proxy
    finally:
        await proxy.close()
