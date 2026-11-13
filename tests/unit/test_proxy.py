"""The TCP proxy the outage tests use, against a local echo server."""

import asyncio
from collections.abc import AsyncIterator

import pytest

from tests.proxy import TcpProxy, running_proxy


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(1024):
            writer.write(data)
            await writer.drain()
    except ConnectionError:
        pass
    finally:
        writer.close()


@pytest.fixture
async def proxy() -> AsyncIterator[TcpProxy]:
    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with running_proxy(f"tcp://127.0.0.1:{port}") as proxy:
            yield proxy
    finally:
        server.close()
        await server.wait_closed()


async def connect(proxy: TcpProxy) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.open_connection("127.0.0.1", proxy.port)


async def round_trip(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bytes:
    writer.write(b"ping")
    await writer.drain()
    async with asyncio.timeout(1):
        return await reader.readexactly(4)


async def closed_by_peer(reader: asyncio.StreamReader) -> bool:
    try:
        async with asyncio.timeout(1):
            return await reader.read(1) == b""
    except ConnectionError:
        return True


async def test_bytes_pass_both_ways(proxy: TcpProxy) -> None:
    reader, writer = await connect(proxy)
    assert await round_trip(reader, writer) == b"ping"
    writer.close()


async def test_a_cut_resets_open_connections_and_refuses_new_ones_until_restored(
    proxy: TcpProxy,
) -> None:
    reader, writer = await connect(proxy)
    assert await round_trip(reader, writer) == b"ping"

    proxy.cut()
    assert await closed_by_peer(reader)
    new_reader, _ = await connect(proxy)
    assert await closed_by_peer(new_reader)

    proxy.restore()
    reader, writer = await connect(proxy)
    assert await round_trip(reader, writer) == b"ping"
    writer.close()


async def test_a_freeze_holds_bytes_on_an_open_connection_until_restored(proxy: TcpProxy) -> None:
    reader, writer = await connect(proxy)
    proxy.freeze()
    writer.write(b"ping")
    await writer.drain()
    # Nothing passes, and the connection is not closed either.
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.3):
            await reader.read(1)

    proxy.restore()
    async with asyncio.timeout(1):
        assert await reader.readexactly(4) == b"ping"
    writer.close()


def test_url_points_at_the_proxy_and_keeps_the_rest(proxy: TcpProxy) -> None:
    url = "postgresql+asyncpg://user:pa%40ss@db.example:5432/jobs_test?ssl=disable"
    assert proxy.url(url) == (
        f"postgresql+asyncpg://user:pa%40ss@127.0.0.1:{proxy.port}/jobs_test?ssl=disable"
    )
    assert proxy.url("amqp://host:5672/") == f"amqp://127.0.0.1:{proxy.port}/"
