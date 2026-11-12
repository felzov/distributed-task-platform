"""The jobs queue's ready-message count, for lost-message recovery (ADR 0008 decision 2)."""

from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection


class ReadyCount:
    """Reads how many messages wait in a queue, ready for a consumer, on a channel of its own.

    The live count needs a passive declare on a channel where nothing else declares the queue:
    aio-pika's robust channel answers a passive declare of a name declared on it from a cache,
    with the count of that first declaration (ADR 0008, observed on aio-pika 10.2.1). With
    `robust=False` the passive declare is not cached either. Messages held unacknowledged by a
    consumer are not counted.
    """

    def __init__(self, connection: AbstractRobustConnection, queue_name: str) -> None:
        self._connection = connection
        self._queue_name = queue_name
        self._channel: AbstractRobustChannel | None = None

    async def ready_messages(self) -> int:
        """The queue's ready messages now. Raises if the broker cannot answer.

        Not bounded here: during a broker outage a robust channel waits for its connection to come
        back, so the caller bounds the call. After an error, such as a missing queue, the next
        call opens a new channel.
        """
        if self._channel is None:
            # Never publishes, so no confirms.
            self._channel = await self._connection.channel(publisher_confirms=False)
        channel = self._channel
        try:
            queue = await channel.declare_queue(self._queue_name, passive=True, robust=False)
        except Exception:
            # A missing queue closes the channel broker-side, and the robust channel then reopens
            # itself within moments (observed). Closed here as well, so it stays closed instead of
            # living on next to the channel the next call opens.
            self._channel = None
            await channel.close()
            raise
        count = queue.declaration_result.message_count
        if count is None:
            # Declare-Ok always carries the field; pamqp types it as optional.
            raise RuntimeError("the queue's Declare-Ok carried no message count")
        return count
