# ADR 0003 — Async I/O stack: SQLAlchemy asyncio, asyncpg, aio-pika

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

Three processes do I/O: the API (HTTP and PostgreSQL), the dispatcher (PostgreSQL and RabbitMQ) and
the worker (RabbitMQ, PostgreSQL and the task handler). They share the domain and service layers, so
they should share one concurrency model. Choosing that model also chooses the database driver and the
RabbitMQ client.

The RabbitMQ client matters most. A consumer must keep its connection alive (heartbeats) while a
handler runs. If heartbeats stop, the broker closes the connection and redelivers the message. In
this design the redelivered copy loses the claim (ADR 0001 decision 7), so the job does not run
twice. The cost is operational: the worker can no longer acknowledge the message, the channel's
other unacknowledged messages are requeued, and the worker has to reconnect.

RabbitMQ also enforces a delivery acknowledgement timeout (`consumer_timeout`, 30 minutes by
default). If a message is not acknowledged within it, the broker cancels the consumer or closes the
channel and requeues its unacknowledged messages. Since RabbitMQ 4.3 this timeout is enforced only
for quorum queues; the queue type is decided in phase 3.

## Decision

1. All three processes use `asyncio`. API endpoints are `async def`.
2. Database access uses SQLAlchemy 2.0's asyncio extension with the `asyncpg` driver.
3. RabbitMQ access uses `aio-pika`, with a robust connection (`connect_robust`) that reconnects
   automatically.
4. Task handlers are `async`. A handler that has to make a blocking call wraps it in
   `asyncio.to_thread`; CPU-heavy work is out of scope for the MVP.
5. Each concurrent task gets its own `AsyncSession`. Sessions are never shared between tasks.
6. Async test support (`pytest-asyncio`, `asyncio_mode = "auto"`, function-scoped event loops) is
   added in phase 2, together with the first async code. How Alembic connects (async template or a
   separate sync driver) is also decided in phase 2.

## Consequences

- One model in all three processes, so the services layer can be called the same way from all of
  them.
- While a handler is awaiting I/O, the event loop keeps serving heartbeats and acknowledgements.
- The flip side: **any blocking call in a handler stalls the whole process**, including heartbeats.
  Code review has to watch for blocking calls in async code.
- Timeouts must be ordered: handler timeout < job lease (ADR 0001 decision 8) < RabbitMQ
  `consumer_timeout`. If a handler outlives its lease, recovery hands the job to another worker
  while it is still running, and its side effect can happen twice. If a handler outlives
  `consumer_timeout`, the broker requeues the message; the copy loses the claim, so the cost is a
  closed channel and a reconnect, not duplicate work.
- A timeout cancels the awaiting coroutine, not a thread started by `asyncio.to_thread`: the
  blocking call keeps running after the handler timeout, so "handler timeout < lease" does not hold
  for it. Its late database writes are still fenced (ADR 0001 decision 7); its side effects are not.
- Async tests need event-loop fixtures and are harder to debug than sync tests.
- The same SQLAlchemy and asyncpg setup was used in the author's previous project,
  banking-transaction-analytics, so its conventions carry over.

## Alternatives considered

- **Sync stack: psycopg 3 and pika.** It is easier to read and debug. But pika's
  `BlockingConnection` is not thread-safe and only sends heartbeats while the connection is
  processing I/O, so a long handler running in the consumer callback can miss heartbeats and lose
  the connection unless the work is moved to a thread and the result handed back through the
  connection's own thread. That is more fragile code than the async version. Rejected.
- **Mixed: async API, sync worker.** It keeps the worker simple, but the services layer would need
  two versions or an adapter. Rejected.
- **Kombu.** It abstracts over several brokers and hides AMQP details (acknowledgements, prefetch,
  dead-letter exchanges) that this project is meant to show explicitly. Rejected.
- **aiormq directly.** aio-pika is built on it and adds robust reconnection. Rejected as too low
  level.
