# ADR 0004 — Dispatch and consumption over RabbitMQ

- **Status:** Accepted
- **Date:** 2026-10-29

## Context

ADR 0002 decided that a dispatcher publishes due jobs from the jobs table with publisher confirms
and the `mandatory` flag, and that a message carries only the job ID. ADR 0001 decided how a worker
claims a job, fences its later writes by the claimed attempt and when it acknowledges. Three things
were left open for phase 3:

1. The queue type (ADR 0003).
2. What the worker does with a message when the claim fails with a database error (ADR 0001
   decision 7), and, more generally, which broker action follows each outcome.
3. How a returned message reaches the dispatcher in the chosen client, aio-pika (noted as
   unverified in phase 1).

RabbitMQ's answer to "how often may a message be redelivered" depends on how the consumer gives it
back, so these questions are decided together. The behaviour below was observed on RabbitMQ 4.3.7
with aio-pika 10.2.1 on 2026-10-29, unless marked as taken from the documentation.

## Decision

1. Jobs go to one durable **quorum queue** (`x-queue-type: quorum`), named by configuration
   (`jobs` by default). The project requires RabbitMQ 4.3 or later. No delivery limit is set, so
   RabbitMQ's default of 20 applies (documentation). Messages are persistent and published to the
   default exchange with the queue name as the routing key.
2. The dispatcher and the worker both declare the queue when they start, from one shared module.
   Declaring the same queue twice is a no-op; a declaration with different arguments fails with
   `PRECONDITION_FAILED` (observed), and the process stops instead of running against the wrong
   queue.
3. The dispatcher publishes on a channel with publisher confirms and **`on_return_raises=True`**,
   with `mandatory=True` and a publish timeout. With aio-pika's default `on_return_raises=False`,
   publishing to a queue that does not exist does not raise: `publish()` returns the returned
   message (observed). The dispatcher would then mark a job dispatched that no queue received. A
   publish counts as dispatched only when it returns the broker's ack. A nack (`DeliveryError`), a
   returned message (`PublishError`, a subclass of `DeliveryError`), a timeout and any other publish
   error, such as a lost connection, leave `dispatched_at` NULL, so the job is published again on a
   later poll (ADR 0002).
4. The message body is `{"message_version": 1, "job_id": "<uuid>"}` (ADR 0002 decision 3). The
   worker parses it strictly, with a size limit, before it touches the database.
5. The worker consumes with **prefetch 1** and handles one message at a time. More throughput means
   more worker processes, not more concurrency inside one.
6. The worker answers each outcome as follows:

   | Outcome | Broker action | Job |
   |---------|---------------|-----|
   | Message cannot be parsed | `reject(requeue=False)` | Not touched |
   | No job with this ID | `ack` | None exists |
   | Claim updates 0 rows (duplicate, not due yet, already finished) | `ack` | Unchanged (ADR 0001) |
   | **Database error during the claim** | wait `worker_db_error_backoff_s`, then `nack(requeue=True)` | Unchanged, or claimed if the error hid a commit |
   | Stored payload fails validation | `ack` after the fenced update | `running → failed`, `invalid_payload` |
   | Handler raises or exceeds `handler_timeout_s` | `ack` after the fenced update | `running → failed`, safe error code |
   | Handler succeeds | `ack` after the fenced update | `running → succeeded` |
   | Final fenced update fails with a database error | `nack(requeue=True)` | Stays `running` |
   | Final fenced update updates 0 rows (stale worker) | `ack` | Owned by a newer attempt |
   | `ack` or `nack` fails (channel or connection gone) | nothing; the broker has requeued it | Final state committed; the redelivery loses the claim and is acked |

   Until phase 5 adds retries, every handler failure is treated as non-retryable, so the job ends
   as `failed` and ADR 0001's retry transition (`running → queued`) is not used yet. Error codes
   are fixed categories; exception text is never stored or logged, since it can quote the payload.
   A failed `ack` or `nack` is logged and otherwise ignored: the broker requeues every
   unacknowledged message of a closed channel, so the worker keeps consuming instead of stopping.
7. **`nack` and `reject` are used for what they mean to RabbitMQ.** Since 4.3, `nack(requeue=True)`
   does not count towards the delivery limit, while `reject(requeue=True)` and a channel closed with
   the message unacknowledged do (observed through the `x-delivery-count` header). A message over
   the limit is dropped while no dead-letter exchange exists (observed with a limit of 1). A
   database outage is "not processed", so it gets `nack`: no message is lost however long the
   outage lasts, and the backoff keeps the worker from spinning on redeliveries. A message that
   cannot be parsed will never succeed, so it is rejected without requeue: dropped and logged now,
   dead-lettered from phase 6.
8. The worker's timeouts are ordered `handler_timeout_s < lease_duration_s < consumer_timeout`
   (ADR 0003). The first pair is checked when settings load. RabbitMQ's `consumer_timeout` is a
   broker setting (30 minutes by default) and is documented, not configured, here. The dispatcher
   holds its row locks for the whole batch: every publish and confirm, then the update and the
   commit. One publish timeout bounds all of the batch's publishes and confirms together, and it is
   shorter than the database statement timeout, because a worker's claim can wait on those row
   locks until the dispatcher commits (ADR 0002). A claim that still times out takes the
   database-error row in decision 6.
9. Integration tests declare a queue with a unique name per test and delete only that queue
   afterwards. They never purge or delete a queue they did not create.

## Consequences

- A broker outage, a returned message and an unconfirmed publish all end the same way: the job
  stays `queued` with `dispatched_at` NULL and goes out on a later poll. None of them can produce a
  false "dispatched".
- A database outage leaves every in-flight message in the queue. Each worker gets its message back
  about once per `worker_db_error_backoff_s`, which is bounded load, not a hot loop.
- If the claim's commit succeeded but its result was lost, the requeued message later loses the
  claim and is acknowledged, and the job stays `running` until stale-job recovery (phase 8).
- Connection loss counts towards the delivery limit. A message whose worker dies more than 20
  times is dropped until phase 6 adds a dead-letter exchange. If no delivery got as far as the
  claim, its job stays `queued` with `dispatched_at` set until lost-message recovery (phase 8, ADR
  0002 decision 7). If one did, the job is `running` and the dropped copy would have lost the claim
  anyway.
- `nack` never counts, so only the two database-error rows can return a message without limit.
  The claim row waits for the backoff first. The final-update row requeues at once, but its
  redelivery either loses the claim and is acknowledged (once the database answers) or hits the
  claim row and its backoff (while it is still down).
- Prefetch 1 keeps the ack rule simple and makes the backlog visible as queue depth. One slow
  handler holds up only its own worker process.
- Several dispatchers and workers can run at once: `SKIP LOCKED`, the claim and fencing keep them
  correct, so nothing here assumes a single process.

## Alternatives considered

- **Classic queue.** Simpler, with no delivery limit, and therefore no poison-message protection:
  a message whose worker keeps dying is redelivered without end. RabbitMQ 4.3 also no longer
  enforces `consumer_timeout` for classic queues (documentation, per ADR 0003; not re-checked in
  phase 3). Rejected.
- **Unlimited delivery (`x-delivery-limit=-1`).** It keeps a message whose worker keeps crashing,
  but RabbitMQ's documentation advises against it, and the database lease already bounds the job
  itself (ADR 0001 decision 8). Rejected.
- **Topology in a `definitions.json` loaded by RabbitMQ at boot.** It keeps declarations out of the
  code, but CI service containers start before the repository is checked out, so the file cannot
  be mounted there. Rejected.
- **Ack and drop on a database error during the claim.** The job would then depend on lost-message
  recovery, which does not exist before phase 8. Rejected.
- **`reject(requeue=True)` on a database error.** Each attempt counts towards the delivery limit,
  so an outage of about 20 redeliveries would drop messages. Rejected.
- **A dedicated exchange now.** It adds nothing until dead-lettering needs one (phase 6). Deferred.
- **Prefetch above 1 with concurrent handlers.** More throughput per process, at the cost of
  concurrent sessions, partial acks and harder shutdown. Deferred until there is a measured need.
