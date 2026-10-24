# ADR 0002 — Dual write: the jobs table as the outbox

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

Submitting a job has to change two systems: PostgreSQL (the job record) and RabbitMQ (the message
that wakes a worker). There is no transaction that spans both, so the naive orderings fail:

- **Commit, then publish.** If the publish fails or the process dies in between, the job is saved
  but no worker ever hears about it. If the API reports success anyway, it has claimed a dispatch it
  never observed.
- **Publish, then commit.** If the commit fails, a worker receives a job that does not exist.

The project's invariants: PostgreSQL is the source of truth, the API never reports a dispatch it did
not observe, and delivery is at-least-once.

## Decision

1. The API writes **only to PostgreSQL**. `POST /jobs` inserts the job with `status = queued`,
   `available_at = now()` and `dispatched_at = NULL` in a single transaction, then returns
   `202 {id, status: "queued"}`. "Queued" means *saved and waiting for dispatch*, not *published*.
   The API never talks to RabbitMQ.
2. A separate **dispatcher** process repeatedly:
   1. selects due jobs in a transaction:
      `SELECT ... FROM jobs WHERE status = 'queued' AND dispatched_at IS NULL AND available_at <= now()
       ORDER BY available_at LIMIT :batch_size FOR UPDATE SKIP LOCKED`;
   2. publishes one persistent message per job to a durable queue with the `mandatory` flag and
      waits for the broker's publisher confirm;
   3. sets `dispatched_at = now()` for the jobs whose publish was confirmed and not returned, and
      commits. After a nack, a returned message or a publish timeout, `dispatched_at` stays `NULL`,
      so the job is picked up again on a later poll.

   The `mandatory` flag matters because a publisher confirm only means the broker took
   responsibility for the message. If no queue is bound to receive it, the broker drops the message
   and still confirms it. With `mandatory`, the broker returns the message instead, and the
   dispatcher treats that as a failed publish.
3. The message body is only `{"message_version": 1, "job_id": "<uuid>"}`. The worker loads
   everything else from PostgreSQL.
4. **Retries use the same mechanism.** To retry a job, the worker sets `status = queued`,
   `available_at = now() + backoff` (computed by PostgreSQL, see ADR 0001 decision 9) and
   `dispatched_at = NULL`. The dispatcher picks it up when it is
   due. No RabbitMQ delay queues or per-message TTLs are used.
5. Duplicate publishes are allowed. If the dispatcher dies after the broker confirm but before its
   commit, the job is published again. Workers tolerate this through the atomic claim in ADR 0001,
   decision 7.
6. The poll interval, batch size and publish timeout are explicit configuration values.
7. A job that was dispatched but whose message never reached a worker stays `queued` with
   `dispatched_at` set. Phase 8 adds recovery for it: clear `dispatched_at` on `queued` jobs that were
   dispatched longer ago than a configured threshold. The system cannot tell a lost message from one
   still waiting in a long queue, so the threshold must be well above the expected time a message
   waits in the queue.

## Consequences

- No dual write on the request path. While RabbitMQ is down, the API keeps accepting jobs: they
  wait in `queued`, and the dispatcher publishes them when the broker returns. No response claims a
  dispatch that did not happen.
- One mechanism (`available_at`, `dispatched_at`) handles first dispatch, retry backoff and, in
  phase 8, re-dispatch of lost messages.
- There is extra latency of up to one poll interval between submission and dispatch.
- The dispatcher polls PostgreSQL even when idle. The query needs an index that matches it, for
  example a partial index on `(available_at) WHERE status = 'queued' AND dispatched_at IS NULL`;
  the exact index is decided with the migration in phase 2.
- The dispatcher holds row locks while it waits for publisher confirms, so a slow broker keeps a
  transaction open. Small batches and a publish timeout keep that bounded. A worker can receive a
  message before the dispatcher commits; its claim then waits on the dispatcher's row lock until
  that commit. This is still correct, but it is why the claim needs a statement timeout.
- If the queue backs up past the lost-message threshold, recovery republishes jobs whose messages
  are only delayed. Each pass adds another copy per waiting job. The claim makes the copies harmless
  for correctness, but they make the backlog worse. This is the retry-storm risk; phase 8 decides
  whether recovery should also check queue depth before republishing.
- There is one more process to run, monitor and shut down cleanly.
- `FOR UPDATE SKIP LOCKED` lets several dispatchers run without publishing the same job at the same
  time. A single dispatcher is enough for the MVP.
- No throughput figures are claimed; dispatch speed is limited by PostgreSQL and has not been
  measured.

## Alternatives considered

- **Publish after commit in the request, plus a reaper.** The request path then depends on the
  broker: the API needs a timeout for the publish, and a second recovery path for jobs it could not
  publish. That recovery path ends up being the dispatcher anyway. Rejected.
- **A separate outbox table.** This is the classic transactional outbox. It needs a second table, an
  extra insert per job and cleanup of old outbox rows, and in this design it would hold exactly one
  row per job dispatch, the same information `dispatched_at` already records. Rejected as redundant
  here; it is the better choice when one transaction emits several different events.
- **Publish before commit.** It can produce messages for jobs that were never saved. Rejected.
- **No broker: workers poll PostgreSQL directly with `SKIP LOCKED`.** This is a real and simpler
  option, and with this ADR's polling dispatcher it is a fair question whether RabbitMQ is still
  needed. Crash recovery does not decide it, because it comes from the lease in PostgreSQL either
  way (ADR 0001 decision 8). RabbitMQ is kept because it pushes work to idle workers instead of
  every worker polling the database, limits in-flight work per worker with prefetch, redelivers
  messages that a worker received but had not yet claimed, and provides dead-letter exchanges
  (phase 6), and because working with real broker semantics is a goal of this project. Revisit if
  the broker turns out to add complexity without these benefits.
- **`LISTEN/NOTIFY` to wake the dispatcher.** It would cut latency, but the polling loop is still
  needed for retries and recovery. Deferred: it is an optimisation, not a correctness need.
