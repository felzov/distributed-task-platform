# ADR 0001 — Job lifecycle and state transitions

- **Status:** Accepted (updated 2026-10-29)
- **Date:** 2026-10-24

## Context

A job moves between the API, the dispatcher and one or more workers. Delivery from RabbitMQ is
at-least-once, so the same job can reach a worker twice, and a worker can crash at any point while
running it. The job's status in PostgreSQL is the only source of truth (the broker message proves
nothing), so the set of statuses and the allowed transitions between them must be explicit, small
enough to test exhaustively, and enforced in one place.

Two questions shape the design:

1. How does a client see that a job is being retried?
2. When is an attempt counted, so that retries stay bounded even when a job crashes the worker?

## Decision

1. A job has exactly four statuses: `queued`, `running`, `succeeded`, `failed`. They are stored as
   `text` with a `CHECK` constraint, not a PostgreSQL `ENUM`, following the convention from previous
   projects (an `ENUM` cannot drop values and is awkward to change in migrations).
2. The allowed transitions are:

   | From      | To          | When |
   |-----------|-------------|------|
   | `queued`  | `running`   | A worker claims the job. |
   | `running` | `succeeded` | The handler finished successfully. |
   | `running` | `queued`    | Retryable failure with `attempts < max_attempts`, or lease expired with `attempts < max_attempts` (decision 8). |
   | `running` | `failed`    | Non-retryable failure, or `attempts >= max_attempts` (after a failure or an expired lease). |

   Every other pair, including staying in the same status, is invalid.
3. `succeeded` and `failed` are terminal. Nothing leaves them.
4. `attempts` is incremented **when a worker claims the job** (`queued → running`), in the same
   update that changes the status and sets the lease (decision 8), not when the attempt fails.
5. A retry is `running → queued` with `available_at` moved into the future. There is no separate
   `retry_scheduled` status; a client sees a retry as `status = queued` with `attempts > 0`. The
   backoff policy itself is decided in phase 5. Every `running → queued` transition, whether a retry
   or lease recovery (decision 8), sets `available_at` and `dispatched_at = NULL` in the same update,
   so the dispatcher publishes the job again when it is due (ADR 0002).
6. The transition table lives only in `src/task_platform/domain/job_status.py`. Routers, the
   dispatcher and workers ask the domain whether a transition is valid; none of them encodes its own
   rules.
7. The domain check is not enough under concurrency: two workers can both read `queued` and both
   pass the check. The data layer therefore applies every transition as a conditional update and
   treats "0 rows updated" as "someone else got there first":
   - The claim: `UPDATE jobs SET status = 'running', attempts = attempts + 1, ... WHERE id = :id
     AND status = 'queued' AND available_at <= now()`. A worker that loses a claim acknowledges the
     message and drops it. This is the normal outcome of a duplicate delivery, not an error. The
     `available_at` condition stops a stale duplicate message from starting a job that is waiting
     for its retry backoff; the dispatcher publishes that job again when it is due.
   - Every later update by the worker is **fenced** by the attempt number it claimed:
     `... WHERE id = :id AND status = 'running' AND attempts = :claimed_attempt`. If the job has
     since been recovered and claimed again (decision 8), the stale worker's write updates 0 rows and
     is discarded, so it cannot overwrite the newer attempt's status or result.
   - The worker acknowledges a message only after its final fenced update has committed, or right
     after losing the claim. The message therefore stays unacknowledged for the whole handler run:
     that is what makes prefetch limit in-flight work, and why the handler must finish within
     RabbitMQ's `consumer_timeout` (ADR 0003). What the worker does when the claim itself fails with
     a database error is decided in phase 3.
8. **Crashed workers are recovered through a lease, not through the broker.** The claim sets
   `lease_expires_at = now() + lease_duration`. The worker enforces a handler timeout shorter than
   the lease. Stale-job recovery (phase 8) periodically moves `running` jobs whose lease has expired
   to `queued` (if `attempts < max_attempts`, resetting `dispatched_at` as in decision 5) or `failed`
   (otherwise), using the transitions above.
   RabbitMQ's redelivery does not recover these jobs: after a worker crash the job is `running`, so a
   redelivered message loses the claim in decision 7 and is dropped.
9. All job timestamps (`available_at`, `lease_expires_at`, `created_at`, `updated_at`) are computed
   by PostgreSQL (`now()`, `now() + interval`), never from a worker's or the API's clock, so clock
   drift between machines cannot shift retries or leases.

**Update 2026-10-29 (phase 3):** the open point in decision 7 is closed by ADR 0004. When the claim
fails with a database error, the worker waits a configured backoff and then returns the message with
`nack(requeue=True)`, which does not count towards RabbitMQ's delivery limit.

## Consequences

- Four statuses give 16 `(from, to)` pairs, so the whole table can be covered by one parametrised
  test.
- Counting attempts at claim time, together with lease recovery, bounds "poison" jobs. If a job
  crashes the worker every time, no failure is ever recorded, but each claim still counts and each
  expired lease sends the job back through recovery, so it reaches `max_attempts` and ends in
  `failed` instead of looping forever.
- Until stale-job recovery exists (phase 8), a job whose worker crashes stays `running`.
- Recovery time after a crash is up to the lease duration plus the recovery interval. A longer lease
  allows slower handlers but delays recovery.
- Because `attempts` only increases, it doubles as the fencing token in decision 7; no extra column
  is needed.
- Fencing rejects a stale worker's *database writes*, not its *side effects*. A worker whose lease
  expired may still finish its handler, so the side effect can happen twice. Handlers with side
  effects must be idempotent (phase 7).
- The cost of the same rule: an attempt is also used up when the worker dies for reasons unrelated
  to the job (for example, the container is killed during a deploy). This is accepted, since
  `max_attempts` is a configured budget, not an exact count of handler errors.
- Retries are less visible than with a dedicated status. Clients must read `attempts` and
  `available_at` to tell a fresh job from a retried one.
- There is no cancellation and no manual re-queue of `failed` jobs. Adding a status later means a
  migration that changes the `CHECK` constraint, plus new rows in the transition table and its tests.

## Alternatives considered

- **Five statuses, with `retry_scheduled`.** It makes retries visible directly in `status`, but adds
  transitions to test and enforce, while giving the dispatcher nothing new: it already selects jobs
  by `status = queued AND available_at <= now()`, which covers both new and retried jobs (see ADR
  0002). Rejected for the MVP.
- **Counting attempts when the attempt fails.** It is simpler to explain, but a job that crashes the
  worker never reaches the failure path, so its attempt is never counted and it retries without
  limit. Rejected.
- **Re-claiming an expired lease from a redelivered message** (allowing `running → running` when
  `lease_expires_at < now()`). It looks like a way to put RabbitMQ redelivery to use after a crash,
  but the redelivered message usually arrives right after the crash, while the lease is still
  valid, so it would be dropped anyway and the periodic recovery would still do the work. It also
  adds a transition and a second recovery path. Rejected for the MVP.
- **A separate `dispatched` status.** Whether a message has been published is transport state, not
  job state; it is tracked by `dispatched_at` instead (ADR 0002). Rejected.
- **A PostgreSQL `ENUM` for `status`.** Rejected for the migration reasons in decision 1.
