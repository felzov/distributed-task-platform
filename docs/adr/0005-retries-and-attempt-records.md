# ADR 0005 — Retries and attempt records

- **Status:** Accepted (updated 2026-11-10)
- **Date:** 2026-11-05

## Context

Until phase 5, every handler failure ends a job as `failed` (ADR 0004 decision 6). ADR 0001 already
allows `running → queued` for a retry and counts an attempt when a worker claims the job, and ADR
0002 decision 4 decided how a retry reaches a worker again: the job goes back to `queued` with a
later `available_at` and `dispatched_at = NULL`, and the dispatcher publishes it when it is due.
ADR 0001 decision 5 left the backoff policy to phase 5.

Four questions remain:

1. Which failures are worth another attempt?
2. How long does a job wait before it is tried again?
3. How is each attempt recorded, so that retries can be inspected and not only counted?
4. What does a job that succeeds after failed attempts keep of their errors?

## Decision

1. **Only the handler's own failures are retried.** `handler_error` and `handler_timeout` are
   retryable: the next attempt may not hit the same error. A stored payload that no longer passes
   validation (`unknown_job_type`, `unsupported_payload_version`, `payload_too_large`,
   `invalid_payload`) fails the same way on every attempt, so the job fails at once. That includes
   `unknown_job_type` raised because this worker has no handler for a known type. Every
   `FailureCode` is classified explicitly in `domain/failures.py`, so a new code cannot become
   retryable or permanent by omission.
2. **A retryable failure is retried while attempts are left.** After attempt `n` fails with a
   retryable code, the job is retried if `n < max_attempts` and fails otherwise. `max_attempts` is
   the budget set at submission (1–20); the attempt counted at claim time (ADR 0001 decision 4) is
   what reaches it.
3. **Exponential backoff with full jitter.** After attempt `n` fails, the job waits
   `jitter × min(retry_max_delay_s, retry_base_delay_s × 2^(n−1))`, with `jitter` drawn uniformly
   from `[0, 1)` for each retry. The domain computes the delay from a `jitter` value it is given, so
   the function is deterministic and tested with fixed values; the worker draws the random number.
   PostgreSQL turns the delay into a timestamp, `available_at = now() + delay` (ADR 0001 decision
   9). Both delays are settings, with placeholder defaults of 2 s and 300 s (not measured); the cap
   is at most one day.
4. **A retry is a database write, then an ack.** The worker's fenced update moves the job from
   `running` to `queued`, sets `available_at`, clears `dispatched_at` and the lease, and stores the
   failure's code; then it acknowledges the message. The next delivery is a new message, published
   by the dispatcher once the job is due (ADR 0002 decision 4). The update is fenced like the final
   ones (ADR 0001 decision 7) and also requires `attempts < max_attempts`, so a stale worker cannot
   requeue a job, and a requeued job always has an attempt left to be claimed with.
5. **Every attempt has a row in `job_attempts`.** The row is keyed by `(job_id, attempt)`. The claim
   inserts it, with `started_at`, in the same transaction that moves the job to `running`; the
   finish closes it, with `finished_at`, the outcome (`succeeded` or `failed`) and the failure code,
   in the same transaction as the fenced job update, and only if that update applied. A stale
   attempt therefore writes neither the job nor its attempt row. An attempt whose worker crashed
   stays open (`finished_at` NULL) until stale-job recovery (phase 8) closes it.
6. **`jobs.error_code` describes the job, `job_attempts` its history.** A retried job that is
   `queued` keeps the code of its last failed attempt (migration 0002 does not tie `error_code` to
   the status). A job that succeeds after failed attempts has its `error_code` cleared; the failed
   attempts and their codes stay in `job_attempts`.
7. **No backfill.** Jobs that ran before `job_attempts` existed have no attempt rows; their history
   starts with the migration. Their claim times are not stored anywhere, so a backfill would have to
   invent them.

**Update 2026-11-10 (phase 7):** the consequence that a retry repeats the handler's side effects
no longer holds for `notification.send`: a retry after the provider was called, whether the
handler then raised or timed out, gets the same notification back and records nothing new (ADR
0007 decision 8; tested for both codes, and end to end with three attempts that each reach the
provider). It still holds for any future handler whose receiver takes no key.

## Consequences

- The number of attempts is bounded by `max_attempts`, each wait by `retry_max_delay_s`, so a
  job's total backoff is at most `(max_attempts − 1) × retry_max_delay_s`. A job that fails with a
  retryable code on every attempt makes `max_attempts` attempts and then fails with the last
  attempt's code.
- Jitter spreads the retries of jobs that failed together, for example while a provider was down,
  instead of sending them back in waves. It does not stop them: there is no circuit breaker, and a
  provider that stays down sees every job's full set of attempts.
- Full jitter has no minimum wait: it can draw a delay close to zero, and the next poll can come
  at any moment, so a retry can be published almost at once. The dispatcher's poll interval only
  adds between zero and `dispatch_poll_interval_s` to each wait; it is not a floor.
- A retry repeats the handler's side effects. A handler that called the provider and then timed out
  calls it again on the next attempt. Fencing does not prevent this (ADR 0001); idempotent handlers
  do (phase 7).
- The broker sees nothing of a retry: no requeue, no redelivery count, no delay queue. A broker
  outage during the backoff only delays the next publish.
- A retried job shows as `queued` with `attempts > 0` (ADR 0001 decision 5); the reason for each
  earlier attempt is read from its attempt rows.
- The attempt rows add one insert per claim and one update per finish, inside transactions that
  already exist. A job has at most 20 rows (`max_attempts` ≤ 20).

## Alternatives considered

- **Retrying every failure.** Simpler, but a payload that fails validation would use up all its
  attempts on the same error and delay the final answer by the whole backoff. Rejected.
- **Letting handlers mark an error as permanent.** More precise, but there is one handler and no
  error of its own that needs it. Deferred until a handler does.
- **No jitter.** Jobs that failed together would retry together, every time. Rejected.
- **Equal jitter** (`ceiling/2 + jitter × ceiling/2`). It guarantees at least half the delay, at the
  cost of spreading the retries over only half the range. Rejected: spreading the retries of jobs
  that failed together is the point of the jitter, and full jitter spreads them over the whole
  range. The cost, an occasional retry that comes almost at once, is bounded by `max_attempts`. This
  is a reasoned choice, not a measured one.
- **Decorrelated jitter.** Each delay depends on the previous one, which the job would have to
  store, and the policy is harder to explain and test. Rejected.
- **RabbitMQ delay: per-message TTL with a dead-letter exchange, or a delayed-message plugin.**
  Rejected in ADR 0002: the job's state, its timing included, stays in PostgreSQL.
- **`nack` with requeue and no delay.** The retry would come at once, with no backoff. Rejected.
- **Recording an attempt only when it finishes.** One write instead of two, but an attempt whose
  worker crashed would leave no trace until recovery. Rejected.
- **A JSONB array of attempts on `jobs`.** One table, but no constraints or foreign key per attempt,
  and every finish rewrites the whole array. Rejected.
- **Keeping `error_code` on a job that succeeded.** No change to the success update, but the job
  would show an error that no longer applies. Rejected: the history is in `job_attempts`.
