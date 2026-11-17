# Delivery guarantees

What the system promises about a job once it is submitted, what it does not promise, and which
decisions and tests each promise rests on. Test names are `file::test` under `tests/`; the
failure paths behind them are in [failure-modes.md](failure-modes.md). All times below are
settings, with their defaults; the defaults are placeholders, not measured values.

## A job's path

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant DB as PostgreSQL
    participant D as Dispatcher
    participant Q as RabbitMQ
    participant W as Worker

    C->>A: POST /jobs
    A->>DB: INSERT job (queued), COMMIT
    A-->>C: 202 {id, status} + Location: saved, not yet published
    Note over D,DB: one transaction from the poll to its COMMIT
    D->>DB: poll due jobs (FOR UPDATE SKIP LOCKED)
    D->>Q: publish job ID (persistent, mandatory)
    Note over D,W: the broker may deliver from here on, even before its confirm; a claim waits on the poll's row locks
    Q-->>D: confirm
    Note over D: dispatcher crash before the COMMIT: rolled back, published again
    D->>DB: mark dispatched, COMMIT
    Q->>W: deliver job ID
    W->>DB: claim: queued → running, attempt + 1, lease, open attempt row, COMMIT
    W->>W: run the handler (its side effect keyed by job)
    Note over W: worker crash here: the lease expires, recovery requeues the job or fails it
    W->>DB: final update fenced by status and attempt, COMMIT
    W->>Q: ack
```

## Guarantees

| Guarantee | How | Decision | Tests |
|---|---|---|---|
| A `202` means the job is saved: it outlives an API, broker or worker crash after the response. The API never says a job was published. | The service answers only after its own `COMMIT`, and the API never talks to the broker. That order is the service's own; no test crashes the API. A database error is `503` when the database is unavailable and `500` otherwise; neither carries a job ID. | [ADR 0002](adr/0002-dual-write-strategy.md) | `integration/test_submit_job.py::test_submitted_job_is_saved_as_queued`, `integration/test_submit_job_failures.py::test_dropped_connection_returns_503_and_the_next_request_reconnects`, `integration/test_submit_job_failures.py::test_check_violation_returns_500_without_the_row`, `integration/test_outages.py::test_a_broker_outage_delays_dispatch_and_nothing_claims_it_happened` |
| A saved job is published at least once, as long as a dispatcher runs and the broker comes back. | The jobs table is the outbox. One transaction polls the due jobs, publishes them and marks only those the broker confirmed; a crash or error before its `COMMIT` leaves them due, and they are published again. | [ADR 0002](adr/0002-dual-write-strategy.md), [ADR 0004](adr/0004-dispatch-and-consumption.md) | `integration/test_dispatch.py::test_unconfirmed_jobs_stay_due_and_the_next_poll_publishes_them`, `integration/test_dispatcher.py::test_a_job_is_marked_only_once_a_queue_took_its_message`, `integration/test_failure_combinations.py::test_a_dispatcher_killed_before_its_commit_publishes_again_and_the_job_runs_once` |
| A message lost after the mark is published again, once the jobs queue is seen empty of ready messages. | The lost-message sweep reads the queue's ready count first and runs only if it is zero. It then clears the mark of jobs still `queued` more than `LOST_MESSAGE_AFTER_S` after their mark, and the next poll publishes them. A count it cannot read skips the sweep. | [ADR 0008](adr/0008-recovery-and-shutdown.md) decision 2 | `integration/test_lost_messages.py::test_an_overdue_job_with_an_empty_queue_is_published_again`, `integration/test_lost_messages.py::test_ready_messages_in_the_queue_skip_the_sweep`, `integration/test_lost_messages.py::test_a_count_not_read_skips_the_sweep`, `integration/test_dispatcher.py::test_the_dispatcher_publishes_a_lost_message_again` |
| Each attempt is claimed once. A delivery for a job that is running, finished, not yet due or out of attempts is refused and acked. | The claim is one conditional `UPDATE`: the job must be `queued`, due and within its attempts. Any message naming a due `queued` job can claim it, so which of a job's duplicate messages wins does not matter. | [ADR 0001](adr/0001-job-lifecycle.md), [ADR 0004](adr/0004-dispatch-and-consumption.md) | `integration/test_execution.py::test_two_deliveries_of_one_job_run_its_handler_once`, `integration/test_worker.py::test_two_workers_given_one_job_twice_run_it_once_and_ack_both`, `integration/test_failure_combinations.py::test_an_ack_lost_on_a_broker_cut_brings_the_message_back_and_the_claim_refuses_it` |
| Only the current attempt can finish or retry the job. A worker whose job was recovered writes nothing to the job or its attempt rows. | The final update and the retry are fenced by the job's status (`running`) and the claimed attempt number, not by the lease: a lease that expired but was not yet recovered does not stop its worker from finishing. | [ADR 0001](adr/0001-job-lifecycle.md), [ADR 0008](adr/0008-recovery-and-shutdown.md) decision 1 | `integration/test_execution.py::test_a_stale_attempts_retry_is_discarded`, `integration/test_recovery.py::test_a_worker_finishing_after_the_sweep_writes_nothing` |
| A job does not stay `running` after its worker crashed, was stopped past its grace period or by a second signal, or could not write its final update: while a dispatcher runs, the job is run again or failed. | The claim sets a lease. The dispatcher's lease sweep requeues an expired job (or fails it on its last attempt) as `lease_expired` and closes its attempt row in the same transaction. | [ADR 0001](adr/0001-job-lifecycle.md) decision 8, [ADR 0008](adr/0008-recovery-and-shutdown.md) decision 1 | `integration/test_recovery.py::test_a_job_with_attempts_left_is_requeued_and_its_attempt_closed`, `integration/test_recovery.py::test_a_job_on_its_last_attempt_fails_and_is_listed_as_failed`, `integration/test_failure_combinations.py::test_a_worker_killed_after_its_notification_is_recovered_and_sends_no_second_one` |
| A job is claimed at most `max_attempts` times, and every claim is recorded. `max_attempts` is fixed when the job is saved, from `DEFAULT_MAX_ATTEMPTS` (5); the database allows 1 to 20. | The claim requires `attempts < max_attempts` and opens the attempt's row in its own transaction. | [ADR 0001](adr/0001-job-lifecycle.md), [ADR 0005](adr/0005-retries-and-attempt-records.md) | `integration/test_execution.py::test_the_claim_opens_the_attempts_row_before_the_handler_runs`, `integration/test_end_to_end.py::test_a_job_that_always_fails_stops_after_its_attempts` |
| Only failures a retry might fix are retried: the handler's error or timeout, and an expired lease. An invalid stored payload or a type without a handler fails at once. | Every failure code is classified as retryable or not. A handler error from a bug is retried until the attempts run out. | [ADR 0005](adr/0005-retries-and-attempt-records.md), [ADR 0008](adr/0008-recovery-and-shutdown.md) decision 1 | `unit/test_failures.py::test_the_handlers_own_failures_and_an_expired_lease_are_retryable`, `unit/test_retries.py::test_a_permanent_failure_fails_the_job_even_with_attempts_left`, `integration/test_execution.py::test_a_stored_payload_that_fails_validation_fails_the_job` |
| A retried job waits a full-jitter backoff: a random share of `min(RETRY_MAX_DELAY_S, RETRY_BASE_DELAY_S × 2^(n−1))` after failed attempt n (2 s base, 300 s cap). An expired lease is retried without one. | The retry sets the job's `available_at`; the dispatcher publishes it only once due. | [ADR 0005](adr/0005-retries-and-attempt-records.md), [ADR 0008](adr/0008-recovery-and-shutdown.md) decision 1 | `unit/test_backoff.py::test_the_ceiling_doubles_per_attempt_up_to_the_cap`, `unit/test_backoff.py::test_the_delay_is_the_jittered_share_of_the_ceiling`, `integration/test_execution.py::test_a_retryable_failure_with_attempts_left_is_retried` |
| `succeeded` and `failed` are final. A failed job is never retried automatically; it stays a row, listed by `GET /jobs?status=failed`. | Every write names the status it expects in its `WHERE` clause, and the transitions it may make are those of the domain's transition table. A test runs every write against every status. | [ADR 0001](adr/0001-job-lifecycle.md), [ADR 0006](adr/0006-dead-letters.md) | `unit/test_job_status.py::test_only_succeeded_and_failed_are_terminal`, `integration/test_transitions.py::test_a_write_moves_a_job_only_where_the_domain_allows`, `integration/test_list_failed_jobs.py::test_failed_jobs_are_listed_newest_first_as_get_job_shows_them` |
| `notification.send` records one notification per job, however often its handler runs, and the job's result names it. | The fake provider keeps one notification per key `notification.send:{job_id}` and returns the first one's ID. | [ADR 0007](adr/0007-idempotency.md) | `integration/test_fake_notification_queries.py::test_concurrent_calls_with_one_key_record_one_notification`, `integration/test_end_to_end.py::test_a_job_whose_attempts_all_reach_the_provider_sends_one_notification`, `integration/test_failure_combinations.py::test_a_worker_killed_after_its_notification_is_recovered_and_sends_no_second_one` |
| A submission retried with the same `Idempotency-Key` and body saves one job. | The key is unique in the jobs table; a replay returns the first job. | [ADR 0007](adr/0007-idempotency.md) | `integration/test_submit_job_idempotency.py::test_a_retry_with_the_same_key_returns_the_first_job`, `integration/test_submit_job_idempotency.py::test_concurrent_requests_with_one_key_save_one_job` |
| A message the worker cannot read goes straight to the dead-letter queue with its body unchanged; a message that keeps hitting a bug goes there on its 21st delivery. Neither stops a worker. | An unreadable message is rejected without requeue. An unexpected error requeues with `reject`, which counts towards the jobs queue's delivery limit (20, RabbitMQ's default, set explicitly on the queue). RabbitMQ adds its `x-death` headers. | [ADR 0004](adr/0004-dispatch-and-consumption.md), [ADR 0006](adr/0006-dead-letters.md) | `integration/test_dead_letters.py::test_a_message_rejected_without_requeue_is_dead_lettered_unchanged`, `integration/test_worker.py::test_a_message_that_keeps_hitting_a_bug_is_dead_lettered_past_the_limit`, `integration/test_failure_combinations.py::test_poison_messages_mixed_with_valid_ones_leave_two_workers_running` |

## Not guaranteed

- **Exactly-once execution.** Delivery is at least once, and so is the handler. A handler runs
  again after a crash, a stop past `SHUTDOWN_GRACE_S` or by a second signal, or a final update
  the database could not take. The handler timeout is shorter than the lease, so a handler that
  honours cancellation stops before its lease expires; a handler that blocks the event loop or
  ignores cancellation, or a paused worker, can still be running when the next attempt starts. A
  test simulates that takeover: the stale attempt keeps its side effect and loses its outcome
  (`integration/test_execution.py::test_a_stale_attempt_keeps_its_side_effect_but_not_its_outcome`).
- **Side effects beyond the fake provider.** Only `notification.send` is safe to repeat, because
  its receiver takes a key per job. A new handler whose receiver takes no key needs a guard of its
  own (ADR 0007).
- **That a failed job had no effect.** A worker killed after its notification and before its
  final update leaves a job that fails with `lease_expired` on its last attempt, though its
  notification was sent
  (`integration/test_failure_combinations.py::test_a_worker_killed_after_its_notification_is_recovered_and_sends_no_second_one[last-attempt]`).
- **That every job ends.** A job whose claim the database rejects for its own data (SQLSTATE
  class 22 or 23), or whose message is dead-lettered before every claim, stays `queued` and is
  published again once per `LOST_MESSAGE_AFTER_S`, with no end. Under a backlog that never
  drains, the lost-message sweep never runs.
- **Order.** A poll publishes the due jobs with the earliest `available_at` first
  (`integration/test_dispatch.py::test_one_poll_publishes_at_most_the_batch_size_oldest_first`),
  but several workers, retries and recovery run jobs in any order.
- **A submission without an `Idempotency-Key` saved once.** If the connection drops or
  `DB_COMMAND_TIMEOUT_S` expires while PostgreSQL commits, the client gets `503` although the job
  may be saved; a client that never received its `202` is in the same place. A retry without a
  key saves a second job.
- **That a crash spares the attempt budget.** A worker crash, or a stop that cuts a job short,
  uses up an attempt, even when the job was not at fault.
- **Times.** No step has a deadline. The bounds below hold only while the processes and their
  dependencies are up and keeping pace; nothing has been measured:
  - publish after submit: up to the rest of the poll in progress, `DISPATCH_POLL_INTERVAL_S`
    (1 s) and one more poll, while fewer than `DISPATCH_BATCH_SIZE` (100) jobs are due; a backlog
    of N batches waits about N intervals;
  - a crashed worker's job requeued or failed: up to `LEASE_DURATION_S` after its claim, plus
    `RECOVERY_INTERVAL_S` and one recovery pass (120 s + 30 s), while fewer than
    `DISPATCH_BATCH_SIZE` leases expire per pass; then published by the next poll;
  - a lost message published again: about `LOST_MESSAGE_AFTER_S` + `RECOVERY_INTERVAL_S`
    (600 s + 30 s) after its mark, once a pass finds no ready messages, one batch per pass; then
    published by the next poll;
  - a handler cancelled: after `HANDLER_TIMEOUT_S` (60 s); a handler that blocks the event loop
    or ignores cancellation is not stopped.
- **Delivery of results.** Nothing calls the client back; it reads `GET /jobs/{id}`.
- **Anything about the dead-letter queue's contents over time.** Nothing consumes or expires it.
