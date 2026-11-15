# Failure modes

What the system does when something goes wrong, and which tests show it. Each section covers one
failure path from the project's scope: duplicate delivery, crashes, retry exhaustion, malformed
input, broker and database outages, and stuck jobs. Test names are given as `file::test` under
`tests/`. Tests marked *process* run the dispatcher and the workers together as real
`python -m task_platform.<process>` children. A few others start one such child to check how it
exits; most run the same code inside the test process.

The rules all of this rests on are in the ADRs:

- PostgreSQL holds the job's state, and RabbitMQ only carries job IDs (ADR 0001, ADR 0002).
- Delivery is at least once. A job runs only after a conditional claim succeeds (ADR 0001,
  ADR 0004).
- The one side effect, the fake notification, is keyed by job, so a repeat sends nothing new
  (ADR 0007).
- Expired leases and lost messages are recovered (ADR 0008).

## Duplicate delivery

| Failure | What happens | Tests |
|---|---|---|
| The same job's message arrives twice, at one worker or at two | The first claim moves the job to `running`. The second finds it no longer `queued`, logs "not claimed (duplicate or stale delivery)" and is acked. The handler runs once. | `integration/test_worker.py::test_two_workers_given_one_job_twice_run_it_once_and_ack_both`, `integration/test_execution.py::test_two_deliveries_of_one_job_run_its_handler_once`, *process* `integration/test_failure_combinations.py::test_poison_messages_mixed_with_valid_ones_leave_two_workers_running` |
| A message arrives for a job that already finished (stale) | The claim is refused and the message acked. | *process* `integration/test_failure_combinations.py::test_an_ack_lost_on_a_broker_cut_brings_the_message_back_and_the_claim_refuses_it` |
| An attempt that lost its job (lease recovered, newer attempt) tries to retry or finish it | Its update is fenced by the job's status and attempt number and changes nothing. | `integration/test_execution.py::test_a_stale_attempts_retry_is_discarded`, `integration/test_execution.py::test_a_stale_attempt_keeps_its_side_effect_but_not_its_outcome`, `integration/test_recovery.py::test_a_worker_finishing_after_the_sweep_writes_nothing` |
| The handler runs again for a job whose side effect already happened (retry, recovery) | The fake provider keeps one notification per key `notification.send:{job_id}` and returns the first one's ID. The job's result names that notification. | `integration/test_execution.py::test_a_retry_after_the_provider_was_called_sends_nothing_new`, `integration/test_execution.py::test_a_newer_attempt_after_a_stale_one_gets_the_same_notification`, `integration/test_end_to_end.py::test_a_job_whose_attempts_all_reach_the_provider_sends_one_notification`, `integration/test_fake_notification_queries.py::test_concurrent_calls_with_one_key_record_one_notification` |
| A client repeats `POST /jobs` with the same `Idempotency-Key` | The first job is returned (200) and no second job is created. A different body under a used key gets 422 `idempotency_key_reused`. | `integration/test_submit_job_idempotency.py::test_a_retry_with_the_same_key_returns_the_first_job`, `integration/test_submit_job_idempotency.py::test_concurrent_requests_with_one_key_save_one_job`, `integration/test_submit_job_idempotency.py::test_a_different_request_under_a_used_key_is_refused` |

## Crashes

| Failure | What happens | Tests |
|---|---|---|
| A worker is killed after its notification committed and before its final update | The final update never commits, so the job stays `running` until its lease expires. The broker gives the killed worker's message back as soon as its connection closes, before any recovery. Lease recovery then closes attempt 1 as `failed`/`lease_expired` and requeues the job, which is published again, so the job now has two messages. The next attempt sends no second notification and reports the first one as its result. The extra message is refused by the claim. | *process* `integration/test_failure_combinations.py::test_a_worker_killed_after_its_notification_is_recovered_and_sends_no_second_one[attempts-left]`; in-process cancel: `integration/test_end_to_end.py::test_a_job_whose_worker_crashed_is_recovered_and_succeeds_with_one_notification` |
| The same crash on the job's **last** attempt | Lease recovery fails the job: `failed`/`lease_expired`, listed by `GET /jobs?status=failed`. **Its notification was sent all the same.** A failed job does not prove that nothing happened: a crash after the side effect and before the final update looks like any other expired lease. | *process* `integration/test_failure_combinations.py::test_a_worker_killed_after_its_notification_is_recovered_and_sends_no_second_one[last-attempt]`, `integration/test_recovery.py::test_a_job_on_its_last_attempt_fails_and_is_listed_as_failed` |
| A worker is stopped (SIGTERM/SIGINT) while running a job | It stops consuming and lets the job finish within `shutdown_grace_s` (30 s by default). After that, or on a second signal, the handler is cancelled and its message goes back to the queue. The job's lease expires and recovery takes over. The process exits 0. | `integration/test_worker_shutdown.py::test_a_stop_signal_lets_the_job_in_hand_finish_and_takes_no_new_message`, `integration/test_worker_shutdown.py::test_a_job_not_finished_in_time_is_cancelled_and_left_for_recovery`, `integration/test_worker_shutdown.py::test_the_worker_process_exits_with_status_0_on_a_signal` |
| The dispatcher is killed after the broker confirmed a publish and before its transaction committed | The poll's transaction rolls back, so the job is still `queued` with `dispatched_at` NULL. The next poll (by the restarted dispatcher, or another) publishes it again. One job then has two messages. The first claim runs it, and the second message is refused and acked. | *process* `integration/test_failure_combinations.py::test_a_dispatcher_killed_before_its_commit_publishes_again_and_the_job_runs_once` |
| The dispatcher is stopped while polling | The poll in hand commits and no new one starts. A second signal cancels the poll and rolls it back, so its jobs are published again later. | `integration/test_dispatcher.py::test_a_stop_signal_lets_the_poll_in_hand_commit_and_starts_no_other`, `integration/test_dispatcher.py::test_a_second_signal_cancels_the_poll_in_hand_and_rolls_it_back` |
| Two dispatchers poll at once | `SELECT … FOR UPDATE SKIP LOCKED`: each due job goes to one poll. | `integration/test_dispatch.py::test_two_dispatchers_never_publish_the_same_job` |

## Retry exhaustion

| Failure | What happens | Tests |
|---|---|---|
| A handler fails with a retryable code (`handler_error`, `handler_timeout`) and attempts are left | The attempt is closed as `failed` and the job is requeued after a full-jitter backoff, drawn within the job's own retry budget (ADR 0005). An expired lease (`lease_expired`) is retried too, but with no backoff: the lease already ran its full length (ADR 0008 decision 1). | `integration/test_execution.py::test_a_retryable_failure_with_attempts_left_is_retried`, `integration/test_end_to_end.py::test_a_job_that_fails_twice_is_retried_and_succeeds_on_attempt_3` |
| The same failure on the last attempt | The job is `failed` with that code and listed by `GET /jobs?status=failed`. It is **not** sent to the broker's dead-letter queue. A failed job is its database row (ADR 0006 decision 1). | `integration/test_execution.py::test_a_retryable_failure_on_the_last_attempt_fails_the_job`, `integration/test_end_to_end.py::test_a_job_that_always_fails_stops_after_its_attempts` (which also shows one attempt row per attempt), `integration/test_list_failed_jobs.py::test_failed_jobs_are_listed_newest_first_as_get_job_shows_them` |
| A message keeps hitting a bug in the worker | Each unexpected error is logged and the message is requeued with `reject`, which counts towards the queue's delivery limit (20). Past the limit the message is dead-lettered as `delivery_limit`. | `integration/test_worker.py::test_a_message_that_keeps_hitting_a_bug_is_dead_lettered_past_the_limit`, `integration/test_dead_letters.py::test_a_message_requeued_with_reject_is_dead_lettered_past_the_limit` |

## Malformed input

| Failure | What happens | Tests |
|---|---|---|
| `POST /jobs` with an invalid envelope, payload or size | 422 or 413, with the error envelope. The input is never echoed and nothing is saved. | `api/test_submit_job_validation.py::test_invalid_requests_are_rejected_without_echoing_input`, `api/test_submit_job_validation.py::test_body_over_limit_is_rejected_without_content_length`, `api/test_submit_job_validation.py::test_payload_over_limit_is_rejected_even_if_body_is_under_its_limit` |
| A stored payload no longer passes validation (written before a schema change, say) | The worker fails the job on its first attempt with `invalid_payload`. That code is not retryable, so there are no further attempts and no side effect. | `integration/test_execution.py::test_a_stored_payload_that_fails_validation_fails_the_job`, *process* `integration/test_failure_combinations.py::test_poison_messages_mixed_with_valid_ones_leave_two_workers_running` |
| A stored job of a type with no handler | `failed`/`unknown_job_type`, not retried. | `integration/test_execution.py::test_a_type_without_a_handler_fails_the_job` |
| A message the worker cannot parse: not JSON, an extra field, an unknown `message_version`, over 1024 bytes | A WARNING naming the fields and error types, never a value. The message is rejected without requeue, so it goes straight to the dead-letter queue, unchanged. A wrong-version message for a real job does not run that job. | `unit/test_messages.py::test_invalid_bodies_are_rejected`, `unit/test_messages.py::test_a_body_over_the_cap_is_rejected_before_parsing`, `unit/test_messages.py::test_errors_never_contain_body_values`, `unit/test_worker_answers.py::test_an_unparsable_message_is_rejected_without_requeue`, `integration/test_worker.py::test_unparsable_and_unknown_messages_leave_the_queue_and_the_worker_goes_on`, `integration/test_dead_letters.py::test_a_message_rejected_without_requeue_is_dead_lettered_unchanged`, *process* `integration/test_failure_combinations.py::test_poison_messages_mixed_with_valid_ones_leave_two_workers_running` |
| A message for a job ID that does not exist | WARNING "unknown job", then acked. Nothing is written. | `integration/test_execution.py::test_an_unknown_job_is_reported_and_nothing_is_written`, *process* `integration/test_failure_combinations.py::test_poison_messages_mixed_with_valid_ones_leave_two_workers_running` |

The process test mixes five of these cases with three valid jobs, with two workers consuming: a
stored invalid payload, a cut-off JSON message, a `message_version` 2 message for a real job, a
message for an unknown job, and a duplicate of a valid job's message. Each worker answers part of
the mix, each poison message is answered once, both workers keep running and exit 0, and no log
or status or attempts response holds the payload.

## Broker failure

| Failure | What happens | Tests |
|---|---|---|
| The broker is down when a job is submitted | The API never talks to the broker (ADR 0002), so it answers 202 after its COMMIT. The job stays `queued` with `dispatched_at` NULL and is published once the broker is back. Nothing reports a dispatch that did not happen. | `integration/test_outages.py::test_a_broker_outage_delays_dispatch_and_nothing_claims_it_happened` |
| A publish is not confirmed | Only jobs whose publish the broker confirmed are marked dispatched. The rest stay due for the next poll. | `integration/test_dispatch.py::test_unconfirmed_jobs_stay_due_and_the_next_poll_publishes_them`, `integration/test_dispatcher.py::test_a_job_is_marked_only_once_a_queue_took_its_message` |
| The broker link drops after a job's final update committed and before its ack arrived | The broker returns the unacked message and delivers it again once the worker's robust connection is back (aio-pika waits 5 s). The claim refuses it and it is acked: one run, one notification. | *process* `integration/test_failure_combinations.py::test_an_ack_lost_on_a_broker_cut_brings_the_message_back_and_the_claim_refuses_it` |
| The broker link drops while the dispatcher runs | The lease sweep goes on. The lost-message sweep is skipped while the queue's ready count cannot be read. | `integration/test_outages.py::test_during_a_broker_cut_the_lease_sweep_runs_and_the_lost_message_sweep_waits`, `integration/test_dispatcher.py::test_a_cut_broker_skips_the_lost_message_sweep_and_leases_are_still_recovered` |
| The broker is down when a process starts | The process waits and retries. A refused login or vhost raises `StartupError`, and the process exits 1. A signal while waiting stops it with exit 0. | `integration/test_startup.py::test_the_broker_connection_waits_for_the_broker_to_come_back`, `integration/test_startup.py::test_a_broker_refusing_the_connection_stops_without_a_retry`, `integration/test_startup.py::test_a_process_waiting_for_the_broker_stops_on_a_signal_with_status_0` |
| A link that stops answering without closing (frozen) | A second signal still stops the process, within its connection and cleanup timeouts. | `integration/test_outages.py::test_a_second_signal_stops_an_idle_process_whose_link_froze` |

When the ack-lost test cuts a running worker's broker link, the worker logs two harmless library
records (aio-pika 10.2.1, aiormq 7.2.4):

- aiormq's WARNING "Cancelling cause reader exited abnormally", with the socket's traceback;
- aio-pika's `ERROR aio_pika.tools: Callback <… RobustChannel._on_close …> error: RuntimeError:
  <Connection …> closed`.

The robust connection then reconnects about 5 s later and restores its channel. Both lines show
the broker's user, host and port, with the password masked. The ack-lost test allows only
records of these two kinds and fails on any other ERROR.

## Database failure

| Failure | What happens | Tests |
|---|---|---|
| The database is slow or drops the connection at `POST /jobs` | 503 `service_unavailable`, with no job ID handed out. The pool recovers. | `integration/test_submit_job_failures.py::test_statement_timeout_returns_503`, `integration/test_submit_job_failures.py::test_command_timeout_returns_503_and_the_pool_recovers`, `integration/test_submit_job_failures.py::test_dropped_connection_returns_503_and_the_next_request_reconnects` |
| The database is down when a worker claims | The worker waits `worker_db_error_backoff_s`, then nacks. The message is requeued, not counted towards the delivery limit. | `integration/test_worker.py::test_a_database_outage_keeps_the_message_without_counting_it` |
| The final update cannot be written | The message is nacked and the job stays `running` until lease recovery. | `integration/test_execution.py::test_a_final_update_the_database_cannot_take_leaves_the_job_running`, `unit/test_worker_answers.py::test_each_outcome_gets_its_answer_at_once` |
| The database is down when a process starts | The process waits and retries. A wrong password, an unknown database, a missing privilege or a schema at another revision raises `StartupError`, and the process exits 1. | `integration/test_startup.py::test_the_schema_check_waits_for_the_database_to_come_back`, `integration/test_startup.py::test_a_database_refusing_the_connection_stops_without_a_retry`, `integration/test_startup.py::test_a_database_error_no_retry_fixes_stops`, `integration/test_startup.py::test_a_process_with_a_wrong_database_password_exits_with_status_1`, `integration/test_startup.py::test_a_database_at_another_revision_stops_and_names_both` |

## Stale and stuck jobs

| Failure | What happens | Tests |
|---|---|---|
| A job stays `running` past its lease (crashed or stuck worker) | The dispatcher's lease sweep requeues it, or fails it on its last attempt, as `lease_expired`. The attempt row is closed in the same transaction. | `integration/test_recovery.py::test_a_job_with_attempts_left_is_requeued_and_its_attempt_closed`, `integration/test_recovery.py::test_concurrent_sweeps_share_the_jobs_and_recover_each_once` |
| A dispatched job's message is gone from the broker | After `lost_message_after_s`, if the jobs queue holds no ready message, the lost-message sweep clears `dispatched_at` and the job is published again. If the queue's ready count cannot be read, nothing is cleared. | `integration/test_lost_messages.py::test_an_overdue_job_with_an_empty_queue_is_published_again`, `integration/test_lost_messages.py::test_ready_messages_in_the_queue_skip_the_sweep`, `integration/test_lost_messages.py::test_a_count_not_read_skips_the_sweep`, `integration/test_dispatcher.py::test_the_dispatcher_publishes_a_lost_message_again` |

## Limits of these tests

- The crash tests place each kill exactly, with a test-only trigger that holds the process inside
  one `UPDATE` (`integration/faults.py`, tested in `integration/test_faults.py`). They show one
  interleaving per scenario. They are not random fault injection, and they say nothing about load.
- A child process is killed when its test ends, even if the test fails. If pytest itself is
  killed (SIGKILL, a closed terminal), its children keep running and consuming from that test's
  queue and holding database connections. Stop them by hand before the next run. They show up as
  `python -m task_platform.worker` and `python -m task_platform.dispatcher`, like a developer's
  own processes, so check which ones they are before stopping them.
- The ack-lost test assumes the worker sends nothing but its ack between the final update and the
  cut. aiormq sends a heartbeat every half of the heartbeat timeout: every 30 s with the default
  60 s. A much shorter `BROKER_HEARTBEAT_S` in a developer's environment could make the test wait
  for the wrong chunk.
