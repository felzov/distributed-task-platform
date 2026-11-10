# ADR 0008 — Recovery and shutdown

- **Status:** Accepted
- **Date:** 2026-11-10

## Context

Two gaps can still leave a job waiting for good, and nothing reads the columns meant to close
them. A third, stopping a process, makes the first one more likely:

- **A worker dies while it holds a job.** The job stays `running` with `lease_expires_at` in the
  past, and its attempt row stays open (ADR 0005). ADR 0001 decision 8 says a recovery moves such
  jobs on; it does not exist yet.
- **A job's message is gone before any claim.** The job stays `queued` with `dispatched_at` set,
  so the dispatcher never publishes it again (ADR 0002 decision 7). This happens when its claim is
  rejected (SQLSTATE class 22 or 23, ADR 0004), when its message is dead-lettered before a claim
  (ADR 0006), or when the broker loses it.
- **A process is stopped.** Neither process handles signals today. SIGINT (Ctrl-C) cancels it
  through `asyncio.run`; SIGTERM ends it at once through Python's default action, without cleanup.
  Either way, a dispatcher stopped between the broker's confirms and its COMMIT publishes the same
  jobs again on the next start (harmless, ADR 0002 decision 5), and a worker stopped while it runs
  a handler leaves its job `running` until the lease recovery above exists.

The broker can also be unavailable. The API never touches it (ADR 0002), so a broker outage cannot
produce a false 202. A running worker survives one through aio-pika's robust connection (ADR 0004,
an 87 s outage), and so does the dispatcher's publisher (the 7 s cut below). But a process that
starts while the broker is down stops: at once when the port is closed, after
`broker_connect_timeout_s` when nothing answers. The worker also stops when its queue iterator
ends (`consume_forever` before phase 8). Two smaller gaps deferred to phase 8 belong here as well:
the dispatcher and the worker do not check the schema revision at start, as `/ready` does, and
nothing configures `task_platform`'s loggers (uvicorn configures only its own), so INFO lines are
not shown; WARNING and above reach stderr through Python's last-resort handler.

The behaviour below was observed on RabbitMQ 4.3.7 with aio-pika 10.2.1 (aiormq 7.2.4) on
2026-11-10, unless marked as read in aio-pika's source. The spike used quorum queues of its own and
a TCP proxy between the client and RabbitMQ that could drop every open connection and close new
ones as soon as it accepted them.

- **Stopping a consumer while it holds a message.** Closing aio-pika's queue iterator sends
  `basic.cancel`. The message the worker already holds can still be acked afterwards, and the loop
  then ends. This holds when another task closes the iterator while the handler is still running.
  With prefetch 1 nothing else is buffered. aio-pika nacks any buffered message with requeue on
  close (source), which does not count towards the delivery limit (ADR 0004 decision 7).
- **Closing the connection with the message unacked** returns it to the queue: redelivered, with
  `x-delivery-count` 1.
- **Reading a queue's ready count.** On a robust channel, `declare_queue(name, passive=True)`
  returns the cached queue object, with its old declare result, once the name was declared on that
  channel, even with `robust=False` (the cache check in `RobustChannel.declare_queue` comes first,
  source). It kept reporting 0 after two messages were published. On a channel that never declared
  the name, a passive declare with `robust=False` read the live count each time: 3, then 2 with
  one message held unacked. Declare-Ok's `message_count` counts ready messages only. A passive
  declare of a missing queue fails with `ChannelNotFoundEntity` and closes that channel.
- **A cut of 7 s.** A publish during the cut raised `AMQPConnectionError` at once, and that
  message was never delivered. The connection reconnected 2.5 s after the proxy accepted
  connections again, a publish with confirms then succeeded, and the same iterator consumed again.
- **A cut of 75 s.** The robust iterator did not end; it consumed again after the restore. Its
  source still ends it when the process closes the connection, and in one path when a channel is
  not restored within 60 s; no outage in the spike reached that path.
- **First connection.** A closed port, a connection closed by the proxy, and a wrong password all
  raised `AMQPConnectionError`, a subclass of `ConnectionError` and so of `OSError`. A wrong
  password raised its subclass `ProbableAuthenticationError`. An unknown vhost raised
  `InvalidFrameError`, which is not an `OSError`. Each failed within 10 ms.
- **Cancelling a first connection from outside**, against a server that accepts TCP and never
  answers, left `RobustConnection.__connection_factory` running, and it survived one more cancel.
  This matches what phase 3 observed on 2026-10-29. The connection's own `timeout=` ended the same
  attempt with `TimeoutError` and left no task behind.

## Decision

1. **Lease recovery.** A sweep takes up to `dispatch_batch_size` `running` jobs with
   `lease_expires_at < now()`, with `FOR UPDATE SKIP LOCKED`, and recovers that batch in one
   transaction. Each update matches `status = 'running'`, the attempt number read and the expired
   lease again:
   - With attempts left (`attempts < max_attempts`), the job goes back to `queued`, due now:
     `available_at = now()`, `dispatched_at = NULL`, the lease cleared,
     `error_code = 'lease_expired'`. There is no backoff: the lease already ran its full length
     since the claim.
   - Without attempts left, the job goes to `failed` with `lease_expired`.
   - In the same transaction, the open attempt row numbered `jobs.attempts` is closed as `failed`
     with `lease_expired`. A missing row is logged as an ERROR and does not stop the recovery, as
     for a worker's finish (ADR 0004, update 2026-11-07).
   - The domain validates both transitions, and `FailureCode.LEASE_EXPIRED` gets a fixed message.
     It is listed as retryable, which matches the requeue above: like a timeout, the next attempt
     may not fail the same way.
   - This is an exception to ADR 0005: decision 1 retries only the handler's own failures, and
     decision 3 waits a backoff after every failed attempt. An expired lease is retried, and
     without backoff. Phase 8 records it in a dated update to ADR 0005.
   - Until the sweep runs, a worker whose lease expired can still finish normally. After it, that
     worker is fenced out: its finish matches on `status = 'running'` and its own attempt number
     (ADR 0001 decision 7), so it writes neither the job nor the attempt rows, also after the next
     claim.
2. **Lost-message recovery** (ADR 0002 decision 7). A second sweep clears `dispatched_at` on up
   to `dispatch_batch_size` `queued` jobs dispatched more than `lost_message_after_s` ago (600 s
   by default, a placeholder: nothing was measured), with `FOR UPDATE SKIP LOCKED` and an update
   that checks both conditions again. The dispatcher then publishes them again as due jobs. The
   sweep runs only when the jobs queue has **no ready messages**, read before its transaction:
   - The count comes from a passive declare with `robust=False` on a channel the recovery loop
     keeps for this alone and on which nothing else declares the queue, so aio-pika's cache never
     answers it. A missing queue closes that channel; the loop opens a new one on the next pass.
   - An empty queue shows that, when it was read, no copy of such a message was waiting, so
     recovery does not republish a message that is only delayed in a backlog. This settles ADR
     0002's retry-storm question.
   - Unacked messages are not counted. A message held by a worker is normally claimed within
     moments, and its job is then `running`, not `queued`; the exceptions are in the consequences.
   - The read, including opening its channel, is bounded by `publish_timeout_s`. A robust channel
     first waits for its connection to be restored (source), so without a bound one read during a
     broker outage would hold the recovery loop, and the lease sweep with it, until the broker is
     back. A timeout counts as a count not read, and a signal can end the wait. Not observed in
     the spike.
   - If the count cannot be read, the sweep is skipped and logged; it runs again on the next pass.
3. **Where recovery runs:** in the dispatcher process, as a second loop next to the poll loop,
   every `recovery_interval_s` (30 s by default). The lease sweep runs before the lost-message
   sweep, and also while the broker is unreachable, since it needs only the database. A failed
   sweep is logged by class and SQLSTATE and tried again on the next pass, like a failed poll.
   `SKIP LOCKED` lets several dispatchers sweep without recovering a job twice.
4. **Two partial indexes** (migration 0007), so the sweeps do not read every job ever saved:
   `(lease_expires_at) WHERE status = 'running'` and
   `(dispatched_at) WHERE status = 'queued' AND dispatched_at IS NOT NULL`. As in phase 6, the
   queries write their status literals into the SQL, so a generic plan can still use the indexes.
5. **Worker shutdown.** On SIGTERM or SIGINT the worker closes its queue iterator, so no new
   message arrives, and lets the message it holds finish: handler, final update, ack. It waits up
   to `shutdown_grace_s` (30 s by default), then exits with status 0.
   - Without a held message, it cancels its consume task at once: an iterator waiting for its
     channel to be restored is not woken by a close (source).
   - Past the grace period, or on a second signal, it cancels the handler, closes the connection
     and exits with status 0. The message goes back to the queue. Unless the final update had
     already committed, the job stays `running` and lease recovery takes it on: the attempt is
     used up, as for a crash. Further signals are ignored.
   - The grace period is shorter than `handler_timeout_s` (60 s) on purpose: a stop should not
     wait for the slowest handler the timeout allows. A handler with more time left than the
     grace period is cancelled.
6. **Dispatcher shutdown.** On SIGTERM or SIGINT the dispatcher lets the current poll and the
   current sweep's batch end with their COMMIT or ROLLBACK, starts no new ones, and exits with
   status 0. A second signal cancels at once; further signals are ignored. The open transaction is
   then rolled back, and the next start publishes those jobs again (harmless, ADR 0002 decision 5).
7. **Start-up.** Both processes wait for their dependencies, but stop on configuration errors:
   - **Schema check first:** `current_revisions` against `SCHEMA_REVISION`, as `/ready` does. A
     database at another revision, or never migrated, is logged as an ERROR naming the expected
     and the found revisions, and the process exits with status 1: migrations run before the
     processes start. A wrong password or an unknown database (SQLSTATE class 28, `3D000`) also
     exit with status 1. Any other connection failure is retried.
   - **Then the broker:** a failed first connection is retried if it is an `OSError` (including
     `TimeoutError`), except `ProbableAuthenticationError`. An unreachable host is retried; wrong
     credentials and an unknown vhost (observed), or a malformed URL (not tested), stop the
     process. Each attempt is bounded by the connection's own `broker_connect_timeout_s`.
   - Every retry logs a WARNING and waits `startup_retry_interval_s`. During start-up a signal
     ends that wait, but never cancels a schema check or a connection attempt in progress: each
     runs to its own timeout (`ready_check_timeout_s`, `broker_connect_timeout_s`), so no
     reconnect task is left behind. A process stopped during start-up exits within one of those
     timeouts.
8. **The worker opens a new consumer when its iterator ends**, unless it is shutting down. It
   logs a WARNING, waits `startup_retry_interval_s`, closes the old channel and opens a new one
   with the queue. Reusing the old channel could leave the old consumer in place: aio-pika's
   source restores consumers it still has on record when the channel comes back, with the old
   iterator's callback. No outage in the spike ended the iterator, so this rests on the source.
   It is tested with a fake queue.
9. **Logging.** The dispatcher and the worker configure logging at start: timestamp, level,
   logger name and message on stderr, at `LOG_LEVEL` (INFO by default). `sqlalchemy.engine` is
   held at WARNING whatever the level: at INFO it logs every statement (its bound parameters are
   hidden, `hide_parameters=True`), and at DEBUG it also logs result rows, which can include
   payloads. The API keeps uvicorn's logging.
10. **Outage tests run through an in-process TCP proxy** that a test can cut and restore. It is a
    test helper, not a dependency, and it never stops the shared PostgreSQL or RabbitMQ containers.

## Consequences

- A job whose worker crashes, or is stopped past its grace period, is back in `queued` at most
  `lease_duration_s + recovery_interval_s` (150 s by default) after its claim, if the sweep's batch
  reaches it, or fails once its attempts are used up. It is published on the next poll; when it
  runs depends on free workers. Its handler then runs more than once. That is safe for
  `notification.send` since phase 7 (ADR 0007); every new side-effecting handler needs the same
  care.
- A job stuck in `queued` with a lost message is published again after
  `lost_message_after_s + recovery_interval_s`, but only once the jobs queue has no ready messages.
  Under a backlog that never drains, lost messages are never recovered. The backlog is the problem
  to fix first.
- A message that dies before any claim, every time (a claim rejected for bad data, or a message
  dead-lettered by a bug), is published again once per threshold and interval, whenever the queue
  is empty, indefinitely, with an ERROR logged each time. In the dead-lettered case each round
  adds a copy to the dead-letter queue, which has no limit (ADR 0006). A republish counter would
  end this; it is not built.
- The ready count cannot see a message a worker holds unacked. Normally that is one claim. During
  a database outage a worker holds its message for one claim backoff (`worker_db_error_backoff_s`,
  5 s) and nacks it back, so a sweep can clear the job of a message that is only delayed: at most
  one extra copy per held message per pass, and the next pass sees the nacked copy as ready. A
  copy that arrives after its job was claimed loses its claim and is dropped (ADR 0001 decision 7).
- A stop past `shutdown_grace_s` uses up an attempt. Its message comes back with
  `x-delivery-count` raised, but its job is then `running`, so that delivery loses its claim and
  is acked: the count does not build up. A process manager must allow more than the grace period
  between SIGTERM and SIGKILL. Docker's default of 10 s (documentation) is less than the 30 s
  default, so `docker stop` needs `-t`.
- A message in the dead-letter queue may belong to a job that was recovered and has finished
  since. A dead letter no longer means its job is stuck (ADR 0006 decision 6).
- The dispatcher process now has three duties: polling, lease recovery and lost-message recovery.
  Stopping it stops all three; jobs wait in PostgreSQL until it is back.
- Lease recovery adds one more transition writer besides the worker. Its update is guarded by
  status, attempt number and the expired lease, under the row lock taken by the sweep, and joins
  the transition matrix test (ADR 0001's phase 4 update).
- Other broker errors that are `OSError`s, such as TLS or protocol errors at the first
  connection, are retried as outages. Only the cases named in decision 7 stop the process.
- The proxy simulates TCP cuts and connections closed on accept, not a broker that is slow or
  answers wrongly. These are the outages tested.
- No recovery times or throughput figures are claimed beyond the configured values; none were
  measured.

## Alternatives considered

- **Recovery in a separate process, or in every worker.** A separate process is one more thing to
  run and stop cleanly. Running it in every worker multiplies the sweeps by the number of workers
  for no gain, since `SKIP LOCKED` already allows several sweepers. Rejected; the dispatcher
  already owns the polling of the jobs table.
- **Recovery from the broker** (a consumer on the dead-letter queue, or RabbitMQ's
  `consumer_timeout`). It lets a broker event decide job state (ADR 0002, ADR 0006), and it misses
  jobs whose message is already gone, such as one acked after a claim whose result was lost. The
  lease sees every crash. Rejected.
- **Backoff before an expired lease is retried.** The lease has already run its full length, so
  more waiting only delays recovery. Rejected.
- **Releasing the job at shutdown without using up its attempt** (a fenced `running → queued`
  that undoes the increment). Attempts would no longer only go up, and the open attempt row would
  need an outcome that is neither success nor failure. Rejected; a stop within the grace period
  lets the attempt finish, so only a stop past it wastes one.
- **A grace period longer than `handler_timeout_s`**, checked when settings load. No handler would
  be cut short, but a stop could take over a minute, more than six times Docker's default. Rejected
  for a shorter grace period and a wasted attempt in the rare long case.
- **Lost-message recovery on a threshold alone.** It republishes every delayed message of a
  backlog older than the threshold, on every pass: the retry storm in ADR 0002. Rejected.
- **A republish counter per job**, failing the job after N republishes. It ends the indefinite
  republish of a message that dies before its claim, but adds a column and a new failure path for
  a case logged as an ERROR every time. Deferred.
- **A per-message TTL** that dead-letters old messages. It turns "lost" into "dead-lettered" but
  still needs the job-side recovery, and adds a second clock. Rejected.
- **Re-declaring the jobs queue to read its count.** It reads the live count (observed), but it is
  a full declaration: every pass would recreate a deleted jobs queue without its dead-letter queue
  first (ADR 0006 decision 2), where today the queues are declared only at start. Rejected for a
  passive declare on a channel of its own.
- **Stopping at start when a dependency is down** (today's behaviour), and relying on a process
  manager to restart. The setup has no process manager (`make dispatcher`, `make worker`), so the
  processes would simply be gone. Rejected. Configuration errors still stop the process.
- **A proxy tool such as Toxiproxy, or stopping the containers in tests.** Toxiproxy is a new
  dependency and service. Stopping the shared containers would break every other test running
  against them. Rejected for a small in-process proxy.
