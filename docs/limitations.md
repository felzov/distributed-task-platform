# Known limitations

What this project does not do, or does only partly, grouped by theme. Many are deliberate for an
MVP that runs on one machine. The README lists the most important ones; the guarantees they
qualify are in [delivery-guarantees.md](delivery-guarantees.md).

## Security and data exposure

- **No authentication.** Anyone who can reach the API can submit jobs, which means triggering
  notifications, and anyone who knows a job ID can read that job's status. `GET /jobs?status=failed`
  also lets anyone find the IDs, codes and times of failed jobs without knowing an ID first (never
  their payloads). Run it only on a trusted network. The same goes for `/ready`: it tells anyone
  who can reach it whether the database is at this code's migration revision.
- **Idempotency keys are global.** Without authentication there is one key space for everyone:
  anyone who guesses a key and sends the same body gets that job's ID, a different body gets 422
  (which shows the key is taken), and someone who learns a key before its owner uses it can take
  it first. Random keys make guessing impractical.
- Database and connection errors are logged as their class, SQLSTATE and constraint name only,
  without a traceback, because PostgreSQL's error detail can quote the stored payload. Any
  `OSError` is treated as "database unavailable" (503). Other unexpected errors return 500 and
  uvicorn logs their full traceback for debugging, so a bug whose traceback holds request data
  could still write it to the log.
- The worker's claim reads the job's payload from PostgreSQL. The dispatcher's and the worker's
  logging holds SQLAlchemy's `sqlalchemy.engine` logger at WARNING, since at DEBUG it logs result
  rows, payloads included (`hide_parameters` hides only query parameters); a test runs both
  processes at `LOG_LEVEL=DEBUG` and finds neither the payload nor a password in their logs. The
  API keeps uvicorn's logging configuration, so the API's own INFO lines are not shown; its
  warnings reach stderr without a timestamp.
- uvicorn's access log records each request's raw path and query string, so job IDs, whatever a
  client sent in place of one and anything after a `?` end up in the log. Payloads never do: they
  travel only in request bodies.
- When an integration test fails, pytest's report shows the test function's arguments (and, with
  `--showlocals`, its locals). `TEST_DATABASE_URL`, `TEST_BROKER_URL` and the URLs the test
  helpers derive from them show there as `<test URL, hidden>`, and the tests keep the processes'
  output and environment out of the report. A failing comparison of a URL itself still prints
  both values in pytest's diff. With `--showlocals`, a URL a test rebuilds as a plain string, or a
  password it reads back (`get_secret_value()`, `urlsplit(url).password`), shows too. So the test
  services should use throwaway passwords (CI's are written in its workflow file).

## Delivery, retries and side effects

- A retry runs the handler again. For `notification.send` that is safe: the handler gives the
  provider the key `notification.send:{job_id}`, and the fake provider records one notification
  per key, so every attempt of a job, current or stale, gets the same `notification_id` (ADR 0007).
  This relies on the receiver deduplicating by key; a real provider usually forgets keys after a
  retention window, which must outlast a job's retries and any outage. Two different jobs with the
  same content still send twice.
- A worker whose lease expired may still finish its handler. Its result is rejected, but its side
  effect still happens; `notification.send` makes it the same notification as the current
  attempt's. A future handler whose receiver takes no key needs a guard of its own (ADR 0007).
- **A submission without an `Idempotency-Key` can be saved twice.** If the connection drops or
  `DB_COMMAND_TIMEOUT_S` expires while PostgreSQL commits, the client gets 503 although the job
  may have been saved, and the same happens to a client that never received its 202; a retry
  without a key then creates a second job. A retry with the same key gets the saved job instead.
- A worker crash uses up one of the job's attempts, even when the crash had nothing to do with the
  job (ADR 0001).
- Full jitter has no minimum wait, so a retry can be published almost at once. There is no
  circuit breaker: while a provider is down, every job uses up all its attempts.
- A claim the database rejects because of the job's own data (SQLSTATE class 22 or 23) is logged
  at ERROR and its message acked (ADR 0004). Lost-message recovery publishes the job again once
  per `LOST_MESSAGE_AFTER_S`, and the claim is rejected again each time: such a job never ends,
  and logs an ERROR every round. The same holds for a message that is dead-lettered before every
  claim, which also adds a copy to the dead-letter queue each round. There is no republish
  counter.
- An unexpected error while handling a message (a bug) requeues it with `reject`, which counts:
  past RabbitMQ's delivery limit (20) the message goes to the dead-letter queue, and a job that was
  never claimed is published again by lost-message recovery.
- When the final update fails with a database error, the message is requeued, but its redelivery
  loses the claim: the job stays `running` until lease recovery, which uses up the attempt.
- A worker stopped past its grace period, or by a second signal, leaves its claimed job `running`
  until lease recovery, and the job's notification may already have been sent, once (a later
  attempt gets the same one).

## Timing and recovery

- Dispatch latency is up to one dispatcher poll interval plus one poll (ADR 0002) while fewer than
  `DISPATCH_BATCH_SIZE` jobs are due. The dispatcher pauses after every poll, even a full one, so
  it publishes at most `DISPATCH_BATCH_SIZE` jobs per `DISPATCH_POLL_INTERVAL_S` (100 per second
  with the defaults) and a backlog of N batches waits about N intervals. A database error during
  a poll turns that poll's confirmed publishes into duplicates, which the worker's claim absorbs.
- Recovering a crashed worker's job takes up to the lease duration plus the recovery interval
  (150 s with the defaults), and its handler then runs again.
- An attempt whose worker crashed stays open (`finished_at` and `outcome` null) until lease
  recovery closes it, up to the lease duration plus the recovery interval after the claim.
- Lost-message recovery waits while the jobs queue has ready messages, so under a backlog that
  never drains, lost messages are never recovered. It cannot see messages a worker holds
  unacknowledged: during a database outage it can publish an extra copy of a message that is only
  held back, which the claim absorbs.
- A second signal abandons the dispatcher's poll in progress: its transaction rolls back, and
  publishes the broker had already confirmed become duplicates after the next start, which the
  claim absorbs.
- A second signal stops a process within a bound, not at once: about 2 s when work in hand waits
  on a database link that stopped answering (SQLAlchemy closes that connection with a 2 s timeout
  no cancel cuts), and each later close gets its own bound (5 s each by default).

## Broker and queues

- The dispatcher declares the jobs queue only when it starts. If the queue is deleted while it
  runs, the broker returns every message and the jobs stay saved, unpublished, until a process
  declares the queue again.
- The dead-letter queue grows without limit: nothing consumes or expires it, and clearing it
  (purging the queue) is an operator's decision. If it is deleted while the system runs, a message
  that dies meanwhile waits for it and shows up in neither queue's `messages` count until it is
  declared again; RabbitMQ then moves it, about 3 minutes later in the one test (ADR 0006).
- Start-up waits only for the first broker connection. If RabbitMQ goes away between that
  connection and the queue declaration, the process exits with status 1 and a traceback, and must
  be started again. A queue the broker refuses to declare (such as a `jobs` queue from before
  phase 6) also stops it with a traceback.
- A signal that arrives during the worker's subscribe (about a millisecond, at start and whenever
  it consumes again) cancels the subscribe midway: one message comes back only when the
  connection closes, counted towards the delivery limit.

## Database, schema and migrations

- Migrations 0004 and 0007 build their indexes with a plain `CREATE INDEX` inside Alembic's
  transaction, which blocks writes to `jobs` while it runs (`CONCURRENTLY` cannot run in a
  transaction). Migrations 0005 and 0006 add columns and unique constraints (0005 also CHECKs,
  which scan `jobs`), and these block reads too, of `jobs` and `fake_notifications`, until they
  commit (PostgreSQL documentation). Fine for the table sizes here, but not measured.
- The schema check runs only at start. A migration applied while the processes run is not noticed
  until they are restarted; with an older schema the worker's claim fails and it backs off, as
  for a database outage.
- `/ready` requires the database revision to equal the code's exactly. During a rolling deploy,
  instances running the old code report not ready as soon as the new migration is applied. The MVP
  runs one instance and migrates before starting it.
- No constraint ties `result` and `error_code` to the status: `GET /jobs/{id}` derives both fields
  from the status alone, and only the worker's own updates keep the columns consistent. A
  `failed` row without an error code shows `failure: null`, and a code this version does not know
  (e.g. one written by a newer worker) is shown with the message "the job failed". A `succeeded`
  row whose result is missing or not a JSON object shows `result: null`. Each case is logged as a
  warning, by job ID only.
- `fake_notifications.job_id` has no index, so looking up a job's notifications (only tests do)
  scans the table. That is fine for the fake provider; a real provider's records would need one.
- Notifications recorded before migration 0006 have no key: a job that had attempts before it and
  after it can have more than one row. A job that succeeded before phase 7 keeps its old result,
  `{"provider": "fake"}`, without a `notification_id`.
- Attempt history starts with migration 0003. Jobs that ran before it have no attempt rows, and a
  job that was running during the migration lists its attempts from the next one on.

## Logs, probes and limits

- Some library log lines are noisy or misleading. While the broker is unreachable at start, aiormq
  logs an ERROR for each try and aio-pika an INFO "Reconnecting after 5 seconds", though the
  process's own retry interval applies. When a stopping worker's job is acked before its consumer
  cancel is answered, the cancel is dropped and aiormq logs a WARNING "Closing channel … because
  RPC call Basic.Cancel cancelled" (harmless: a message delivered meanwhile goes back marked
  redelivered, not counted). After a second signal, SQLAlchemy can log "Exception closing
  connection" with a traceback of the cancelled close. When a test cut a running worker's broker
  link, aiormq logged a WARNING "Cancelling cause reader exited abnormally" with a socket traceback
  and aio-pika an `ERROR aio_pika.tools: Callback <… RobustChannel._on_close …> error:
  RuntimeError: <Connection …> closed`. Both are harmless: the robust connection reconnects about
  5 s later and restores its channel. They show the broker's user, host and port, with the
  password masked ([failure-modes.md](failure-modes.md)).
- Each failing `/ready` call logs one warning, so frequent probing during an outage produces many
  identical lines. The database line includes pool usage (e.g. `pool 5/5 in use`), but under
  traffic a slow database also fills the pool, so a full pool alone does not show the database is
  healthy.
- All limits and timeouts are placeholders, not measured values; `.env.example` lists every one
  with its default (e.g. payload 64 KiB, statement timeout 5 s, client command timeout 10 s).
