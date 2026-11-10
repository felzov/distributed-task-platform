# distributed-task-platform

A background-job system built to make failure behaviour explicit: an HTTP API saves jobs in
PostgreSQL, a dispatcher publishes them to RabbitMQ, and separate workers run them with bounded
retries, so slow work stays out of the request path and every failure is recorded.

> **Status: early development (phase 7 of 10).** So far there are the domain rules, a local
> PostgreSQL with migrations for the `jobs` table (including the lease, error and result columns
> the worker writes), the `job_attempts` table and the fake provider's `fake_notifications` table,
> `POST /jobs` (validates and saves a job, safe to retry with an `Idempotency-Key`; 503 when the
> database is unavailable), `GET /jobs/{id}`
> (a job's state, result or failure, never its payload), `GET /jobs/{id}/attempts` (each recorded
> attempt and how it ended), `GET /jobs?status=failed` (failed jobs, newest first), the `/health`
> and `/ready` probes, a local RabbitMQ with the connection, queue, dead-letter queue and
> confirmed-publish code, the dispatcher process that publishes saved jobs
> to it, the worker process that claims each job, runs it against a fake notification provider
> (one notification per job, however often its handler runs), records the result and retries the
> handler's own failures with backoff, and the development checks (see Development setup).
> Architecture says which parts of the design run today; MVP scope and the roadmap describe the
> planned design; Known limitations covers both.

## Architecture

```mermaid
flowchart LR
    Client -->|POST /jobs, GET /jobs/id| API[FastAPI API]
    API -->|insert job, read status| DB[(PostgreSQL<br/>source of truth)]
    Dispatcher -->|poll due jobs<br/>SKIP LOCKED| DB
    Dispatcher -->|publish job_id| MQ[RabbitMQ]
    MQ -->|deliver job_id| Worker
    Worker -->|claim, record result| DB
```

The diagram and the bullets describe the design; the last bullet says what runs today.

- The API only writes to PostgreSQL. A `202` response means the job is **saved**, not that it was
  published ([ADR 0002](docs/adr/0002-dual-write-strategy.md)).
- Messages carry only the job ID. Workers read the job from PostgreSQL.
- Delivery is at-least-once. Workers claim a job with an atomic conditional update, so a duplicate
  message cannot claim a job that is already claimed. Each claim holds a lease; jobs of crashed
  workers are recovered when the lease expires, and a stale worker's late result is rejected
  ([ADR 0001](docs/adr/0001-job-lifecycle.md)).
- **Running today (phase 7):** the API, the dispatcher and the worker run as separate processes
  (`make run`, `make dispatcher`, `make worker`), with the claim, the lease column, the fenced
  final update, retries with exponential backoff and jitter, a row per attempt, a dead-letter
  queue for messages the worker can never process, a list of failed jobs, idempotency keys for
  submissions, and one notification per job however often its handler runs. Integration tests
  take a job from `POST /jobs` through the dispatcher and a worker to `succeeded`, once at the
  first attempt, once after two failed attempts, and once after three attempts that each reached
  the provider (one notification), and a job that always fails to `failed` after its last attempt,
  where the failed-jobs list shows it. **Not yet:** recovery of expired leases
  and lost messages (phase 8).

## MVP scope

- Submit a job of one type, `notification.send`, over HTTP. Its payload is typed, versioned and
  size-limited. The notification goes to a **fake provider** that records it in the database instead
  of sending anything, so tests can see each notification and which attempt sent it.
- Save the job durably and return its ID.
- Dispatch it through RabbitMQ and run it in a separate worker process.
- Retry failures with bounded attempts and exponential backoff with jitter.
- Move jobs that exhaust their attempts to `failed`, with a dead-letter path for inspection.
- Read a job's status, attempts, safe error and result over HTTP.
- Recover from worker crashes, broker outages and stuck jobs, with tests for each failure path.

## Non-goals (MVP)

- Authentication and multiple tenants (see Known limitations).
- Job cancellation, priorities, cron or scheduled jobs, and workflows or job chains.
- More than one task type.
- Large results. Results are small JSON values stored with the job.
- A UI or dashboard.
- A second broker or queue technology.
- Performance claims. Nothing has been benchmarked.

## Roadmap

The ADRs refer to these phases by number.

1. Job contracts, statuses and architecture decisions *(done)*
2. HTTP API and durable job records *(done)*
3. Dispatcher, RabbitMQ and the first worker *(done)*
4. Status and result endpoints *(done)*
5. Retries with exponential backoff and jitter *(done)*
6. Dead letters and failure inspection *(done)*
7. Idempotency for submission and side effects *(done)*
8. Graceful shutdown, stale-job and lost-message recovery, broker outages
9. Failure-path test suite
10. Documentation of delivery guarantees and operations

## Architecture decisions

| ADR | Decision |
|-----|----------|
| [0001](docs/adr/0001-job-lifecycle.md) | Four job statuses; attempts counted when a worker claims a job |
| [0002](docs/adr/0002-dual-write-strategy.md) | The jobs table is the outbox; a dispatcher publishes due jobs |
| [0003](docs/adr/0003-async-io-stack.md) | asyncio throughout: SQLAlchemy asyncio, asyncpg, aio-pika |
| [0004](docs/adr/0004-dispatch-and-consumption.md) | One quorum queue; the worker acks, nacks or rejects per outcome |
| [0005](docs/adr/0005-retries-and-attempt-records.md) | Only handler failures are retried, with full-jitter backoff; a row per attempt |
| [0006](docs/adr/0006-dead-letters.md) | Messages the worker can never process go to a dead-letter queue; failed jobs stay rows |
| [0007](docs/adr/0007-idempotency.md) | Submissions deduplicated by an `Idempotency-Key` in PostgreSQL; side effects by the receiver, with a key per job |

## Submitting a job

`POST /jobs` takes `{"type", "payload_version", "payload"}` and answers `202 {"id", "status"}`
with a `Location` header once the job is saved (not yet dispatched).

To retry a submission safely, send an `Idempotency-Key` header and retry with the same key and the
same body (ADR 0007):

```http
POST /jobs
Idempotency-Key: 8e03978e-40d5-43e8-bc93-6894a57f9324
Content-Type: application/json

{"type": "notification.send", "payload_version": 1,
 "payload": {"recipient": "max@example.com", "subject": "Hi", "body": "Hello"}}
```

| The key | Answer |
|---------|--------|
| Not sent, or not used before | `202` and a new job, as above |
| Used before, with the same `type`, `payload_version` and `payload` | `200` with that job's ID and *current* status, and its `Location`; nothing new is saved |
| Used before, with anything else | `422 idempotency_key_reused`; nothing is saved |

- A key is 1–255 visible ASCII characters (no spaces), compared exactly; `"abc"` with quotes
  and `abc` are different keys. Anything else, or the header sent twice, is `422 invalid_request`.
- Use a random value such as a UUIDv4, a new one per job. A key must not carry secrets: it is
  stored as sent (never returned or logged).
- **Keys never expire.** A key names its job for as long as the job exists, and jobs are never
  deleted.
- "The same body" means the same parsed JSON: whitespace and the order of object keys do not
  matter; any other difference counts.
- The key is checked before the body, and the body before the database, so a refused request does
  not use its key: fix the body and send it again with the same key.
- Two requests with one key at the same time do not both save a job: the second waits for the
  first one's COMMIT, then gets the `200`. If that wait outlasts `DB_STATEMENT_TIMEOUT_MS`, it
  gets `503`, and its next retry finds the job. If the first request fails instead, the waiting
  one saves its own job.

## Reading a job

`GET /jobs/{id}` returns a job's state from PostgreSQL: `status`, `attempts`, `max_attempts`, the
timestamps, and two fields that depend on the status. It never returns the payload.

| `status` | `result` | `failure` |
|----------|----------|-----------|
| `queued`, `running` | `null` | `null` |
| `succeeded` | what the handler returned, e.g. `{"provider": "fake", "notification_id": "…"}` | `null` |
| `failed` | `null` | `{"code": "handler_timeout", "message": "the job's handler did not finish in time"}` |

`failure.code` is one of a fixed set of categories and `failure.message` a fixed sentence per code,
never exception text, which can quote the payload. The key is `failure`, not `error`, so a failed
job's `200` body cannot be mistaken for the API's error envelope. A handler's result is shown to
clients, so a handler must never copy its payload into it.

A job that is waiting for a retry is `queued` with `attempts > 0` and shows `failure: null`: its
earlier failures are in its attempts.

## Retries and attempts

When a handler raises (`handler_error`) or exceeds `HANDLER_TIMEOUT_S` (`handler_timeout`), the
worker puts the job back to `queued`, due after a backoff, and acks the message; the dispatcher
publishes it again once it is due. After attempt `n` fails, the backoff is a random share of
`min(RETRY_MAX_DELAY_S, RETRY_BASE_DELAY_S × 2^(n−1))` (full jitter). The job fails when its last
attempt (`max_attempts`, set at submission from `DEFAULT_MAX_ATTEMPTS`) fails. A stored payload
that no longer passes validation fails the job at once: it would fail the same way every time
([ADR 0005](docs/adr/0005-retries-and-attempt-records.md)).

`GET /jobs/{id}/attempts` lists the job's recorded attempts, lowest first, never its payload:

```json
{"attempts": [
  {"attempt": 1, "started_at": "…", "finished_at": "…", "outcome": "failed",
   "failure": {"code": "handler_error", "message": "the job's handler raised an error"}},
  {"attempt": 2, "started_at": "…", "finished_at": null, "outcome": null, "failure": null}
]}
```

`outcome` is `succeeded`, `failed`, or `null` while the attempt runs. The list is empty until the
job is first claimed, and for a job that ran before migration 0003; an unknown job gets `404
job_not_found`. The list need not start at attempt 1: see Known limitations.

## Listing failed jobs

`GET /jobs?status=failed` lists failed jobs, newest first, each exactly as `GET /jobs/{id}` shows
it, never with its payload. `status` is required and only `failed` is accepted so far.

```json
{"jobs": [{"id": "…", "status": "failed", "attempts": 3, "max_attempts": 3, "…": "…",
           "failure": {"code": "handler_error", "message": "the job's handler raised an error"}}],
 "next_cursor": "MjAyNi0xMS0wN1QyMzo1OTowMS4xMjM0NTYrMDA6MDBf…"}
```

A page holds `limit` jobs (1–100, default 50; placeholders, not measured). Pass `next_cursor` back
as `cursor` for the next page; it is `null` on the last one. The cursor is opaque. Pages are cut by
the time a job failed and then its ID, and a failed job never changes again, so walking the pages
lists every failed job that existed when the walk began exactly once, even while new jobs fail. A
cursor the API did not make gets `422 invalid_request`. Migration 0004 adds the index the list
reads.

## Dead letters

A job that failed for good is its `failed` row in PostgreSQL, readable over the API. A **message**
the worker can never process is kept on RabbitMQ instead of being dropped
([ADR 0006](docs/adr/0006-dead-letters.md)). It goes to the dead-letter queue
`<JOBS_QUEUE_NAME>.dead` (`jobs.dead` by default) when:

- it cannot be parsed (the worker rejects it), with reason `rejected`;
- it is returned to the queue more than 20 times (`x-delivery-limit`): requeued with `reject` after
  a bug in its handling, or left unacknowledged when its worker dies, with reason `delivery_limit`.

A database outage never sends a message there: the worker answers it with `nack`, which RabbitMQ
does not count. Nothing reads the dead-letter queue and it never changes a job: a job whose message
died stays `queued` or `running` until recovery (phase 8). To see how many messages it holds:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

A message's body is `{"message_version": 1, "job_id": "…"}`, which names its job, and its
`x-death` header says from which queue and why. The project has no tool to show a body yet.

**Upgrading from phase 5.** The jobs queue's arguments are fixed when RabbitMQ creates it, so a
`jobs` queue created by an earlier version makes the dispatcher and the worker stop at start with
`PRECONDITION_FAILED`. Recreate it once: stop the dispatcher; let the workers empty the queue, then
stop them (a running worker would declare a deleted queue again, with the old arguments); run
`docker compose exec rabbitmq rabbitmqctl delete_queue jobs --if-empty --if-unused`; start the new
version.

## Health and readiness probes

- `GET /health` is liveness: it returns `200 {"status": "ok"}` whenever the process answers and
  touches no dependency, so a liveness check on it never restarts the API because of a database
  outage.
- `GET /ready` returns `200 {"status": "ready"}` when PostgreSQL answers and its migration revision
  is exactly the one this code expects. Otherwise it returns `503 service_unavailable` with
  `details` naming the failed check, `["database"]` or `["migrations"]`, never an error text or a
  revision id. The whole check is bounded by `READY_CHECK_TIMEOUT_S`.
- `/ready` does **not** check RabbitMQ. The API never talks to the broker: it only saves jobs, and
  a dispatcher publishes them (ADR 0002). An instance that can save jobs must keep accepting them
  during a broker outage.
- `/ready` takes its connection from the API's own pool, so it reports whether a request could get
  a connection right now, not only whether PostgreSQL is up. A full pool therefore fails `/ready`
  with `["database"]` even when PostgreSQL is fine, which takes a busy instance out of rotation.

## Known limitations

- **No authentication.** Anyone who can reach the API can submit jobs, which means triggering
  notifications, and anyone who knows a job ID can read that job's status. `GET /jobs?status=failed`
  also lets anyone find the IDs, codes and times of failed jobs without knowing an ID first (never
  their payloads). Run it only on a trusted network. The same goes for `/ready`: it tells anyone
  who can reach it whether the database is at this code's migration revision.
- A worker crash uses up one of the job's attempts, even when the crash had nothing to do with the
  job (ADR 0001).
- Dispatch latency is up to one dispatcher poll interval plus one poll (ADR 0002) while fewer than
  `DISPATCH_BATCH_SIZE` jobs are due. The dispatcher pauses after every poll, even a full one, so
  it publishes at most `DISPATCH_BATCH_SIZE` jobs per `DISPATCH_POLL_INTERVAL_S` (100 per second
  with the defaults) and a backlog of N batches waits about N intervals. A database error during
  a poll turns that poll's confirmed publishes into duplicates, which the worker's claim absorbs.
- The dispatcher declares the jobs queue only when it starts. If the queue is deleted while it
  runs, the broker returns every message and the jobs stay saved, unpublished, until a process
  declares the queue again.
- Ctrl-C stops the dispatcher with a traceback, abandoning the poll in progress: its transaction
  rolls back and those jobs are published again later, so publishes the broker had already
  confirmed become duplicates. Graceful shutdown comes in phase 8.
- A retry runs the handler again. For `notification.send` that is safe: the handler gives the
  provider the key `notification.send:{job_id}`, and the fake provider records one notification
  per key, so every attempt of a job, current or stale, gets the same `notification_id` (ADR 0007).
  This relies on the receiver deduplicating by key; a real provider usually forgets keys after a
  retention window, which must outlast a job's retries and any outage. Two different jobs with the
  same content still send twice.
- Notifications recorded before migration 0006 have no key: a job that had attempts before it and
  after it can have more than one row. A job that succeeded before phase 7 keeps its old result,
  `{"provider": "fake"}`, without a `notification_id`.
- Full jitter has no minimum wait, so a retry can be published almost at once. There is no
  circuit breaker: while a provider is down, every job uses up all its attempts.
- Attempt history starts with migration 0003. Jobs that ran before it have no attempt rows, and a
  job that was running during the migration lists its attempts from the next one on.
- An attempt whose worker crashed stays open (`finished_at` and `outcome` null) until stale-job
  recovery (phase 8), which must close it in the same transaction as the job's own change.
- A claim the database rejects because of the job's own data (SQLSTATE class 22 or 23) is logged
  at ERROR and its message acked: the job stays `queued`, already dispatched, until lost-message
  recovery (phase 8). Retrying would fail the same way on every delivery (ADR 0004).
- Ctrl-C stops the worker with a traceback. The broker requeues the message it held. If the job
  was already claimed, the redelivery loses the claim, so the job stays `running` until stale-job
  recovery (phase 8), and its notification may already have been sent, once (a later attempt gets
  the same one); otherwise the job is claimed normally later.
- When the final update fails with a database error, the message is requeued, but its redelivery
  loses the claim: the job stays `running` until stale-job recovery (phase 8).
- An unexpected error while handling a message (a bug) requeues it with `reject`, which counts:
  past RabbitMQ's delivery limit (20) the message goes to the dead-letter queue, and a job that was
  never claimed stays `queued`, already dispatched, until lost-message recovery (phase 8).
- Migration 0004 builds its index with a plain `CREATE INDEX` inside Alembic's transaction, which
  blocks writes to `jobs` while it runs (`CONCURRENTLY` cannot run in a transaction). Migrations
  0005 and 0006 add columns and unique constraints (0005 also CHECKs, which scan `jobs`), and
  these block reads too, of `jobs` and `fake_notifications`, until they commit (PostgreSQL
  documentation). Fine for the table sizes here, but not measured.
- The dead-letter queue grows without limit: nothing consumes or expires it, and clearing it
  (purging the queue) is an operator's decision. If it is deleted while the system runs, a message
  that dies meanwhile waits for it and shows up in neither queue's `messages` count until it is
  declared again; RabbitMQ then moves it, about 3 minutes later in the one test (ADR 0006).
- The worker reconnects after a broker outage (an 87 s outage was observed once), but aio-pika can
  still end its consumer, after which the worker exits with an error and must be started again.
- The dispatcher and the worker do not check the database's migration revision when they start.
  Against a database that is unmigrated or behind this code they keep running. Without the `jobs`
  table every dispatcher poll fails and is logged; with an older schema the dispatcher publishes
  as usual and the worker's claim fails, so the worker backs off and requeues each message until
  the schema is there. Without migration 0003, for example, every claim fails because
  `job_attempts` does not exist (SQLSTATE 42P01), which the worker treats as an outage. Nothing is
  lost. A check at start comes with phase 8.
- No constraint ties `result` and `error_code` to the status: `GET /jobs/{id}` derives both fields
  from the status alone, and only the worker's own updates keep the columns consistent. A
  `failed` row without an error code shows `failure: null`, and a code this version does not know
  (e.g. one written by a newer worker) is shown with the message "the job failed". A `succeeded`
  row whose result is missing or not a JSON object shows `result: null`. Each case is logged as a
  warning, by job ID only.
- `fake_notifications.job_id` has no index, so looking up a job's notifications (only tests do)
  scans the table. That is fine for the fake provider; a real provider's records would need one.
- A worker whose lease expired may still finish its handler. Its result is rejected, but its side
  effect still happens; `notification.send` makes it the same notification as the current
  attempt's. A future handler whose receiver takes no key needs a guard of its own (ADR 0007).
- Recovering a crashed worker's job takes up to the lease duration plus the recovery interval.
- **A submission without an `Idempotency-Key` can be saved twice.** If the connection drops or
  `DB_COMMAND_TIMEOUT_S` expires while PostgreSQL commits, the client gets 503 although the job
  may have been saved, and the same happens to a client that never received its 202; a retry
  without a key then creates a second job. A retry with the same key gets the saved job instead.
- **Idempotency keys are global.** Without authentication there is one key space for everyone:
  anyone who guesses a key and sends the same body gets that job's ID, a different body gets 422
  (which shows the key is taken), and someone who learns a key before its owner uses it can take
  it first. Random keys make guessing impractical.
- Database and connection errors are logged as their class, SQLSTATE and constraint name only,
  without a traceback, because PostgreSQL's error detail can quote the stored payload. Any
  `OSError` is treated as "database unavailable" (503). Other unexpected errors return 500 and
  uvicorn logs their full traceback for debugging, so a bug whose traceback holds request data
  could still write it to the log.
- The worker's claim reads the job's payload from PostgreSQL. Nothing logs it at the default log
  levels, but SQLAlchemy's `sqlalchemy.engine` logger at DEBUG logs result rows, payloads
  included: `hide_parameters` hides only query parameters. Logging configuration comes with
  phase 8.
- uvicorn's access log records each request's raw path and query string, so job IDs, whatever a
  client sent in place of one and anything after a `?` end up in the log. Payloads never do: they
  travel only in request bodies.
- `/ready` requires the database revision to equal the code's exactly. During a rolling deploy,
  instances running the old code report not ready as soon as the new migration is applied. The MVP
  runs one instance and migrates before starting it.
- Each failing `/ready` call logs one warning, so frequent probing during an outage produces many
  identical lines. The database line includes pool usage (e.g. `pool 5/5 in use`), but under
  traffic a slow database also fills the pool, so a full pool alone does not show the database is
  healthy.
- All limits and timeouts are placeholders, not measured values; `.env.example` lists every one
  with its default (e.g. payload 64 KiB, statement timeout 5 s, client command timeout 10 s).

## Development setup

Requires [uv](https://docs.astral.sh/uv/), GNU Make and Docker with Compose. uv installs the pinned
Python (3.12, see `.python-version`) if it is missing.

```bash
make install                  # create .venv and install locked dependencies
uv run pre-commit install     # run lint, format, mypy and secret checks on every commit
cp .env.example .env          # then replace every change-me with a local password
make up                       # start PostgreSQL (127.0.0.1:5432) and RabbitMQ (127.0.0.1:5672)
make db-migrate               # apply migrations to the development database
make run                      # start the API on 127.0.0.1:8000 (docs at /docs)
make dispatcher               # publish saved jobs to RabbitMQ until Ctrl-C
make worker                   # run published jobs, one at a time, until Ctrl-C
make check                    # lint, format check, mypy --strict, unit tests
make test-integration         # tests against task_platform_test and the local RabbitMQ
make check-all                # both of the above
make format                   # apply ruff formatting and safe lint fixes
make down                     # stop both; the data volumes are kept
```

- The passwords in `.env` are applied only when each container's volume is first created, and
  the `task_platform_test` database is created at the same moment. Changing them later requires
  recreating the volumes (`docker compose down -v`, which deletes all local data).
- `make up` waits until both healthchecks pass: PostgreSQL accepts connections and RabbitMQ's
  AMQP listener is up.
- Integration tests fail, rather than skip, when `TEST_DATABASE_URL` or `TEST_BROKER_URL` is
  missing, and refuse any database whose name does not end in `_test`. Broker tests declare
  queues with unique names and delete only those and their `.dead` queues, so they can share the
  local RabbitMQ.
- The dispatcher stops at start if RabbitMQ is unreachable or the jobs queue cannot be declared; a
  database outage only fails polls, and the next poll tries again. It prints nothing while it
  works: until logging is configured (phase 8) only warnings reach stderr, such as a failed poll
  or a poll whose publishes were not confirmed (one line per poll, by error class).
- The worker also stops at start if RabbitMQ is unreachable or the queue cannot be declared, for
  example a `jobs` queue from before phase 6 (see Dead letters, "Upgrading from phase 5"). While
  the database is down it gives each message back (`nack`) after `WORKER_DB_ERROR_BACKOFF_S`, so
  no message is lost; its warnings name the job ID and error class, never the payload. Several
  workers can run at once: the claim lets only one of them run a job.
- Migrations are written by hand, never autogenerated:
  `make db-revision id=0002 m="what it does"` creates an empty revision to fill in.

CI runs `make check` and the integration tests against PostgreSQL and RabbitMQ service containers
on Python 3.12 and 3.14, and scans the full git history for secrets with gitleaks.
