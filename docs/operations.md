# Operations

How to run, configure, stop and watch the three processes, and what they do when something goes
wrong. The setup commands are in the [README's Development setup](../README.md#development-setup);
the guarantees behind this page
are in [delivery-guarantees.md](delivery-guarantees.md), the known gaps in
[limitations.md](limitations.md). This is a local, single-machine setup: there is no deployment
or monitoring tooling.

## Processes and start order

| Process | Command | Talks to |
|---|---|---|
| API | `make run` (uvicorn on 127.0.0.1:8000) | PostgreSQL only |
| Dispatcher | `make dispatcher` (`python -m task_platform.dispatcher`) | PostgreSQL, RabbitMQ |
| Worker | `make worker` (`python -m task_platform.worker`) | PostgreSQL, RabbitMQ |

1. `make up` starts PostgreSQL and RabbitMQ and waits until both healthchecks pass.
2. `make db-migrate` brings the database to this code's migration revision. The dispatcher and the
   worker exit with status 1 at start if it is not there, and `/ready` reports `["migrations"]`.
3. The API, the dispatcher and the workers can then start in any order. The API needs no broker;
   the dispatcher and the worker wait for whichever dependency is not reachable yet (see Starting).

The API's `202` only means a job is saved. Without a dispatcher nothing is published, and without
a worker nothing runs; the jobs wait in `queued`.

## Configuration

All three processes read `.env` (copied from `.env.example`, never committed) and the environment.
`.env.example` lists every setting with its default; the defaults are placeholders, not measured
values. Some settings must agree:

- The dispatcher and the worker must share `DB_STATEMENT_TIMEOUT_MS`: a worker's claim can wait on
  the dispatcher's row locks for `PUBLISH_TIMEOUT_S` plus the mark and commit, under the worker's
  statement timeout. One `.env` for both keeps them equal.
- All three processes refuse to start if `DB_COMMAND_TIMEOUT_S` is not longer than
  `DB_STATEMENT_TIMEOUT_MS`, or `MAX_REQUEST_BODY_BYTES` is smaller than `MAX_PAYLOAD_BYTES`.
- The dispatcher and the worker also refuse if `PUBLISH_TIMEOUT_S` is not shorter than
  `DB_STATEMENT_TIMEOUT_MS`, `HANDLER_TIMEOUT_S` not shorter than `LEASE_DURATION_S`, or
  `RETRY_BASE_DELAY_S` exceeds `RETRY_MAX_DELAY_S`. The API does not read these settings.
- A refused or malformed setting stops the process before it connects to anything, with a
  validation error that names the setting (never its value) and a traceback, not the logged ERROR
  of a start-up failure (see Starting). So does a malformed `DATABASE_URL` in the dispatcher and
  the worker: it fails when their database engine is created, before the start-up checks, with a
  traceback that does not show the password.
- `JOBS_QUEUE_NAME` must be the same for the dispatcher and every worker.

## Migrations

Migrations are written by hand (`make db-revision`), applied with `make db-migrate`, and each has a
working downgrade, tested up, down and up again. Some downgrades drop data: 0002's the results,
error codes and recorded notifications, 0003's the attempt history, 0005's and 0006's the
idempotency keys.

The dispatcher and the worker check the revision only at start. The API checks nothing at start
and keeps serving with a schema of another revision; only `/ready` shows the mismatch. So stop the
API, the dispatcher and the workers, migrate, then start them again. Some migrations block writes
while they run (see [limitations.md](limitations.md)).

## Running several workers and dispatchers

- **Workers:** run as many as needed; each takes one message at a time (prefetch 1). The claim
  lets only one of them run a job, so a duplicate delivery is refused and acked.
- **Dispatchers:** polls and both recovery sweeps take their rows with
  `FOR UPDATE SKIP LOCKED`, so several dispatchers never publish or recover the same job twice in
  one round. One is enough for the local setup.

## Failed jobs and dead letters

A job that failed for good is its `failed` row in PostgreSQL. `GET /jobs?status=failed` lists them
newest first, each with its failure code, and `GET /jobs/{id}/attempts` shows how each attempt
ended. Nothing retries a failed job.

A **message** the worker cannot parse, or that keeps coming back, is kept on RabbitMQ instead of being dropped
([ADR 0006](adr/0006-dead-letters.md)). It goes to the dead-letter queue
`<JOBS_QUEUE_NAME>.dead` (`jobs.dead` by default) when:

- it cannot be parsed (the worker rejects it), with reason `rejected`;
- it is returned to the queue more than 20 times (`x-delivery-limit`): requeued with `reject` after
  a bug in its handling, or left unacknowledged when its worker dies, with reason `delivery_limit`.

A database outage never sends a message there: the worker answers it with `nack`, which RabbitMQ
does not count. Nothing reads the dead-letter queue and it never changes a job: a job whose message
died is taken on by recovery instead (see Recovery), so a dead letter does not mean its job is
stuck. To see how many messages it holds:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

A message's body is `{"message_version": 1, "job_id": "…"}`, which names its job, and its
`x-death` header says from which queue and why. The project has no tool to show a body. The
queue grows without limit; clearing it (purging the queue) is an operator's decision.

**Upgrading from phase 5.** The jobs queue's arguments are fixed when RabbitMQ creates it, so a
`jobs` queue created by an earlier version makes the dispatcher and the worker stop at start with
`PRECONDITION_FAILED`. Recreate it once: stop the dispatcher; let the workers empty the queue, then
stop them (a running worker would declare a deleted queue again, with the old arguments); run
`docker compose exec rabbitmq rabbitmqctl delete_queue jobs --if-empty --if-unused`; start the new
version.

## Recovery

The dispatcher runs two recovery sweeps every `RECOVERY_INTERVAL_S` (30 s), next to its polls
([ADR 0008](adr/0008-recovery-and-shutdown.md)):

- **Expired leases.** A job still `running` after its lease (`LEASE_DURATION_S`, 120 s) expired
  belonged to a worker that crashed or was stopped too soon. With attempts left it goes back to
  `queued`, due at once, without backoff; otherwise it fails. Either way its open attempt is closed
  as `failed` with `lease_expired`, in the same transaction. A worker that finishes after the
  sweep writes nothing: its result is rejected like any stale one.
- **Lost messages.** A job `queued` and published more than `LOST_MESSAGE_AFTER_S` (600 s) ago
  whose message never led to a claim (lost, dead-lettered, or rejected at the claim) is marked
  unpublished, and the next poll publishes it again. The sweep runs only while the jobs queue has
  no ready messages, so a message that is only waiting in a backlog is not published twice. While
  the broker is unreachable it is skipped; the lease sweep needs only the database and still
  runs.

Both sweeps take at most `DISPATCH_BATCH_SIZE` jobs per pass with `FOR UPDATE SKIP LOCKED`, so
several dispatchers can run them without recovering a job twice.

## Stopping

On SIGTERM or SIGINT (Ctrl-C):

- the worker cancels its consumer, lets the job in hand finish and be acked for up to
  `SHUTDOWN_GRACE_S` (30 s), and exits with status 0. Past that, the job is cancelled: its message
  goes back to the queue and lease recovery takes the job on, which uses up the attempt;
- the dispatcher lets the poll and the recovery pass in hand commit, starts no new ones, and exits
  with status 0.

A second signal stops either one within a few seconds: it cancels the work in hand (a dispatcher's
open transaction rolls back, and its confirmed publishes are repeated on the next start), and each
connection's close is bounded by `BROKER_CONNECT_TIMEOUT_S` and `DB_CONNECT_TIMEOUT_S`. If you run
the processes in containers (this repository has none for them), note that Docker waits 10 s
between SIGTERM and SIGKILL by default, less than the 30 s grace: give `docker stop` a longer
`-t`.

## Starting

Both processes first check that the database is at exactly this code's migration revision, as
`/ready` does, then connect to RabbitMQ:

- a database that is unreachable, starting up or out of connections, and a broker that is
  unreachable, are waited for: a warning, then another try every `STARTUP_RETRY_INTERVAL_S` (5 s)
  until it works or a signal comes;
- a configuration error stops the process with an ERROR and exit status 1: another migration
  revision (both are named), a wrong password, an unknown database or vhost, a missing privilege,
  a malformed broker URL.

The dispatcher and the worker also stop at start, with exit status 1 and a traceback, if the jobs
queue cannot be declared, for example a `jobs` queue from before phase 6 (see Upgrading from
phase 5).

## Outages while running

The API never talks to RabbitMQ, so a broker outage never fails a submission: jobs wait in
`queued` and are published once the broker is back. The dispatcher and the worker reconnect by
themselves. If the worker's consumer ends or the broker fails it, the worker consumes again on a
new channel every `STARTUP_RETRY_INTERVAL_S` until that works; only a queue the broker refuses to
declare stops it. The tests cut and freeze connections through a small TCP proxy instead of
stopping the shared containers.

A database outage only fails the dispatcher's polls and sweeps; the next one tries again. While
the database is down the worker gives each message back (`nack`) after
`WORKER_DB_ERROR_BACKOFF_S`, so no message is lost; its warnings name the job ID and error class,
never the payload. API requests that need the database answer `503 service_unavailable` when it
is unavailable (other database errors are `500`); a submission cut off during its `COMMIT` may
still have been saved (see [limitations.md](limitations.md)). `/health` still answers `200`, and
`/ready` answers `503` with `["database"]`.

## Logs

The dispatcher and the worker log one line per event to stderr, with time, level and logger, at
`LOG_LEVEL` (`INFO` by default; `DEBUG`, `INFO` or `WARNING`). SQLAlchemy's engine stays at WARNING
whatever the level, since at DEBUG it would log result rows, payloads included. Errors are logged
by class and SQLSTATE, never their text. The API keeps uvicorn's logging.

A running dispatcher logs each poll that found due jobs, with how many it published, and each job
it recovered. Some library lines are noisy or misleading; [limitations.md](limitations.md) lists
them.

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

The dispatcher and the worker have no probes; watch their logs and exit status.
