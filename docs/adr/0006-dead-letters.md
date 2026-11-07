# ADR 0006 — Dead letters

- **Status:** Accepted
- **Date:** 2026-11-07

## Context

Work can fail for good in two places.

- **A job** fails for good when its handler fails without a retry left, or when its stored payload
  no longer passes validation. PostgreSQL already records this: the job is `failed`, with a safe
  code (ADR 0004, ADR 0005). Today it can only be read one job at a time, by ID.
- **A message** fails for good when the worker can never process it. Until now RabbitMQ drops
  these messages (ADR 0004 decision 7):
  - a message that cannot be parsed, answered with `reject(requeue=False)`;
  - a message that goes over the queue's delivery limit: one that a bug keeps answering with
    `reject(requeue=True)`, or one whose worker keeps dying while it holds the message.

  A dropped message leaves no trace on the broker. Its job, if it has one, stays `queued` with
  `dispatched_at` set, or `running`, until the recovery of phase 8.

Phase 6 should make both kinds visible without making the broker a second source of truth for
job state (ADR 0002). This ADR changes ADR 0004 decision 1 (the jobs queue's arguments) and
decision 7 (rejected and over-limit messages are dead-lettered, not dropped); ADR 0004 gets a dated
update when the code changes.

The behaviour below was observed on RabbitMQ 4.3.7 with aio-pika 10.2.1 (aiormq 7.2.4) on
2026-11-07, unless marked as taken from the documentation. The spike used queues of its own, with
the delivery limit lowered to 2.

## Decision

1. **There are two kinds of dead letter, and each stays where it already is.** A job that failed
   for good is its `failed` row in PostgreSQL. It is inspected through the API, not the broker. A
   message that could not be processed is kept in a dead-letter queue on the broker. A failed job
   is never also published to the dead-letter queue: that would be a second write to the database
   and the broker for one change, the problem ADR 0002 avoids.
2. **One dead-letter queue, `{jobs_queue_name}.dead`** (`jobs.dead` by default), is a durable
   quorum queue with no other arguments: no dead-lettering of its own, and RabbitMQ's default
   delivery limit of 20 (documentation). One function declares it and then the jobs queue, so the
   dead-letter route exists before any message can die; the dispatcher and the worker both call
   it. The suffix adds 5 characters, so `jobs_queue_name` is capped at 250 to keep the dead-letter
   queue's name within RabbitMQ's 255 (documentation).
3. **The jobs queue gets these arguments**, next to `x-queue-type: quorum`:

   | Argument | Value | Why |
   |----------|-------|-----|
   | `x-dead-letter-exchange` | `""` (the default exchange) | It routes by queue name, so no exchange of our own is needed. |
   | `x-dead-letter-routing-key` | `{jobs_queue_name}.dead` | The dead-letter queue's name. |
   | `x-dead-letter-strategy` | `at-least-once` | A dead-lettered message is not dropped if it cannot be routed (decision 4). |
   | `x-overflow` | `reject-publish` | Required by `at-least-once` (documentation). It only acts on a queue with a length limit, and this one has none (documentation); a confirmed publish routed as before (observed). |
   | `x-delivery-limit` | `20` | RabbitMQ's default (documentation), now written down, so the queue's arguments state the whole policy. |

   The limit is an integer keyword parameter of the declaring function, 20 by default, so tests
   can lower it. RabbitMQ seems to compare an argument's type as well as its value (inferred from
   the error text, which names the type), so it must stay an integer.
4. **Dead-lettering is at-least-once.** With the dead-letter queue missing, a rejected message was
   not dropped. The jobs queue no longer counted or delivered it, and RabbitMQ moved it into the
   dead-letter queue about 183 s after that queue was declared (observed once). The broker's
   `dead_letter_worker_publisher_confirm_timeout` is 180000 ms, which probably explains the wait
   (inferred, not tested). With the default `at-most-once` strategy the same message had not
   arrived 300 s after the dead-letter queue was declared: it was dropped (observed). The
   dead-letter queue is declared before the jobs queue in normal operation, so this only matters
   if someone deletes it while the system runs.
5. **What reaches the dead-letter queue, and how it is marked** (observed):

   | The worker's answer | Reason in `x-death` |
   |---------------------|---------------------|
   | `reject(requeue=False)`, for a message that cannot be parsed | `rejected` |
   | `reject(requeue=True)`, for a bug, past the delivery limit | `delivery_limit` |
   | Channel closed with the message unacknowledged (the worker died), past the limit | `delivery_limit` |
   | `nack(requeue=True)`, for a database outage | Never: `nack` is still not counted (`x-delivery-count` stayed 0 over 6 nacks) |

   With a limit of `N`, a message is dead-lettered when it comes back from its `N + 1`-th
   delivery: with a limit of 2, the worker saw `x-delivery-count` 0, 1 and 2, and the third
   return dead-lettered it. RabbitMQ adds an `x-death` entry (`reason`, `queue`, `count`, `time`,
   `exchange`, `routing-keys`) and the `x-first-death-*` and `x-last-death-*` headers, so the queue
   and the reason can be read without parsing the body. The body and the persistent delivery mode
   are unchanged.
6. **Nothing reads the dead-letter queue, and it never changes a job.** An operator counts its
   messages with `rabbitmqctl list_queues`. The project has no tool that shows a message's body
   (the image has no management plugin), so matching a dead letter with its job takes a manual
   `basic.get` followed by `nack(requeue=True)`. Answering with `reject(requeue=True)` instead
   would count towards the dead-letter queue's own limit of 20 and then drop the message, since
   that queue has no dead-lettering of its own. A body names the job (`{message_version, job_id}`,
   ADR 0004 decision 4). What happened to that job is still decided only by PostgreSQL:
   - an unparsable message has no job;
   - a job whose message died before any claim stays `queued` with `dispatched_at` set until
     lost-message recovery (phase 8);
   - a job that was claimed stays `running` until its lease expires and lease recovery (phase 8)
     moves it on.
7. **An existing jobs queue must be recreated once.** RabbitMQ fixes a queue's arguments when it
   creates the queue. Declaring the new arguments over a jobs queue created without them fails
   with `PRECONDITION_FAILED`, naming the first argument that differs (observed: "inequivalent arg
   'x-dead-letter-exchange'"). An explicit `x-delivery-limit` of 20 also differs from a queue that
   relied on the default (observed). The dispatcher or worker then stops at start (ADR 0004
   decision 2), after it has created the empty dead-letter queue, which does no harm. Neither runs
   against a queue without dead-lettering. To upgrade:
   1. Stop the dispatcher, so nothing new is published.
   2. Let the workers empty the jobs queue, then stop them. They must be stopped before the
      delete: a running worker's robust channel declares a deleted queue again (ADR 0004 update
      2026-11-02), with the old arguments.
   3. `rabbitmqctl delete_queue jobs --if-empty --if-unused`, which refuses a queue that still
      holds messages or has a consumer.
   4. Start the new version, which declares both queues.

   Running the old version against the new queue presumably fails the same way; that direction
   was not observed.

## Consequences

- A message the worker cannot process is kept on the broker, with the reason, instead of
  disappearing. Its job is not changed: the broker still proves nothing about a job (ADR 0002).
- The dead-letter queue grows without limit. Nothing consumes or expires it. Clearing it is an
  operator's decision, since purging a queue is destructive.
- While a dead-lettered message waits for a missing dead-letter queue, the jobs queue does not
  deliver it, and its `messages`, `messages_ready` and `messages_unacknowledged` all show 0
  (observed). Other counters were not checked, so an operator may see nothing until it arrives.
- An unparsable body can hold anything a publisher sent. It stays on the broker and is never
  logged.
- Jobs whose messages were dead-lettered still wait for phase 8 recovery, as before. The
  difference is that their messages can now be found.
- The upgrade in decision 7 needs a short stop of the dispatcher and the workers. New jobs keep
  being saved meanwhile (ADR 0002).

## Alternatives considered

- **A RabbitMQ policy instead of queue arguments.** RabbitMQ recommends policies for
  dead-lettering (documentation), because a policy can change without recreating the queue. But a
  policy lives outside the processes: a fresh broker (CI, a clean clone) has no dead-lettering
  until an extra step sets it, and the processes cannot check at start that it is there, while
  wrong arguments already fail with `PRECONDITION_FAILED`. The per-test queues and the lowered
  limit in tests are also naturally per queue. Rejected; the one-time upgrade in decision 7 is the
  cost.
- **A dead-letter exchange of our own.** It adds an exchange with no routing choice to make: there
  is one dead-letter queue. The default exchange already routes by queue name. Rejected. This
  settles ADR 0004's deferred "dedicated exchange".
- **`at-most-once` dead-lettering (the default).** Simpler, but a message is dropped when the
  dead-letter queue cannot take it (observed, decision 4). Rejected.
- **Publishing failed jobs to the dead-letter queue as well.** It would put both kinds of dead
  letter in one place, but it adds a second write to the database and the broker for one change,
  and the job's row already holds everything the message would. Rejected.
- **Failing a job when its message is dead-lettered.** It would end such jobs before phase 8, but
  it lets a broker event decide job state, and it needs a consumer on the dead-letter queue. Lease
  and lost-message recovery (phase 8) decide from PostgreSQL instead. Rejected.
- **A consumer that records dead letters in a table, with an API.** It would make dead letters
  visible over HTTP, but it adds a process and a third write path. Deferred.
- **A command that shows the dead-letter queue's messages without removing them** (get, then
  `nack` back). Useful for an operator, but how a requeued message is ordered in the dead-letter
  queue was not checked. Deferred.
- **A TTL or length limit on the dead-letter queue.** It bounds the queue but drops the evidence
  it exists to keep. Deferred until there is a retention requirement.
