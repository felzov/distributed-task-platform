# ADR 0007 — Idempotency

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

Delivery is at-least-once, and two kinds of repeat are still possible.

- **A client submits the same job twice.** `POST /jobs` saves the job and answers 202 after the
  COMMIT (ADR 0002). If the database connection drops or `DB_COMMAND_TIMEOUT_S` expires while
  PostgreSQL commits, the client gets 503 although the job may have been saved, and a retry saves
  a second job (the README's limitation). A client that saved a job but never received the 202
  (its own timeout, a dropped connection) retries the same way.
- **A handler's side effect happens twice.** A duplicate message is already harmless: the claim
  is a conditional update, so a message for a job that is no longer `queued` loses the claim and is
  acked (ADR 0001 decision 7, ADR 0004). A new attempt of the same job still runs the handler again:
  after a handler error or timeout and a retry (ADR 0005), and, from phase 8, after lease recovery,
  whether the old worker crashed or is still finishing (ADR 0001 decision 8, ADR 0003). Fencing
  rejects the stale attempt's database writes, not its side effects (ADR 0001). The one handler,
  `notification.send`, calls a fake provider that records each call as a row in
  `fake_notifications`, committed in its own transaction before the job is finished, as a real
  provider's call would be (ADR 0004). Until this ADR is implemented, every attempt that reaches
  it adds a row, including one whose insert committed although the handler then saw a database
  error (a `handler_error`, so a retry).

ADR 0001, 0004 and 0005 defer idempotent side effects to phase 7, and the README defers
idempotency keys for submission to it. The two problems look alike but differ in one way that
decides the design: a submission is a write to our own database, so it can be deduplicated inside
one transaction; a side effect happens outside our database, so it cannot.

This ADR changes a consequence of ADR 0001 (side effects must be made idempotent in phase 7), ADR
0004's fake-provider bullet (rows are never deduplicated) and a consequence of ADR 0005 (a retry
calls the provider again until phase 7). Each gets a dated update when the code changes.

The header contract follows the IETF draft `draft-ietf-httpapi-idempotency-key-header-08`
(2026-10-22, an active Internet-Draft, not an RFC), read on 2026-11-09, where decisions 2–6 cite
it; decision 5 departs from it on purpose. Behaviour marked "documentation" is taken from the
PostgreSQL documentation, not observed here. Phase 7's later slices observe some of these claims:
that jobs without a key do not conflict (migration 0005's tests), and that an insert waits for an
uncommitted insert of its key, then finds that job, or saves its own if the first one rolls back
(the insert-or-find query's tests, and the same for the fake provider's key). The wait's bound by
`DB_STATEMENT_TIMEOUT_MS` and the lock levels stay untested.

## Decision

1. **Two problems, two mechanisms.** A submission is deduplicated by an idempotency key in
   PostgreSQL, in the same transaction as the job's `INSERT` (decisions 2–7). A side effect is
   deduplicated by its receiver, with a key the handler derives from the job (decisions 8–9).
2. **`POST /jobs` takes an optional `Idempotency-Key` header.** Its value is opaque: 1 to 255
   visible ASCII characters (0x21–0x7E), compared byte for byte. An empty or longer value, a
   space, a control or non-ASCII character inside the value, or the header sent more than once is
   a 422 `invalid_request`. The value is never echoed in a response, never returned by any
   endpoint, and never logged by this code. Without the header, `POST /jobs` behaves as before.
   - The draft defines the value as a Structured Field String (section 2.1) and quotes it in its
     examples (sections 2.2 and 6). This API does not parse it as one. A client that follows the
     draft sends `"…"`, and the quotes are simply part of the key, so it gets the same behaviour
     as long as it sends the same bytes each time and its string has no space (a Structured Field
     String may hold one; this API rejects it). A client that sends the same key once quoted and
     once bare gets two different keys.
   - The draft recommends a UUID or a similar random value (section 2.2).
3. **A request fingerprint tells a retry from a reuse.** It is the SHA-256 of
   `json.dumps({"type": …, "payload_version": …, "payload": …}, sort_keys=True,
   separators=(",", ":"), ensure_ascii=False)` encoded as UTF-8, over the three values as the API
   parsed them, computed after validation. It is not computed over the raw body, so whitespace and
   key order do not matter; nor over the validated model's dump, so a default field added to a
   payload model later does not turn an honest retry into a reuse. The draft allows a fingerprint
   of this kind (section 2.4). It is a pure function in `domain/idempotency.py` (phase 7, S2).
   Fingerprints are stored for as long as their keys, so this exact form is part of the stored
   data: changing any argument would turn retries of older keys into 422.
4. **The key and the fingerprint are columns of `jobs`** (migration 0005): `idempotency_key`
   (text) and `request_hash` (bytea), both nullable, with
   - `UNIQUE (idempotency_key)`; PostgreSQL treats NULLs as distinct by default (documentation),
     so jobs without a key are not affected;
   - a CHECK that both are NULL or both are set;
   - a CHECK on the key's length (1–255) and on the hash's length (32 bytes).

   Existing jobs keep NULL in both and need no backfill. A key lives as long as its job, and jobs
   are never deleted, so **a key never expires**. If jobs are ever deleted, their keys go with them
   and the published policy must change. The draft asks a resource to publish its expiry policy
   where one applies (sections 2.3 and 2.5.2).
5. **One transaction inserts or finds the job.** `INSERT … ON CONFLICT (idempotency_key) DO
   NOTHING RETURNING id, status`. If no row comes back, the key is taken: `SELECT id, status,
   request_hash … WHERE idempotency_key = :key`. Under READ COMMITTED, the isolation level these
   sessions use (PostgreSQL's default, not changed here), an `INSERT` that meets a key another
   transaction has inserted but not yet committed waits for that transaction. If it commits, the
   `INSERT` does nothing and the following `SELECT`, which takes a new snapshot, sees the row. If
   it rolls back, the `INSERT` goes ahead (documentation).
   - **This departs from the draft on purpose.** For a request retried while the first is still
     being processed, the draft says the resource SHOULD answer 409 (sections 2.6 and 2.7). Here
     the second request waits for the first one's COMMIT instead, then gets the answer for a
     completed request (decision 6). The window is one `INSERT` and its COMMIT, so a wait is
     cheaper for the client than a 409 it has to retry.
   - The wait is bounded by `DB_STATEMENT_TIMEOUT_MS` (documentation). A wait that hits it fails
     with SQLSTATE 57014, which the API already answers with 503; the client's next retry with the
     same key then finds the committed job.
6. **The answer depends on what the key already names.**

   | Key | Answer |
   |-----|--------|
   | Not sent, or not used before | 202 `{id, status}` and `Location`, as before |
   | Used before, same fingerprint | **200** `{id, status}` of that job, its *current* status, and the same `Location` |
   | Used before, different fingerprint | **422 `idempotency_key_reused`**, no new job |

   A 200 instead of a repeated 202 tells the client that nothing new was saved, and the current
   status is what a client would read next anyway. For a retry after the first request completed,
   the draft asks for a response consistent with the request having been completed (section 2.6,
   whose retry text Appendix A marks as still open) and does not require the first response to be
   replayed. It uses 422 for a key reused with a different payload (section 2.7).
7. **Every check runs before the database is touched**, cheapest first: the key's format (a
   header, so no body is read), the body's size (413, as today), the envelope, then the payload
   (as today). Only a valid request is ever written, so a request that fails a check does not use
   its key up: the same key with a corrected body creates the job.
8. **The provider deduplicates by a key the handler gives it.** The fake provider's call becomes
   `send(key) → notification_id`: `INSERT … ON CONFLICT (idempotency_key) DO NOTHING`, then the
   row's id, so every call with one key returns the same id and leaves one row. Migration 0006
   adds `fake_notifications.idempotency_key` (text, nullable, `UNIQUE`). Rows recorded before it
   keep NULL: their duplicates are what phases 3–6 did, and are kept. The handler's key is
   `notification.send:{job_id}`, so every attempt of a job, current or stale, maps to one
   notification. A job's payload never changes, so one key always stands for one request, and the
   provider does not compare contents. The `attempt` column keeps recording which attempt made the
   first call.
9. **The handler's result names the notification:** `{"provider": "fake", "notification_id":
   "<uuid>"}`, the same for every attempt of a job. A client reads it from `GET /jobs/{id}` once
   the job has succeeded.

## Consequences

- The 503-after-COMMIT duplicate, and the lost 202, are closed for clients that send a key and
  retry with it. Clients that send no key keep the old behaviour, which stays a documented
  limitation.
- **Keys are global.** There is no authentication, so there are no per-client namespaces (the
  draft's section 5 suggests a key combined with client attributes). Anyone who guesses a key and
  sends the same body gets that job's ID. With a different body they get 422, which reveals that
  the key is taken. And someone who learns a key before its owner uses it can take it first, so
  the owner's request fails with 422. Random keys make guessing impractical; this is a known
  limitation, like the failed-jobs list that anyone can read (README, Known limitations).
- A key reused years later still names its old job, since keys never expire. A client that needs
  a fresh job uses a fresh key.
- The fingerprint compares the parsed JSON, not its meaning. The same text written with Unicode
  normalized differently, or a number written as `1` and later as `1.0`, counts as a different
  request (422). The envelope already requires `payload_version` to be a JSON integer, and the one
  payload today has no numbers; a future payload with numbers inherits this.
- The key is stored as sent, so it must not carry anything secret. The API cannot check that.
- The README must publish this contract (draft section 2.5.2): the header and its format, the
  answers in decision 6, that keys never expire, that a key should be a UUID or similar random
  value, and that it must not carry secrets.
- Migrations 0005 and 0006 alter their tables inside Alembic's transaction. `ALTER TABLE … ADD
  COLUMN` and `ADD CONSTRAINT` take an ACCESS EXCLUSIVE lock (documentation), held until the
  migration commits, so reads *and* writes of `jobs` (0005) or `fake_notifications` (0006) wait
  while the columns are added, the unique index is built and the CHECKs scan the table. Migration
  0004's `CREATE INDEX` blocked only writes. Not measured.
- `notification.send` produces one notification per job, however often the provider is called:
  after a retry, and in phase 8 after a stale attempt finishing or crash recovery.
  Two *different* jobs with the same content still send twice; deduplicating those is the
  client's job, with a submission key. A job with one attempt before migration 0006 and one after
  can still have two rows, the first with no key.
- A repeated provider call no longer leaves a row of its own, so tests show a repeat differently:
  one row in `job_attempts` per attempt, one row in `fake_notifications`, and the job's result
  naming that row's id.
- This assumes the receiver remembers a key for as long as a job may still call it. The fake
  provider keeps keys forever. A real provider typically forgets them after a retention window.
  That window must cover at least `max_attempts` × (`retry_max_delay_s` + the lease), plus phase
  8's recovery delay, and in fact more: a job also waits for the dispatcher or a worker, and a
  backlog or an outage has no bound. Swapping in a real provider needs its window checked against
  these settings and against how long an outage may last.
- This works because the receiver takes a key. A future handler whose receiver has no such key
  can only choose between two imperfect guards (see the alternatives): at-least-once with a
  narrower window, or at-most-once. Each such handler needs its own decision.
- Phase 8's recovery can retry a crashed attempt's job without a second notification.

## Alternatives considered

- **A separate `idempotency_keys` table** (key, fingerprint, job ID, expiry). It pays off once
  keys expire or cover several endpoints; here they do neither, and it adds a join and a second
  row per submission. Rejected for now.
- **Catching the unique violation (SQLSTATE 23505) instead of `ON CONFLICT`.** It needs a
  savepoint or a second transaction to read the existing row after the error, and the API's error
  mapping turns an uncaught 23505 into a 500 logged at ERROR, which a forgotten `except` would
  then produce. `ON CONFLICT DO NOTHING` says the same thing without an error. Rejected.
- **An advisory lock on the key's hash.** It serializes requests with one key, which the unique
  constraint already does, so it adds a second mechanism for the same guarantee. Rejected.
- **Replaying the first response (202 and its body).** It needs the response stored per key, and
  its `status` would be stale (`queued` for a job long finished). Rejected.
- **409 for a request whose key is in use by an unfinished request**, as the draft suggests. It
  makes the client retry for a window that lasts one `INSERT` and its COMMIT; waiting gives the
  final answer at once (decision 5). Rejected.
- **Parsing the header as a Structured Field String**, as the draft defines it. It would accept
  only quoted values, would reject the plain keys many APIs use, and adds a parser for no
  behaviour a client can see, since a quoted key without spaces already works as an opaque value.
  Rejected.
- **Fingerprinting the raw body, or the validated model's dump.** The raw body differs with
  whitespace and key order; the model's dump changes when a payload model gains a default field.
  Both turn an honest retry into a 422. Rejected.
- **Requiring the header.** Safer, but it changes the API contract and every existing test for a
  protection a client can opt into. Rejected; the draft's 400 applies only where a resource
  documents the header as required (section 2.7).
- **A guard on our side instead of the provider's key.** A table of "effects done", checked before
  calling the provider, cannot be made atomic with a call outside the database:
  - written *after* the call, a crash between the call and the write repeats the call on the next
    attempt (at-least-once, with a smaller window);
  - written *before* the call, a crash between the write and the call skips it on the next attempt,
    and the effect is lost (at-most-once).

  Only the receiver can make the effect happen once, given a key, or the effect must be
  idempotent in itself (for example, setting a value instead of adding to it). Rejected for
  `notification.send`; it remains the fallback for a receiver without keys.
- **Recording the fake call in the job's own finishing transaction.** It would make the call and
  the finish atomic, and so remove every duplicate, but only because the fake provider happens to
  live in our database. A real provider does not, so the fake would stop showing the problem it
  exists to show (ADR 0004). Rejected.
- **Keying the provider call per attempt** (`{job_id}:{attempt}`). It still sends once per retry,
  which is the repeat this ADR exists to remove. Rejected.
