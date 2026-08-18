# What this queue guarantees — and what it explicitly does not

Precision about non-guarantees is the point of this document. A queue that
promises everything is a queue nobody can reason about.

## Guaranteed

### At-least-once delivery
Every enqueued job is delivered to a worker at least once. If a worker dies
holding a lease, the job becomes claimable again as soon as the lease lapses and
its attempt counter increments.

### Exactly-once *effects* (for jobs with a dedup key)
The side effect and the job's completion are written in **one transaction**
against a table with a unique constraint on the dedup key. Die before the commit
and nothing happened; die after and the redelivery is absorbed. There is no
window that produces a double effect.

### At-most-one concurrent holder
Two workers never hold the same job simultaneously. On Postgres this is
`SELECT ... FOR UPDATE SKIP LOCKED`; here it is an equivalent immediate-transaction
claim. `test_no_two_workers_ever_hold_the_same_job` covers it.

### Bounded retries, then a dead letter
Exponential backoff with **full jitter**, capped, then the dead-letter queue with
the full payload, dedup key, attempt count and last error preserved.

### Replay safety
A replayed dead letter keeps its **original** dedup key. Replay is not a second
chance to double-charge someone.

---

## NOT guaranteed, deliberately

### Exactly-once delivery
**This does not exist and claiming it is a red flag.** A worker can always die in
the window between doing the work and recording that it did. The honest framing
is at-least-once delivery plus idempotent consumers, which yields exactly-once
*effects*. Saying "exactly-once delivery" unprompted is how you fail this
interview.

### Global ordering
Jobs are **not** processed in enqueue order, and global ordering is the wrong
default:

* it forces a single consumer, or coordination that costs more than the ordering
  is worth
* one slow job blocks every unrelated job behind it (head-of-line blocking)
* it is almost never what the business actually needs — usually the requirement
  is per-*entity* ordering ("this user's events in order"), not global

**Per-key FIFO** is the right primitive when ordering matters, and it is a
roadmap item rather than something claimed here.

### Exactly-once effects WITHOUT a dedup key
Jobs enqueued with no `dedup_key` get at-least-once delivery and nothing more.
`test_jobs_without_a_dedup_key_are_not_deduplicated` asserts this, because
silently pretending otherwise would be worse than not offering it.

### Effects outside the database
The guarantee comes from committing the effect **in the same transaction** as
the job state. An effect that leaves the database — sending an email, calling a
payment API, publishing to another queue — is not covered. Those need either:

* an idempotency key the remote side honours (Stripe-style), or
* the transactional outbox pattern: write the intent locally in the same
  transaction, then a separate relay delivers it at-least-once.

### Latency bounds
Nothing here promises when a job runs. `run_after` is a floor, not a schedule,
and backoff jitter means retry timing is deliberately imprecise.

### Fairness across job types
A flood of one job type will starve others. Priority is a coarse lever, not
weighted fair queueing.

---

## The interview questions this design is built to answer

**"Visibility timeout is 30s; a job takes 45s. What happens, and how do you fix
it without a global timeout bump?"**
The lease expires mid-work, the job is redelivered, and two workers run it
concurrently — the effect is still applied once thanks to the dedup key, but the
work is done twice. The fix is **lease extension**: the worker heartbeats while
it runs (`extend_lease`). Raising the global timeout instead would slow recovery
for *every* job to accommodate the slowest one.
Covered by `test_heartbeat_prevents_redelivery_of_a_long_job`.

**"Walk me through SKIP LOCKED at the row level. Where does it stop scaling?"**
The lock is acquired as part of the select, so contending workers skip locked
rows rather than blocking on them. It stops scaling when the *scan* to find
unlocked rows becomes the bottleneck: with many workers and a large backlog of
locked rows at the head of the index, each claim walks further before finding a
free row. Mitigations: claim in batches (amortise the scan), partition the queue
by type or shard key, or move to a broker with per-partition consumer offsets.

**"Why not just use SQS?"**
For most teams, **use SQS.** It is cheaper than operating this. Building it here
was to own and demonstrate the semantics; the conditions under which I would
genuinely build rather than buy are: the jobs are transactional with data already
in my database (this design gets exactly-once effects *for free* from that
co-location, which SQS cannot), or I need queue state queryable alongside
business data. Otherwise, buy.

**"Your DLQ has 10K messages after a bad deploy. Sequence your recovery."**
1. **Stop the bleeding** — roll back the deploy first. Replaying into a broken
   consumer just refills the DLQ.
2. **Classify** — group dead letters by `last_error`. 10K messages is rarely 10K
   problems; it is usually one or two.
3. **Verify the fix on a sample** — replay 10, confirm they succeed.
4. **Replay in batches with rate control** — 10K jobs hitting a
   just-recovered downstream is how you cause the second outage.
5. **Watch the dedup counter** — replayed jobs keep their original keys, so
   anything already applied deduplicates rather than double-firing. That counter
   rising is expected and is the proof the safety net worked.
