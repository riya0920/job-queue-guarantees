# Distributed Job Queue with Named Delivery Guarantees

At-least-once delivery with **exactly-once effects**, proven by a crash storm that
kills workers at the worst possible moments and checks the result against the
producer's ground truth.

> **Status: ~40% built.** The queue core, leases and heartbeats, jittered retries,
> the dead-letter queue with replay, and both correctness drills are done and
> **verified**. The Postgres backend, Grafana dashboards and the worker-scaling
> curve are not — see [Roadmap](#roadmap).

## The claim, stated precisely

**At-least-once DELIVERY. Exactly-once EFFECTS.**

"Exactly-once delivery" does not exist — a worker can always die between doing
the work and recording that it did. Saying it unprompted is a red flag, so this
project says the accurate thing instead, and then proves it:

```
$ make storm

jobs_enqueued                    1500
poison_messages_planted            24
worker_kills_injected             699
distinct_effects_applied         1476
expected_effects                 1476
effects_applied_more_than_once      0    <-- no double charges
effects_missing                     0    <-- nothing lost
jobs_left_unfinished                0
dead_lettered                      24    <-- exactly the poison messages
passed                           true
```

Workers are killed at **both** dangerous moments: before the effect commits (the
job must come back) and after it commits (redelivery must be absorbed, not
re-applied). 699 kills against 1,500 jobs.

## Why it holds: one transaction

```python
con.execute("BEGIN IMMEDIATE")
#   INSERT INTO side_effects (dedup_key, ...)   -- the effect
#   UPDATE jobs SET state='succeeded' ...       -- the record that it happened
con.execute("COMMIT")
```

Die before the commit: nothing happened, and the lease lapses so the job is
redelivered. Die after: the dedup key is present, so redelivery is a no-op. There
is no window in between.

This is also exactly why the guarantee **stops at the database boundary** — an
effect that leaves it (an email, a payment API call) is not covered and needs its
own idempotency key or a transactional outbox. That limit is written down in
[docs/GUARANTEES.md](docs/GUARANTEES.md) rather than glossed over.

## What is deliberately NOT guaranteed

The section worth reading is the **[non-guarantees](docs/GUARANTEES.md#not-guaranteed-deliberately)**:

* **No global ordering**, and it is the wrong default — it forces a single
  consumer, creates head-of-line blocking, and is almost never what the business
  needs (per-*entity* ordering usually is).
* **No exactly-once effects without a dedup key.** A test asserts this rather
  than letting it be assumed.
* **No latency bounds.** `run_after` is a floor, not a schedule.
* **No fairness across job types.** Priority is a coarse lever, not weighted fair
  queueing.

## The 30-second lease and the 45-second job

The classic question, and the wrong answer is raising the global visibility
timeout — that slows recovery for *every* job to accommodate the slowest one.

The right answer is **lease extension**: a long job heartbeats while it works, so
a dead worker is still detected within one lease period.
`test_heartbeat_prevents_redelivery_of_a_long_job` holds a lease across 60
seconds of a 30-second lease by heartbeating, then confirms the job *is*
redelivered once the heartbeats stop.

## Retries: exponential backoff with full jitter

Jitter is not cosmetic. When a downstream dependency fails, every in-flight job
fails at roughly the same instant — and without jitter they all retry at the same
instant too, re-killing the dependency the moment it recovers. Full jitter
(uniform over `[0, backoff]`) spreads them out.

`test_retry_uses_backoff_with_jitter` fails if 30 retries produce fewer than 5
distinct delays, which is what a missing-jitter implementation looks like.
Backoff is also **capped** — uncapped doubling puts attempt 20 somewhere in 2039.

## DLQ replay

Operators need this at 3am, and it has one property that matters: a replayed dead
letter **keeps its original dedup key**, so replay is not a second chance to
double-charge someone. The `dlq_replay` drill dead-letters 50 jobs, replays all
50, then re-enqueues the same keys a third time and asserts the effect count is
still exactly 50.

## Run it

```bash
pip install -r requirements.txt      # pytest. That is the whole dependency list.
make test                            # 17 tests
make storm                           # the crash storm + DLQ replay drills
```

## About the backend

The runnable path is **SQLite**; `PG_CLAIM_SQL` in `queue.py` is the Postgres
`SELECT ... FOR UPDATE SKIP LOCKED` statement kept as the reference
implementation.

**What differs:** SKIP LOCKED lets N workers claim *different* rows
simultaneously, while the SQLite immediate-transaction claim serialises claims.
That is a **throughput** difference, not a correctness one — the invariant (no
two workers hold the same job) holds either way, which is why the drills are
meaningful on SQLite. But it does mean the jobs/sec figure below is a floor set
by claim serialisation, not a measure of the design.

## Roadmap (the remaining ~60%)

| Milestone | Status |
|---|---|
| Queue core: claim, lease, complete, fail | done |
| Exactly-once effects via dedup key + one transaction | done |
| Lease extension (heartbeats) | done |
| Exponential backoff with full jitter, capped | done |
| DLQ with full failure context + replay | done |
| Priorities and delayed jobs | done |
| Crash-storm drill vs ground truth | done |
| DLQ replay drill | done |
| `docs/GUARANTEES.md` incl. non-guarantees | done |
| **Postgres backend (SKIP LOCKED SQL written, not wired)** | not started |
| **Real multi-process workers with SIGKILL (in-process simulation today)** | not started |
| **Graceful shutdown: drain in flight, do not drop** | not started |
| **Per-key FIFO as an optional mode** | not started |
| **Grafana: queue depth, in-flight, age-of-oldest, failure rate by type** | not started |
| **Worker-scaling curve at 1/2/4/8 workers on named hardware** | **not measured** |
| **DLQ inspection/replay CLI (library methods exist; no CLI)** | not started |

## Honesty notes

* **The `jobs_per_s` figure in the drill output is not a benchmark.** It is
  measured on a laptop against SQLite with artificial sleeps for lease expiry,
  and it is dominated by those sleeps. No throughput claim is made anywhere, and
  the worker-scaling curve has not been run.
* **Worker deaths are simulated in-process**, not by killing OS processes. The
  simulation exercises the same state transitions — a lease held by a worker that
  never returns — but it does not test OS-level partial writes. Real `SIGKILL`
  against separate processes is a roadmap item.
* Graceful shutdown is **not** implemented. A worker stopped mid-job today relies
  on lease expiry rather than draining, which is correct but slower than it needs
  to be.
