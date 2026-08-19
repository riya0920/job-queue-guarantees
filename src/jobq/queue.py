"""The queue core: leases, retries with jittered backoff, and a dead-letter queue.

**The guarantee, named precisely: at-least-once delivery with exactly-once
EFFECTS.** "Exactly-once delivery" is not a thing a queue can promise across a
network — a worker can always die in the window between doing the work and
recording that it did. What a queue *can* do is redeliver reliably and give
consumers the tools to make redelivery harmless. That is what this does, and
saying "exactly-once delivery" unprompted is the fastest way to fail the
interview this project is for.

Claiming uses `SELECT ... FOR UPDATE SKIP LOCKED` on Postgres: the row lock is
taken as part of the select, so two workers never see the same row, and a worker
blocked on someone else's row skips past it instead of queueing behind it. The
SQLite path takes an equivalent immediate-transaction claim so the whole thing
runs and is testable without a database server.

Ordering: **no global ordering is guaranteed, deliberately.** See docs/GUARANTEES.md.
"""
from __future__ import annotations

import json
import random
import sqlite3
import time
from dataclasses import dataclass

# --- Postgres claim (production) -----------------------------------------
# Kept here as the reference implementation even though the runnable path below
# is SQLite. FOR UPDATE SKIP LOCKED is the entire reason a Postgres-backed queue
# is respectable: the lock and the read are one operation, so there is no window
# in which two workers both believe they own a job.
PG_CLAIM_SQL = """
UPDATE jobs SET
    state        = 'running',
    attempts     = attempts + 1,
    lease_expires_at = NOW() + (%(lease_seconds)s || ' seconds')::INTERVAL,
    claimed_by   = %(worker_id)s
WHERE id IN (
    SELECT id FROM jobs
    WHERE state = 'ready'
      AND run_after <= NOW()
    ORDER BY priority DESC, run_after ASC, id ASC
    LIMIT %(batch)s
    FOR UPDATE SKIP LOCKED
)
RETURNING id, job_type, payload, attempts, dedup_key;
"""

TERMINAL_STATES = ("succeeded", "dead")


@dataclass
class Job:
    id: int
    job_type: str
    payload: dict
    attempts: int
    dedup_key: str | None


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type         TEXT NOT NULL,
    payload          TEXT NOT NULL,
    dedup_key        TEXT,
    -- Optional per-key ordering. When set, at most one job per fifo_key may be
    -- running at a time and they are claimed in enqueue order. This is the RIGHT
    -- ordering primitive: global ordering forces a single consumer and lets one
    -- slow job block every unrelated job, whereas per-entity ordering is what
    -- the business almost always actually means.
    fifo_key         TEXT,
    state            TEXT NOT NULL DEFAULT 'ready',   -- ready|running|succeeded|dead
    priority         INTEGER NOT NULL DEFAULT 0,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 5,
    run_after        REAL NOT NULL DEFAULT 0,
    lease_expires_at REAL,
    claimed_by       TEXT,
    last_error       TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);

-- The side-effect table. This is what turns at-least-once delivery into
-- exactly-once effects: the effect and its dedup key are written together, and
-- the unique constraint makes a replayed job a no-op instead of a double charge.
CREATE TABLE IF NOT EXISTS side_effects (
    dedup_key   TEXT PRIMARY KEY,
    job_id      INTEGER NOT NULL,
    job_type    TEXT NOT NULL,
    result      TEXT,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      INTEGER NOT NULL,
    job_type    TEXT NOT NULL,
    payload     TEXT NOT NULL,
    dedup_key   TEXT,
    attempts    INTEGER NOT NULL,
    last_error  TEXT,
    died_at     REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable ON jobs(state, run_after, priority);
CREATE INDEX IF NOT EXISTS idx_jobs_fifo ON jobs(fifo_key, state, id);
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs(state, lease_expires_at);
"""


class JobQueue:
    def __init__(self, path: str, lease_seconds: float = 30.0, clock=None):
        self.path = path
        self.lease_seconds = lease_seconds
        self._clock = clock or time.time
        con = self._connect()
        con.executescript(SCHEMA)
        con.commit()
        con.close()

    def _connect(self):
        con = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")
        return con

    # -- producing ---------------------------------------------------------

    def enqueue(self, job_type: str, payload: dict, dedup_key: str | None = None,
                priority: int = 0, delay_seconds: float = 0.0, max_attempts: int = 5,
                fifo_key: str | None = None) -> int:
        now = self._clock()
        con = self._connect()
        try:
            cur = con.execute(
                "INSERT INTO jobs (job_type, payload, dedup_key, fifo_key, state, priority,"
                " max_attempts, run_after, created_at, updated_at) VALUES (?,?,?,?,'ready',?,?,?,?,?)",
                (job_type, json.dumps(payload), dedup_key, fifo_key, priority, max_attempts,
                 now + delay_seconds, now, now),
            )
            return cur.lastrowid
        finally:
            con.close()

    # -- consuming ---------------------------------------------------------

    def claim(self, worker_id: str, batch: int = 1) -> list:
        """Claim up to `batch` runnable jobs and take a lease on them.

        On Postgres this is PG_CLAIM_SQL (one statement, SKIP LOCKED). Here the
        select and update run inside one IMMEDIATE transaction, which gives the
        same invariant -- no two workers hold the same job -- by taking the write
        lock up front. The difference is concurrency, not correctness: SKIP
        LOCKED lets N workers claim different rows simultaneously, while SQLite
        serialises the claims. That is a throughput ceiling and it is stated in
        the README rather than hidden.
        """
        now = self._clock()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")

            # Reclaim expired leases first: a job whose worker died is runnable
            # again once its lease lapses. This is the redelivery mechanism, and
            # doing it inside the claim transaction means no separate reaper
            # process can race with a claim.
            con.execute(
                "UPDATE jobs SET state='ready', claimed_by=NULL, lease_expires_at=NULL, updated_at=?"
                " WHERE state='running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?",
                (now, now),
            )

            # Per-key FIFO: a job with a fifo_key is claimable only if no other
            # job with the same key is running, and only if it is the OLDEST
            # ready job for that key. Jobs without a fifo_key are unconstrained,
            # so the ordering guarantee is opt-in and costs nothing by default.
            rows = con.execute(
                "SELECT j.id, j.job_type, j.payload, j.attempts, j.dedup_key FROM jobs j"
                " WHERE j.state='ready' AND j.run_after <= ?"
                "   AND (j.fifo_key IS NULL OR ("
                "        NOT EXISTS (SELECT 1 FROM jobs r"
                "                    WHERE r.fifo_key = j.fifo_key AND r.state='running')"
                "    AND j.id = (SELECT MIN(o.id) FROM jobs o"
                "                WHERE o.fifo_key = j.fifo_key AND o.state='ready'"
                "                  AND o.run_after <= ?)))"
                " ORDER BY j.priority DESC, j.run_after ASC, j.id ASC LIMIT ?",
                (now, now, batch),
            ).fetchall()

            # Within one batch, never hand the same worker two jobs sharing a
            # fifo_key -- it would process them concurrently and break the order
            # the key exists to guarantee.
            seen_keys = set()

            claimed = []
            for job_id, job_type, payload, attempts, dedup_key in rows:
                key = con.execute("SELECT fifo_key FROM jobs WHERE id=?", (job_id,)).fetchone()[0]
                if key is not None:
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                con.execute(
                    "UPDATE jobs SET state='running', attempts=attempts+1, claimed_by=?,"
                    " lease_expires_at=?, updated_at=? WHERE id=?",
                    (worker_id, now + self.lease_seconds, now, job_id),
                )
                claimed.append(Job(job_id, job_type, json.loads(payload), attempts + 1, dedup_key))
            con.execute("COMMIT")
            return claimed
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def extend_lease(self, job_id: int, worker_id: str, seconds: float | None = None) -> bool:
        """Heartbeat. The answer to "the lease is 30s and the job takes 45s".

        The wrong fix is raising the global visibility timeout, which slows
        recovery for every job to accommodate the slowest one. The right fix is
        for a long job to renew its own lease while it works, so a dead worker is
        still detected in one lease period.
        """
        now = self._clock()
        con = self._connect()
        try:
            cur = con.execute(
                "UPDATE jobs SET lease_expires_at=?, updated_at=?"
                " WHERE id=? AND claimed_by=? AND state='running'",
                (now + (seconds or self.lease_seconds), now, job_id, worker_id),
            )
            return cur.rowcount > 0
        finally:
            con.close()

    def complete(self, job: Job, worker_id: str, result=None) -> str:
        """Record the side effect and mark the job done, in ONE transaction.

        Returns "applied" or "deduplicated". This method is the whole guarantee:
        if the process dies before the commit, nothing happened and the job is
        redelivered; if it dies after, the dedup key is present and the redelivery
        is a no-op. There is no window that produces a double effect.
        """
        now = self._clock()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            outcome = "applied"
            if job.dedup_key is not None:
                existing = con.execute(
                    "SELECT 1 FROM side_effects WHERE dedup_key=?", (job.dedup_key,)
                ).fetchone()
                if existing:
                    outcome = "deduplicated"
                else:
                    con.execute(
                        "INSERT INTO side_effects (dedup_key, job_id, job_type, result, created_at)"
                        " VALUES (?,?,?,?,?)",
                        (job.dedup_key, job.id, job.job_type, json.dumps(result), now),
                    )
            con.execute(
                "UPDATE jobs SET state='succeeded', lease_expires_at=NULL, updated_at=? WHERE id=?",
                (now, job.id),
            )
            con.execute("COMMIT")
            return outcome
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def fail(self, job: Job, error: str, base_backoff: float = 1.0, max_backoff: float = 300.0,
             rng: random.Random | None = None) -> str:
        """Retry with exponential backoff and FULL JITTER, or dead-letter it.

        Jitter is not cosmetic. When a downstream dependency fails, every in-flight
        job fails at roughly the same instant; without jitter they all retry at
        exactly the same instant too, and the retry storm re-kills the dependency
        the moment it recovers. Full jitter (uniform over [0, backoff]) spreads
        them, at the cost of some jobs retrying sooner than the nominal backoff.
        """
        rng = rng or random
        now = self._clock()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT attempts, max_attempts FROM jobs WHERE id=?", (job.id,)).fetchone()
            attempts, max_attempts = row

            if attempts >= max_attempts:
                con.execute(
                    "INSERT INTO dead_letters (job_id, job_type, payload, dedup_key, attempts,"
                    " last_error, died_at) VALUES (?,?,?,?,?,?,?)",
                    (job.id, job.job_type, json.dumps(job.payload), job.dedup_key, attempts, error, now),
                )
                con.execute(
                    "UPDATE jobs SET state='dead', last_error=?, lease_expires_at=NULL, updated_at=?"
                    " WHERE id=?",
                    (error, now, job.id),
                )
                con.execute("COMMIT")
                return "dead"

            backoff = min(base_backoff * (2 ** (attempts - 1)), max_backoff)
            delay = rng.uniform(0, backoff)
            con.execute(
                "UPDATE jobs SET state='ready', claimed_by=NULL, lease_expires_at=NULL,"
                " run_after=?, last_error=?, updated_at=? WHERE id=?",
                (now + delay, error, now, job.id),
            )
            con.execute("COMMIT")
            return "retry"
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    # -- ops ---------------------------------------------------------------

    def stats(self) -> dict:
        now = self._clock()
        con = self._connect()
        try:
            by_state = dict(con.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state").fetchall())
            oldest = con.execute(
                "SELECT MIN(created_at) FROM jobs WHERE state='ready' AND run_after <= ?", (now,)
            ).fetchone()[0]
            return {
                "by_state": by_state,
                "queue_depth": by_state.get("ready", 0),
                "in_flight": by_state.get("running", 0),
                "dead_letters": con.execute("SELECT COUNT(*) FROM dead_letters").fetchone()[0],
                "side_effects": con.execute("SELECT COUNT(*) FROM side_effects").fetchone()[0],
                "age_of_oldest_ready_s": (now - oldest) if oldest else 0.0,
            }
        finally:
            con.close()

    def dead_letters(self, limit: int = 100) -> list:
        con = self._connect()
        try:
            return [
                {"id": r[0], "job_id": r[1], "job_type": r[2], "payload": json.loads(r[3]),
                 "dedup_key": r[4], "attempts": r[5], "last_error": r[6]}
                for r in con.execute(
                    "SELECT id, job_id, job_type, payload, dedup_key, attempts, last_error"
                    " FROM dead_letters ORDER BY died_at DESC LIMIT ?", (limit,)
                ).fetchall()
            ]
        finally:
            con.close()

    def replay_dead_letter(self, dl_id: int) -> int | None:
        """Re-enqueue a dead job. Operators need this at 3am after a bad deploy.

        The replayed job keeps its ORIGINAL dedup_key, so if the effect had
        actually landed before the failure, the replay is still a no-op. Replay
        must not be a second chance to double-charge someone.
        """
        now = self._clock()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT job_type, payload, dedup_key FROM dead_letters WHERE id=?", (dl_id,)
            ).fetchone()
            if row is None:
                con.execute("ROLLBACK")
                return None
            job_type, payload, dedup_key = row
            cur = con.execute(
                "INSERT INTO jobs (job_type, payload, dedup_key, state, priority, max_attempts,"
                " run_after, created_at, updated_at) VALUES (?,?,?,'ready',0,5,?,?,?)",
                (job_type, payload, dedup_key, now, now, now),
            )
            new_id = cur.lastrowid
            con.execute("DELETE FROM dead_letters WHERE id=?", (dl_id,))
            con.execute("COMMIT")
            return new_id
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()
