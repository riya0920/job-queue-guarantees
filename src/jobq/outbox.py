"""Transactional outbox — the one gap docs/GUARANTEES.md names as uncovered.

The queue's exactly-once-effects guarantee comes from writing the effect and the
job's completion in a **single database transaction**. That works precisely
because both live in the same database. It stops working the moment an effect
leaves it — sending an email, charging a card, publishing to another queue —
because there is no transaction spanning your database and someone else's API.

The two naive orderings both fail:

    call the API, then commit    -> a crash in between re-calls the API on retry
    commit, then call the API    -> a crash in between LOSES the call entirely

The outbox pattern removes the choice. Inside the job's transaction you write the
*intent* to a local `outbox` table — that is a plain database write, so it is
covered by the same transaction that makes the job idempotent. A separate relay
then reads the outbox and performs the real call, at-least-once, marking each
entry delivered.

**The guarantee this actually buys, stated precisely:** the effect is *recorded*
exactly once and *delivered* at-least-once. The remote side still needs to be
idempotent — which is why every real payment API asks for an idempotency key.
The outbox does not make a non-idempotent remote call safe; it makes the
*decision to call* durable and non-duplicated, which is the half you control.
"""
from __future__ import annotations

import json
import random
import time

OUTBOX_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    -- The idempotency key handed to the REMOTE system. Distinct from the job's
    -- dedup_key: one job may emit several outbound effects.
    idempotency_key TEXT NOT NULL UNIQUE,
    job_id        INTEGER NOT NULL,
    destination   TEXT NOT NULL,
    payload       TEXT NOT NULL,
    state         TEXT NOT NULL DEFAULT 'pending',   -- pending|delivered|dead
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 5,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    REAL NOT NULL,
    delivered_at  REAL
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(state, next_attempt_at);
"""


class Outbox:
    """Reads and writes the outbox table on an existing JobQueue database."""

    def __init__(self, queue):
        self.q = queue
        con = queue._connect()
        try:
            con.executescript(OUTBOX_SCHEMA)
        finally:
            con.close()

    # -- writing (inside the job's transaction) ----------------------------

    @staticmethod
    def stage(cursor, job_id: int, destination: str, payload: dict,
              idempotency_key: str, now: float, max_attempts: int = 5):
        """Write an intent using a cursor ALREADY inside the caller's transaction.

        This takes a cursor rather than opening its own connection, and that is
        the entire point: the intent must commit atomically with the job state.
        Opening a second connection here would reintroduce exactly the two-phase
        problem the outbox exists to remove.
        """
        cursor.execute(
            "INSERT OR IGNORE INTO outbox (idempotency_key, job_id, destination, payload,"
            " state, max_attempts, next_attempt_at, created_at)"
            " VALUES (?,?,?,?,'pending',?,?,?)",
            (idempotency_key, job_id, destination, json.dumps(payload), max_attempts, now, now),
        )

    # -- relaying (separate, at-least-once) --------------------------------

    def claim_batch(self, limit: int = 10, now: float = None) -> list:
        now = now if now is not None else self.q._clock()
        con = self.q._connect()
        try:
            rows = con.execute(
                "SELECT id, idempotency_key, destination, payload, attempts FROM outbox"
                " WHERE state='pending' AND next_attempt_at <= ? ORDER BY id LIMIT ?",
                (now, limit),
            ).fetchall()
            return [{"id": r[0], "idempotency_key": r[1], "destination": r[2],
                     "payload": json.loads(r[3]), "attempts": r[4]} for r in rows]
        finally:
            con.close()

    def mark_delivered(self, entry_id: int, now: float = None):
        now = now if now is not None else self.q._clock()
        con = self.q._connect()
        try:
            con.execute("UPDATE outbox SET state='delivered', delivered_at=? WHERE id=?",
                        (now, entry_id))
        finally:
            con.close()

    def mark_failed(self, entry_id: int, error: str, base_backoff: float = 1.0,
                    rng=None, now: float = None):
        """Retry with jitter, or dead-letter after max_attempts."""
        rng = rng or random
        now = now if now is not None else self.q._clock()
        con = self.q._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT attempts, max_attempts FROM outbox WHERE id=?",
                              (entry_id,)).fetchone()
            attempts = row[0] + 1
            if attempts >= row[1]:
                con.execute("UPDATE outbox SET state='dead', attempts=?, last_error=? WHERE id=?",
                            (attempts, error, entry_id))
                con.execute("COMMIT")
                return "dead"
            delay = rng.uniform(0, min(base_backoff * (2 ** (attempts - 1)), 300.0))
            con.execute(
                "UPDATE outbox SET attempts=?, last_error=?, next_attempt_at=? WHERE id=?",
                (attempts, error, now + delay, entry_id),
            )
            con.execute("COMMIT")
            return "retry"
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def relay_once(self, deliver, limit: int = 10, rng=None) -> dict:
        """Deliver one batch. `deliver(entry) -> None` raises to signal failure.

        `deliver` receives the idempotency key and is expected to pass it to the
        remote system. That is what makes redelivery safe on the far side -- the
        outbox guarantees at-least-once delivery, and the key is what turns that
        into at-most-once *effect* remotely.
        """
        counts = {"delivered": 0, "retried": 0, "dead": 0}
        for entry in self.claim_batch(limit):
            try:
                deliver(entry)
                self.mark_delivered(entry["id"])
                counts["delivered"] += 1
            except Exception as exc:
                outcome = self.mark_failed(entry["id"], "%s: %s" % (type(exc).__name__, exc),
                                           rng=rng)
                counts["dead" if outcome == "dead" else "retried"] += 1
        return counts

    def stats(self) -> dict:
        con = self.q._connect()
        try:
            by_state = dict(con.execute(
                "SELECT state, COUNT(*) FROM outbox GROUP BY state").fetchall())
            return {
                "by_state": by_state,
                "pending": by_state.get("pending", 0),
                "delivered": by_state.get("delivered", 0),
                "dead": by_state.get("dead", 0),
                "total": sum(by_state.values()),
            }
        finally:
            con.close()


def complete_with_outbox(queue, job, worker_id: str, result=None, *, destination: str,
                         outbound_payload: dict, idempotency_key: str = None) -> str:
    """Complete a job AND stage its outbound effect in one transaction.

    This is the whole pattern in one function. If the process dies before the
    commit, neither the completion nor the intent exists and the job is
    redelivered. If it dies after, both exist and the relay will deliver. There
    is no ordering in which the API is called but the job is not recorded, or
    vice versa.
    """
    now = queue._clock()
    key = idempotency_key or (job.dedup_key or "job-%d" % job.id)
    con = queue._connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        outcome = "applied"
        if job.dedup_key is not None:
            existing = con.execute("SELECT 1 FROM side_effects WHERE dedup_key=?",
                                   (job.dedup_key,)).fetchone()
            if existing:
                outcome = "deduplicated"
            else:
                con.execute(
                    "INSERT INTO side_effects (dedup_key, job_id, job_type, result, created_at)"
                    " VALUES (?,?,?,?,?)",
                    (job.dedup_key, job.id, job.job_type, json.dumps(result), now),
                )

        if outcome == "applied":
            # Same transaction. This is the load-bearing line.
            Outbox.stage(con, job.id, destination, outbound_payload, key, now)

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
