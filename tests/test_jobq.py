"""Tests for the delivery semantics."""
import random
import time

import pytest

from jobq.queue import JobQueue


@pytest.fixture
def q(tmp_path):
    return JobQueue(str(tmp_path / "q.db"), lease_seconds=1.0)


def test_claim_returns_the_job_once(q):
    q.enqueue("t", {"n": 1})
    assert len(q.claim("w1", batch=5)) == 1
    assert q.claim("w2", batch=5) == [], "a leased job must not be claimable by a second worker"


def test_expired_lease_makes_the_job_claimable_again(tmp_path):
    """Redelivery: a dead worker's job comes back after its lease lapses."""
    now = {"t": 1000.0}
    q = JobQueue(str(tmp_path / "q.db"), lease_seconds=30.0, clock=lambda: now["t"])
    q.enqueue("t", {"n": 1})
    first = q.claim("dead-worker")
    assert len(first) == 1
    assert q.claim("other") == []

    now["t"] += 31
    again = q.claim("other")
    assert len(again) == 1
    assert again[0].id == first[0].id
    assert again[0].attempts == 2, "redelivery must increment the attempt count"


def test_heartbeat_prevents_redelivery_of_a_long_job(tmp_path):
    """The 30s lease / 45s job problem, solved without a global timeout bump."""
    now = {"t": 1000.0}
    q = JobQueue(str(tmp_path / "q.db"), lease_seconds=30.0, clock=lambda: now["t"])
    q.enqueue("slow", {})
    job = q.claim("w1")[0]

    for _ in range(3):
        now["t"] += 20
        assert q.extend_lease(job.id, "w1")
        assert q.claim("w2") == [], "a heartbeating worker must keep its lease"

    now["t"] += 31
    assert len(q.claim("w2")) == 1, "once heartbeats stop, the job must be redelivered"


def test_heartbeat_from_the_wrong_worker_is_rejected(q):
    q.enqueue("t", {})
    job = q.claim("w1")[0]
    assert not q.extend_lease(job.id, "impostor")


def test_completing_twice_applies_the_effect_once(q):
    """The core guarantee, in isolation."""
    q.enqueue("charge", {"amount": 10}, dedup_key="k1")
    job = q.claim("w1")[0]
    assert q.complete(job, "w1", {"ok": 1}) == "applied"
    assert q.complete(job, "w1", {"ok": 1}) == "deduplicated"
    assert q.stats()["side_effects"] == 1


def test_redelivery_after_a_crash_does_not_double_apply(tmp_path):
    now = {"t": 1000.0}
    q = JobQueue(str(tmp_path / "q.db"), lease_seconds=10.0, clock=lambda: now["t"])
    q.enqueue("charge", {"amount": 10}, dedup_key="k1")

    job = q.claim("w1")[0]
    q.complete(job, "w1", {"ok": 1})       # effect committed...
    # ...then the worker dies before anything else. The job is already succeeded,
    # so it is not redelivered; but if it were, the dedup key would absorb it.
    now["t"] += 11
    assert q.claim("w2") == []
    assert q.stats()["side_effects"] == 1


def test_jobs_without_a_dedup_key_are_not_deduplicated(q):
    """Honest behaviour: no key means no exactly-once promise."""
    q.enqueue("notify", {"msg": "hi"})
    job = q.claim("w1")[0]
    assert q.complete(job, "w1") == "applied"
    assert q.stats()["side_effects"] == 0


def test_retry_uses_backoff_with_jitter(tmp_path):
    now = {"t": 1000.0}
    q = JobQueue(str(tmp_path / "q.db"), lease_seconds=5.0, clock=lambda: now["t"])
    delays = []
    for trial in range(30):
        jid = q.enqueue("t", {"i": trial}, max_attempts=5)
        job = q.claim("w")[0]
        q.fail(job, "boom", base_backoff=10.0, rng=random.Random(trial))
        con = q._connect()
        delays.append(con.execute("SELECT run_after FROM jobs WHERE id=?", (jid,)).fetchone()[0] - now["t"])
        con.close()
    assert len(set(round(d, 4) for d in delays)) > 5, "identical delays means jitter is missing"
    assert all(0 <= d <= 10.0 for d in delays)


def test_backoff_grows_with_attempts(tmp_path):
    now = {"t": 1000.0}
    q = JobQueue(str(tmp_path / "q.db"), lease_seconds=0.0, clock=lambda: now["t"])
    jid = q.enqueue("t", {}, max_attempts=10)
    caps = []

    class MaxRng:
        """Always take the full backoff, so the growth curve is observable."""
        def uniform(self, a, b):
            return b

    for _ in range(5):
        job = q.claim("w")[0]
        q.fail(job, "boom", base_backoff=1.0, rng=MaxRng())
        con = q._connect()
        run_after = con.execute("SELECT run_after FROM jobs WHERE id=?", (jid,)).fetchone()[0]
        con.close()
        caps.append(run_after - now["t"])
        # Advance past the backoff, or the next claim finds nothing to do.
        now["t"] = run_after
    assert caps == sorted(caps)
    assert caps[-1] > caps[0]


def test_backoff_is_capped(tmp_path):
    now = {"t": 1000.0}
    q = JobQueue(str(tmp_path / "q.db"), lease_seconds=0.0, clock=lambda: now["t"])
    q.enqueue("t", {}, max_attempts=50)

    class MaxRng:
        def uniform(self, a, b):
            return b

    last = 0
    for _ in range(20):
        job = q.claim("w")[0]
        q.fail(job, "boom", base_backoff=1.0, max_backoff=60.0, rng=MaxRng())
        con = q._connect()
        run_after = con.execute("SELECT run_after FROM jobs WHERE id=?", (job.id,)).fetchone()[0]
        con.close()
        last = run_after - now["t"]
        now["t"] = run_after
    # 2**19 seconds without a cap; the cap is what stops a retry landing in 2039.
    assert last <= 60.0


def test_poison_message_lands_in_the_dlq_after_max_attempts(q):
    q.enqueue("t", {}, dedup_key="p1", max_attempts=3)
    outcomes = []
    for _ in range(3):
        job = q.claim("w")[0]
        outcomes.append(q.fail(job, "always fails", base_backoff=0.001))
    assert outcomes == ["retry", "retry", "dead"]
    dl = q.dead_letters()
    assert len(dl) == 1 and dl[0]["attempts"] == 3 and dl[0]["last_error"] == "always fails"


def test_dead_letter_preserves_full_failure_context(q):
    q.enqueue("charge", {"amount": 42}, dedup_key="ctx", max_attempts=1)
    job = q.claim("w")[0]
    q.fail(job, "downstream 503", base_backoff=0.001)
    dl = q.dead_letters()[0]
    assert dl["payload"] == {"amount": 42}
    assert dl["dedup_key"] == "ctx"
    assert "503" in dl["last_error"]


def test_replay_keeps_the_original_dedup_key(q):
    q.enqueue("charge", {"amount": 1}, dedup_key="same-key", max_attempts=1)
    job = q.claim("w")[0]
    q.fail(job, "boom", base_backoff=0.001)
    new_id = q.replay_dead_letter(q.dead_letters()[0]["id"])
    assert new_id
    replayed = q.claim("w")[0]
    assert replayed.dedup_key == "same-key", "replay must not become a second chance to double-apply"


def test_priority_is_respected(q):
    q.enqueue("low", {}, priority=0)
    q.enqueue("high", {}, priority=10)
    assert q.claim("w")[0].job_type == "high"


def test_delayed_jobs_are_not_claimable_early(tmp_path):
    now = {"t": 1000.0}
    q = JobQueue(str(tmp_path / "q.db"), clock=lambda: now["t"])
    q.enqueue("later", {}, delay_seconds=60)
    assert q.claim("w") == []
    now["t"] += 61
    assert len(q.claim("w")) == 1


def test_no_two_workers_ever_hold_the_same_job(q):
    for i in range(50):
        q.enqueue("t", {"i": i})
    seen = []
    for w in range(10):
        seen.extend(j.id for j in q.claim("w%d" % w, batch=5))
    assert len(seen) == len(set(seen)) == 50


def test_stats_report_depth_and_inflight(q):
    for i in range(5):
        q.enqueue("t", {"i": i})
    q.claim("w", batch=2)
    s = q.stats()
    assert s["in_flight"] == 2
    assert s["queue_depth"] == 3


# --------------------------------------------------------------------------
# per-key FIFO
# --------------------------------------------------------------------------

def test_per_key_fifo_claims_in_order_and_one_at_a_time(q):
    for i in range(4):
        q.enqueue("t", {"i": i}, fifo_key="user-1")

    first = q.claim("w1", batch=10)
    assert len(first) == 1, "only one job per fifo_key may be in flight"
    assert first[0].payload["i"] == 0, "must be the oldest job for the key"

    assert q.claim("w2", batch=10) == [], "the key is busy; nothing else is claimable"

    q.complete(first[0], "w1")
    second = q.claim("w2", batch=10)
    assert len(second) == 1 and second[0].payload["i"] == 1


def test_different_fifo_keys_run_concurrently(q):
    q.enqueue("t", {"k": "a"}, fifo_key="a")
    q.enqueue("t", {"k": "b"}, fifo_key="b")
    claimed = q.claim("w1", batch=10)
    assert {j.payload["k"] for j in claimed} == {"a", "b"}, "distinct keys must not block each other"


def test_jobs_without_a_fifo_key_are_unconstrained(q):
    for i in range(5):
        q.enqueue("t", {"i": i})
    assert len(q.claim("w1", batch=5)) == 5


def test_one_batch_never_contains_two_jobs_of_the_same_key(q):
    """A worker handed two same-key jobs would process them concurrently."""
    for i in range(3):
        q.enqueue("t", {"i": i}, fifo_key="same")
    claimed = q.claim("w1", batch=3)
    assert len(claimed) == 1


def test_fifo_key_releases_after_a_failure(q):
    """A failed job must not wedge its key forever."""
    q.enqueue("t", {"i": 0}, fifo_key="k", max_attempts=5)
    q.enqueue("t", {"i": 1}, fifo_key="k")
    job = q.claim("w", batch=5)[0]
    q.fail(job, "boom", base_backoff=0.001)
    time.sleep(0.05)
    assert len(q.claim("w", batch=5)) >= 1, "the key must become claimable again"


# --------------------------------------------------------------------------
# operator CLI
# --------------------------------------------------------------------------

def test_cli_groups_dead_letters_by_normalised_error(tmp_path, capsys):
    from jobq.cli import main as cli_main

    db = str(tmp_path / "q.db")
    q = JobQueue(db, lease_seconds=5.0)
    for i in range(6):
        q.enqueue("charge", {"i": i}, dedup_key="k%d" % i, max_attempts=1)
    for i in range(6):
        job = q.claim("w")[0]
        # Two failure classes that differ only by a number -- they must group.
        err = "timeout after %ds" % (i + 1) if i < 4 else "invalid card"
        q.fail(job, err, base_backoff=0.001)

    assert cli_main(["--db", db, "dlq", "group"]) == 0
    out = capsys.readouterr().out
    assert "6 dead letters in 2 distinct failure classes" in out
    assert "timeout after Ns" in out


def test_cli_replay_dry_run_changes_nothing(tmp_path, capsys):
    from jobq.cli import main as cli_main

    db = str(tmp_path / "q.db")
    q = JobQueue(db, lease_seconds=5.0)
    q.enqueue("charge", {"i": 1}, dedup_key="k", max_attempts=1)
    job = q.claim("w")[0]
    q.fail(job, "boom", base_backoff=0.001)

    before = len(q.dead_letters())
    assert cli_main(["--db", db, "dlq", "replay", "--all", "--dry-run", "--rate", "0"]) == 0
    assert len(q.dead_letters()) == before, "a dry run must not consume dead letters"


def test_cli_replay_by_error_match(tmp_path):
    from jobq.cli import main as cli_main

    db = str(tmp_path / "q.db")
    q = JobQueue(db, lease_seconds=5.0)
    for i, err in enumerate(["downstream 503", "downstream 503", "bad payload"]):
        q.enqueue("charge", {"i": i}, dedup_key="k%d" % i, max_attempts=1)
        job = q.claim("w")[0]
        q.fail(job, err, base_backoff=0.001)

    assert cli_main(["--db", db, "dlq", "replay", "--match", "503", "--rate", "0"]) == 0
    remaining = q.dead_letters()
    assert len(remaining) == 1
    assert "bad payload" in remaining[0]["last_error"]


# --------------------------------------------------------------------------
# transactional outbox -- effects that leave the database
# --------------------------------------------------------------------------

def test_completion_and_outbound_intent_commit_together(q):
    from jobq.outbox import Outbox, complete_with_outbox

    ob = Outbox(q)
    q.enqueue("charge", {"amount": 10}, dedup_key="k1")
    job = q.claim("w")[0]
    assert complete_with_outbox(q, job, "w", {"ok": 1}, destination="payments",
                                outbound_payload={"amount": 10}) == "applied"

    assert q.stats()["side_effects"] == 1
    assert ob.stats()["pending"] == 1


def test_a_crash_before_commit_stages_nothing(q):
    """Neither the completion nor the intent may survive a failed transaction."""
    from jobq.outbox import Outbox, complete_with_outbox

    ob = Outbox(q)
    q.enqueue("charge", {"amount": 10}, dedup_key="k1")
    job = q.claim("w")[0]

    real_connect = q._connect

    class ExplodingConnection:
        def __init__(self, inner):
            self.inner = inner
            self.calls = 0

        def execute(self, sql, *a):
            self.calls += 1
            # Blow up at the final COMMIT, after everything is staged.
            if sql.strip().upper().startswith("COMMIT"):
                raise RuntimeError("process died before commit")
            return self.inner.execute(sql, *a)

        def close(self):
            self.inner.close()

    q._connect = lambda: ExplodingConnection(real_connect())
    with pytest.raises(RuntimeError):
        complete_with_outbox(q, job, "w", {"ok": 1}, destination="payments",
                             outbound_payload={"amount": 10})
    q._connect = real_connect

    assert q.stats()["side_effects"] == 0, "the effect must not exist"
    assert ob.stats()["total"] == 0, "the outbound intent must not exist either"


def test_relay_delivers_and_marks_delivered(q):
    from jobq.outbox import Outbox, complete_with_outbox

    ob = Outbox(q)
    for i in range(3):
        q.enqueue("charge", {"amount": i}, dedup_key="k%d" % i)
        job = q.claim("w")[0]
        complete_with_outbox(q, job, "w", destination="payments",
                             outbound_payload={"amount": i})

    seen = []
    counts = ob.relay_once(lambda e: seen.append(e["idempotency_key"]))
    assert counts["delivered"] == 3
    assert ob.stats()["pending"] == 0
    assert len(set(seen)) == 3, "each entry carries a distinct idempotency key"


def test_the_relay_passes_an_idempotency_key_to_the_remote_side(q):
    """At-least-once delivery is only safe if the far side can deduplicate."""
    from jobq.outbox import Outbox, complete_with_outbox

    ob = Outbox(q)
    q.enqueue("charge", {"amount": 5}, dedup_key="stable-key")
    job = q.claim("w")[0]
    complete_with_outbox(q, job, "w", destination="payments", outbound_payload={"amount": 5})

    captured = {}
    ob.relay_once(lambda e: captured.update(e))
    assert captured["idempotency_key"] == "stable-key"


def test_a_failing_delivery_retries_then_dead_letters(q):
    from jobq.outbox import Outbox, complete_with_outbox

    ob = Outbox(q)
    q.enqueue("charge", {"amount": 1}, dedup_key="doomed")
    job = q.claim("w")[0]
    complete_with_outbox(q, job, "w", destination="payments", outbound_payload={"amount": 1})
    # Force every attempt to be due immediately.
    con = q._connect()
    con.execute("UPDATE outbox SET max_attempts = 3")
    con.close()

    def always_fails(entry):
        raise ConnectionError("payment gateway down")

    outcomes = []
    for _ in range(3):
        con = q._connect()
        con.execute("UPDATE outbox SET next_attempt_at = 0 WHERE state='pending'")
        con.close()
        outcomes.append(ob.relay_once(always_fails))

    assert ob.stats()["dead"] == 1
    assert ob.stats()["pending"] == 0


def test_staging_the_same_key_twice_is_a_no_op(q):
    """The unique idempotency key is what stops a redelivered job double-staging."""
    from jobq.outbox import Outbox, complete_with_outbox

    ob = Outbox(q)
    q.enqueue("charge", {"amount": 7}, dedup_key="same")
    job = q.claim("w")[0]
    complete_with_outbox(q, job, "w", destination="payments", outbound_payload={"amount": 7})
    # A redelivery of the same job deduplicates and must not stage a second intent.
    assert complete_with_outbox(q, job, "w", destination="payments",
                                outbound_payload={"amount": 7}) == "deduplicated"
    assert ob.stats()["total"] == 1
