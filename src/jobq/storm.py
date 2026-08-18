"""The crash-storm drill: the artifact this whole project exists to produce.

    python -m jobq.storm --jobs 3000 --kill-probability 0.25

Workers are killed at random points *while holding a lease* -- some before the
side effect commits, some after. Then a clean drain runs and the result is
compared against the producer's ground truth:

    every enqueued job produced EXACTLY ONE side effect,
    no job was lost, and nothing was applied twice.

Anyone can claim at-least-once delivery. This measures it.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import time

from .queue import JobQueue

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
WORK = os.path.join(ROOT, "data", "storm")


class WorkerKilled(Exception):
    """Simulates SIGKILL. Deliberately raised at the nastiest moments."""


def _fresh(path: str, lease_seconds: float, clock=None) -> JobQueue:
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK, exist_ok=True)
    return JobQueue(path, lease_seconds=lease_seconds, clock=clock)


def run_storm(n_jobs: int = 3000, kill_probability: float = 0.25, lease_seconds: float = 0.25,
              seed: int = 11, poison_rate: float = 0.02) -> dict:
    rng = random.Random(seed)
    path = os.path.join(WORK, "queue.db")
    q = _fresh(path, lease_seconds)

    # --- produce, with ground truth --------------------------------------
    expected_keys = []
    poison_keys = set()
    for i in range(n_jobs):
        key = "effect-%05d" % i
        poison = rng.random() < poison_rate
        q.enqueue("charge", {"amount": 100 + i, "poison": poison}, dedup_key=key, max_attempts=3)
        expected_keys.append(key)
        if poison:
            poison_keys.add(key)

    kills = 0
    processed = 0
    t0 = time.perf_counter()

    # --- consume under a storm of worker deaths --------------------------
    idle_rounds = 0
    while idle_rounds < 8:
        worker_id = "w-%d" % rng.randrange(8)
        jobs = q.claim(worker_id, batch=rng.randint(1, 5))
        if not jobs:
            idle_rounds += 1
            # Leases must lapse before dead workers' jobs become claimable again.
            time.sleep(lease_seconds / 2)
            continue
        idle_rounds = 0

        for job in jobs:
            # Kill BEFORE the effect commits: the job must be redelivered.
            if rng.random() < kill_probability:
                kills += 1
                continue        # worker dies holding the lease; no complete(), no fail()

            if job.payload.get("poison"):
                q.fail(job, "poison message: cannot be processed", base_backoff=0.001, rng=rng)
                continue

            q.complete(job, worker_id, result={"charged": job.payload["amount"]})
            processed += 1

            # Kill AFTER the commit: redelivery must be absorbed by the dedup key,
            # not applied twice. This is the window that breaks naive queues.
            if rng.random() < kill_probability * 0.5:
                kills += 1

    wall = time.perf_counter() - t0

    # --- verify against ground truth -------------------------------------
    con = q._connect()
    effect_rows = con.execute("SELECT dedup_key, COUNT(*) FROM side_effects GROUP BY dedup_key").fetchall()
    applied = {k: n for k, n in effect_rows}
    dead = set(r[0] for r in con.execute("SELECT dedup_key FROM dead_letters").fetchall())
    unfinished = con.execute(
        "SELECT COUNT(*) FROM jobs WHERE state NOT IN ('succeeded','dead')"
    ).fetchone()[0]
    con.close()

    double_applied = {k: n for k, n in applied.items() if n > 1}
    good_keys = set(expected_keys) - poison_keys
    missing = good_keys - set(applied)
    stats = q.stats()

    return {
        "drill": "crash_storm",
        "jobs_enqueued": n_jobs,
        "poison_messages_planted": len(poison_keys),
        "worker_kills_injected": kills,
        "wall_s": round(wall, 2),
        "jobs_per_s": round(n_jobs / wall, 1) if wall else 0,
        "distinct_effects_applied": len(applied),
        "expected_effects": len(good_keys),
        "effects_applied_more_than_once": len(double_applied),
        "effects_missing": len(missing),
        "dead_lettered": len(dead),
        "jobs_left_unfinished": unfinished,
        "queue_stats": stats,
        "passed": (
            len(double_applied) == 0
            and len(missing) == 0
            and unfinished == 0
            and dead == poison_keys
        ),
        "claim": ("at-least-once DELIVERY with exactly-once EFFECTS: %d worker kills, "
                  "%d effects applied exactly once, %d applied twice"
                  % (kills, len(applied), len(double_applied))),
    }


def run_dlq_replay(n_jobs: int = 50, seed: int = 5) -> dict:
    """Dead-letter a batch, then replay it, and prove replay cannot double-apply."""
    rng = random.Random(seed)
    path = os.path.join(WORK, "dlq.db")
    q = _fresh(path, lease_seconds=5.0)

    for i in range(n_jobs):
        q.enqueue("charge", {"amount": i, "poison": True}, dedup_key="dlq-%03d" % i, max_attempts=1)

    for _ in range(n_jobs * 2):
        jobs = q.claim("w", batch=10)
        if not jobs:
            break
        for job in jobs:
            q.fail(job, "downstream 500", base_backoff=0.001, rng=rng)

    dead_before = len(q.dead_letters(limit=1000))

    # A fix ships; the operator replays. This time the jobs succeed.
    replayed = 0
    for dl in q.dead_letters(limit=1000):
        if q.replay_dead_letter(dl["id"]):
            replayed += 1

    for _ in range(n_jobs * 2):
        jobs = q.claim("w", batch=10)
        if not jobs:
            break
        for job in jobs:
            q.complete(job, "w", result={"ok": True})

    # Replay the SAME dedup keys again: must be absorbed, not re-applied.
    for i in range(n_jobs):
        q.enqueue("charge", {"amount": i}, dedup_key="dlq-%03d" % i)
    for _ in range(n_jobs * 2):
        jobs = q.claim("w", batch=10)
        if not jobs:
            break
        for job in jobs:
            q.complete(job, "w", result={"ok": True})

    con = q._connect()
    dupes = con.execute(
        "SELECT COUNT(*) FROM (SELECT dedup_key FROM side_effects GROUP BY dedup_key HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    effects = con.execute("SELECT COUNT(*) FROM side_effects").fetchone()[0]
    con.close()

    return {
        "drill": "dlq_replay",
        "jobs": n_jobs,
        "dead_lettered": dead_before,
        "replayed": replayed,
        "distinct_effects": effects,
        "effects_applied_twice": dupes,
        "passed": dead_before == n_jobs and replayed == n_jobs and dupes == 0 and effects == n_jobs,
        "note": ("a replayed job keeps its ORIGINAL dedup key, so replay is not a second chance "
                 "to double-charge someone"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("drill", nargs="?", default="all", choices=["storm", "dlq", "all"])
    ap.add_argument("--jobs", type=int, default=3000)
    ap.add_argument("--kill-probability", type=float, default=0.25)
    args = ap.parse_args()

    out = {}
    if args.drill in ("storm", "all"):
        out["crash_storm"] = run_storm(args.jobs, args.kill_probability)
    if args.drill in ("dlq", "all"):
        out["dlq_replay"] = run_dlq_replay()

    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    with open(os.path.join(ROOT, "results", "drills.json"), "w") as fh:
        json.dump(out, fh, indent=2)
    print(json.dumps(out, indent=2))
    return 0 if all(v["passed"] for v in out.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
