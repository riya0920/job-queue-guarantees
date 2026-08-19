"""Multi-process crash storm and the worker-scaling curve.

    python -m jobq.bench storm --jobs 2000 --workers 4
    python -m jobq.bench scaling --jobs 3000 --worker-counts 1 2 4 8

The storm here spawns **real OS processes that really die** (`os._exit(137)`,
bypassing every cleanup path), which is what the in-process simulation could not
do. The verification is unchanged: compare against the producer's manifest and
require zero lost effects and zero double effects.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time

from .queue import JobQueue

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
WORK = os.path.join(ROOT, "data", "bench")
RESULTS = os.path.join(ROOT, "results")


def _seed_jobs(q: JobQueue, n_jobs: int, poison_rate: float, work_s: float, rng) -> tuple:
    expected, poison = [], set()
    for i in range(n_jobs):
        key = "effect-%06d" % i
        is_poison = rng.random() < poison_rate
        q.enqueue("charge", {"amount": 100 + i, "poison": is_poison, "work_s": work_s},
                  dedup_key=key, max_attempts=3)
        expected.append(key)
        if is_poison:
            poison.add(key)
    return expected, poison


def _spawn_workers(db: str, n: int, lease: float, suicidal: bool, kill_probability: float,
                   seed0: int = 0) -> list:
    procs = []
    for i in range(n):
        cmd = [sys.executable, "-m", "jobq.worker", "--db", db, "--id", "w%d" % i,
               "--lease", str(lease), "--seed", str(seed0 + i * 977),
               "--max-idle-rounds", "60"]
        if suicidal:
            cmd += ["--suicidal", "--kill-probability", str(kill_probability)]
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(ROOT, "src")
        procs.append(subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL))
    return procs


def _drain_remaining(db: str, lease: float, rounds: int = 6) -> None:
    """Healthy workers finish whatever the crashed ones left behind."""
    for _ in range(rounds):
        procs = _spawn_workers(db, 2, lease, suicidal=False, kill_probability=0.0)
        for p in procs:
            p.wait()
        q = JobQueue(db, lease_seconds=lease)
        left = q.stats()["by_state"].get("ready", 0) + q.stats()["by_state"].get("running", 0)
        if left == 0:
            return
        time.sleep(lease)     # let any stale leases lapse before the next round


def run_storm(n_jobs: int, n_workers: int, kill_probability: float, poison_rate: float,
              lease: float, seed: int) -> dict:
    import random

    rng = random.Random(seed)
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK, exist_ok=True)
    db = os.path.join(WORK, "storm.db")

    q = JobQueue(db, lease_seconds=lease)
    expected, poison = _seed_jobs(q, n_jobs, poison_rate, work_s=0.0, rng=rng)

    t0 = time.perf_counter()
    procs = _spawn_workers(db, n_workers, lease, suicidal=True, kill_probability=kill_probability)
    exit_codes = [p.wait() for p in procs]
    killed = sum(1 for c in exit_codes if c == 137)

    _drain_remaining(db, lease)
    wall = time.perf_counter() - t0

    con = q._connect()
    applied = dict(con.execute(
        "SELECT dedup_key, COUNT(*) FROM side_effects GROUP BY dedup_key").fetchall())
    dead = set(r[0] for r in con.execute("SELECT dedup_key FROM dead_letters").fetchall())
    unfinished = con.execute(
        "SELECT COUNT(*) FROM jobs WHERE state NOT IN ('succeeded','dead')").fetchone()[0]
    con.close()

    good = set(expected) - poison
    double = {k: v for k, v in applied.items() if v > 1}
    missing = good - set(applied)

    return {
        "drill": "multiprocess_crash_storm",
        "jobs_enqueued": n_jobs,
        "worker_processes": n_workers,
        "processes_killed_by_os_exit": killed,
        "kill_probability": kill_probability,
        "poison_planted": len(poison),
        "wall_s": round(wall, 2),
        "effects_applied": len(applied),
        "expected_effects": len(good),
        "effects_applied_twice": len(double),
        "effects_missing": len(missing),
        "dead_lettered": len(dead),
        "jobs_unfinished": unfinished,
        "passed": len(double) == 0 and len(missing) == 0 and unfinished == 0,
        "claim": ("real OS processes killed with os._exit(137), bypassing every cleanup path: "
                  "%d effects applied exactly once, %d twice, %d lost"
                  % (len(applied), len(double), len(missing))),
    }


def run_scaling(n_jobs: int, worker_counts: list, lease: float, work_s: float, seed: int) -> dict:
    import random

    rows = []
    for n_workers in worker_counts:
        rng = random.Random(seed)
        if os.path.exists(WORK):
            shutil.rmtree(WORK)
        os.makedirs(WORK, exist_ok=True)
        db = os.path.join(WORK, "scale_%d.db" % n_workers)

        q = JobQueue(db, lease_seconds=lease)
        _seed_jobs(q, n_jobs, poison_rate=0.0, work_s=work_s, rng=rng)

        t0 = time.perf_counter()
        procs = _spawn_workers(db, n_workers, lease, suicidal=False, kill_probability=0.0)
        for p in procs:
            p.wait()
        wall = time.perf_counter() - t0

        done = q.stats()["by_state"].get("succeeded", 0)
        rows.append({"workers": n_workers, "jobs_completed": done, "wall_s": round(wall, 2),
                     "jobs_per_s": round(done / wall, 1) if wall else 0.0})
        print("  workers=%d  %d jobs in %.2fs  = %.1f jobs/s"
              % (n_workers, done, wall, rows[-1]["jobs_per_s"]))

    base = rows[0]["jobs_per_s"]
    for r in rows:
        r["speedup"] = round(r["jobs_per_s"] / base, 2) if base else 0.0
        r["ideal"] = r["workers"] / rows[0]["workers"]
        r["efficiency"] = round(r["speedup"] / r["ideal"], 3) if r["ideal"] else 0.0

    return {
        "benchmark": "worker_scaling",
        "jobs_per_run": n_jobs,
        "per_job_work_s": work_s,
        "lease_s": lease,
        "hardware": {"platform": platform.platform(),
                     "processor": platform.processor() or platform.machine(),
                     "cpu_count": os.cpu_count()},
        "rows": rows,
        "caveat": ("SQLite serialises claims through a single write lock, so this curve measures "
                   "that contention as much as it measures parallelism. Postgres SKIP LOCKED lets "
                   "N workers claim different rows simultaneously and would change the shape."),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["storm", "scaling"])
    ap.add_argument("--jobs", type=int, default=2000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--worker-counts", type=int, nargs="+", default=[1, 2, 4, 8])
    ap.add_argument("--kill-probability", type=float, default=0.15)
    ap.add_argument("--poison-rate", type=float, default=0.02)
    ap.add_argument("--lease", type=float, default=1.5)
    ap.add_argument("--work-s", type=float, default=0.002)
    ap.add_argument("--seed", type=int, default=11)
    args = ap.parse_args()

    os.makedirs(RESULTS, exist_ok=True)
    if args.mode == "storm":
        out = run_storm(args.jobs, args.workers, args.kill_probability, args.poison_rate,
                        args.lease, args.seed)
        path = os.path.join(RESULTS, "multiprocess_storm.json")
    else:
        out = run_scaling(args.jobs, args.worker_counts, args.lease, args.work_s, args.seed)
        path = os.path.join(RESULTS, "worker_scaling.json")

    with open(path, "w") as fh:
        json.dump(out, fh, indent=2)
    print(json.dumps(out, indent=2))
    print("\nwrote", path)
    return 0 if out.get("passed", True) else 1


if __name__ == "__main__":
    sys.exit(main())
