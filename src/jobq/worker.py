"""A real worker process: claims, heartbeats, drains on shutdown.

    python -m jobq.worker --db data/q.db --id w1

Two behaviours here that the in-process simulation could not exercise:

* **Graceful shutdown.** On SIGTERM the worker stops claiming NEW work, finishes
  what it holds, and exits. It does not drop in-flight jobs on the floor. Without
  this, every deploy costs one visibility-timeout of latency per in-flight job —
  correct, because the lease expires and the job is redelivered, but needlessly
  slow. Draining turns a 30-second recovery into a sub-second one.
* **Lease heartbeats.** A job that outlives its lease is renewed while it runs,
  so a genuinely dead worker is still detected within one lease period without
  raising the timeout for everyone.

SIGKILL is deliberately NOT handled — that is the point of the crash storm. A
worker that can clean up after SIGKILL is not testing anything.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import threading
import time

from .queue import Job, JobQueue


class Worker:
    def __init__(self, queue: JobQueue, worker_id: str, batch: int = 4,
                 heartbeat_every: float = 0.0, poll_sleep: float = 0.02):
        self.q = queue
        self.worker_id = worker_id
        self.batch = batch
        self.heartbeat_every = heartbeat_every or (queue.lease_seconds / 3.0)
        self.poll_sleep = poll_sleep
        self.draining = False
        self.stop_now = False
        self.processed = 0
        self.failed = 0
        self.deduplicated = 0
        self._current = None
        self._hb_thread = None

    def install_signal_handlers(self):
        def _drain(signum, frame):
            # Stop claiming, keep finishing. Idempotent: a second signal is a
            # request to stop immediately.
            if self.draining:
                self.stop_now = True
            self.draining = True

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _drain)
            except (ValueError, OSError):
                pass  # not on the main thread, or unsupported on this platform

    def _heartbeat_loop(self):
        while not self.stop_now:
            job = self._current
            if job is not None:
                self.q.extend_lease(job.id, self.worker_id)
            time.sleep(self.heartbeat_every)

    def handle(self, job: Job) -> dict:
        """Override point. The default simulates work and honours a poison flag."""
        payload = job.payload
        if payload.get("poison"):
            raise RuntimeError("poison message: cannot be processed")
        work_s = float(payload.get("work_s", 0.0))
        if work_s:
            time.sleep(work_s)
        return {"charged": payload.get("amount", 0)}

    def run(self, max_idle_rounds: int = 40, max_jobs: int | None = None) -> dict:
        self._hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._hb_thread.start()

        idle = 0
        t0 = time.perf_counter()
        while idle < max_idle_rounds and not self.stop_now:
            if self.draining:
                break                       # drain: finish nothing new, exit cleanly
            if max_jobs is not None and self.processed >= max_jobs:
                break

            jobs = self.q.claim(self.worker_id, batch=self.batch)
            if not jobs:
                idle += 1
                time.sleep(self.poll_sleep)
                continue
            idle = 0

            for job in jobs:
                self._current = job
                try:
                    result = self.handle(job)
                    outcome = self.q.complete(job, self.worker_id, result)
                    self.processed += 1
                    if outcome == "deduplicated":
                        self.deduplicated += 1
                except Exception as exc:
                    self.q.fail(job, "%s: %s" % (type(exc).__name__, exc), base_backoff=0.05)
                    self.failed += 1
                finally:
                    self._current = None

        self.stop_now = True
        return {
            "worker_id": self.worker_id,
            "processed": self.processed,
            "failed": self.failed,
            "deduplicated": self.deduplicated,
            "wall_s": time.perf_counter() - t0,
            "drained": self.draining,
        }


class SuicidalWorker(Worker):
    """A worker that kills its own process at random, mid-job.

    `os._exit` bypasses every cleanup path -- no atexit, no finally, no flushed
    buffers, no connection close. That is what makes it a real crash rather than
    a tidy shutdown, and it is what the crash storm needs in order to mean
    anything.
    """

    def __init__(self, *args, kill_probability: float = 0.15, seed: int = 0,
                 kill_after_effect: float = 0.5, **kwargs):
        super().__init__(*args, **kwargs)
        self.kill_probability = kill_probability
        self.kill_after_effect = kill_after_effect
        self.rng = random.Random(seed)

    def handle(self, job: Job) -> dict:
        # Kill BEFORE the effect commits: the job must be redelivered.
        if self.rng.random() < self.kill_probability:
            os._exit(137)
        return super().handle(job)

    def run(self, *args, **kwargs):
        original_complete = self.q.complete

        def complete_then_maybe_die(job, worker_id, result=None):
            outcome = original_complete(job, worker_id, result)
            # Kill AFTER the commit: redelivery must be absorbed by the dedup
            # key, not applied twice. This is the window that breaks naive queues.
            if self.rng.random() < self.kill_probability * self.kill_after_effect:
                os._exit(137)
            return outcome

        self.q.complete = complete_then_maybe_die
        return super().run(*args, **kwargs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--id", required=True)
    ap.add_argument("--batch", type=int, default=4)
    ap.add_argument("--lease", type=float, default=2.0)
    ap.add_argument("--max-idle-rounds", type=int, default=40)
    ap.add_argument("--suicidal", action="store_true")
    ap.add_argument("--kill-probability", type=float, default=0.15)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    q = JobQueue(args.db, lease_seconds=args.lease)
    cls = SuicidalWorker if args.suicidal else Worker
    kwargs = {"kill_probability": args.kill_probability, "seed": args.seed} if args.suicidal else {}
    worker = cls(q, args.id, batch=args.batch, **kwargs)
    worker.install_signal_handlers()

    stats = worker.run(max_idle_rounds=args.max_idle_rounds)
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(stats, fh)
    print(json.dumps(stats))
    return 0


if __name__ == "__main__":
    sys.exit(main())
