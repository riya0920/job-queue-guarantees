"""Operator CLI. Written because operators need it at 3am, not because it demos well.

    python -m jobq.cli --db data/q.db stats
    python -m jobq.cli --db data/q.db dlq list --limit 20
    python -m jobq.cli --db data/q.db dlq group          # cluster by error
    python -m jobq.cli --db data/q.db dlq replay --all --rate 50
    python -m jobq.cli --db data/q.db dlq replay --id 7

The recovery sequence this exists to support, from docs/GUARANTEES.md:
  1. stop the bleeding (roll back) -- replaying into a broken consumer refills the DLQ
  2. `dlq group` -- 10K dead letters is rarely 10K problems, usually one or two
  3. replay a sample, confirm it succeeds
  4. `dlq replay --all --rate N` -- rate-limited, because 10K jobs hitting a
     just-recovered downstream is how you cause the second outage
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter

from .queue import JobQueue


def cmd_stats(q: JobQueue, args) -> int:
    s = q.stats()
    print(json.dumps(s, indent=2))
    return 0


def cmd_dlq_list(q: JobQueue, args) -> int:
    rows = q.dead_letters(limit=args.limit)
    if not rows:
        print("dead-letter queue is empty")
        return 0
    print("%-6s %-14s %-10s %s" % ("id", "type", "attempts", "last_error"))
    for r in rows:
        print("%-6s %-14s %-10s %s" % (r["id"], r["job_type"], r["attempts"],
                                       (r["last_error"] or "")[:70]))
    return 0


def cmd_dlq_group(q: JobQueue, args) -> int:
    """Cluster dead letters by error text.

    The single most useful view during an incident: it turns "10,000 dead
    letters" into "two problems", which is what actually decides the fix.
    """
    rows = q.dead_letters(limit=100_000)
    if not rows:
        print("dead-letter queue is empty")
        return 0
    # Normalise trailing ids/numbers so "timeout after 3s" and "timeout after 5s"
    # group together rather than looking like two distinct failures.
    import re

    def normalise(err: str) -> str:
        return re.sub(r"\d+", "N", (err or "unknown"))[:90]

    counts = Counter(normalise(r["last_error"]) for r in rows)
    total = sum(counts.values())
    print("%d dead letters in %d distinct failure classes\n" % (total, len(counts)))
    for err, n in counts.most_common(args.limit):
        print("%6d  (%5.1f%%)  %s" % (n, 100.0 * n / total, err))
    return 0


def cmd_dlq_replay(q: JobQueue, args) -> int:
    rows = q.dead_letters(limit=100_000)
    if args.id is not None:
        rows = [r for r in rows if r["id"] == args.id]
        if not rows:
            print("no dead letter with id %s" % args.id, file=sys.stderr)
            return 1
    elif args.match:
        rows = [r for r in rows if args.match in (r["last_error"] or "")]
    elif not args.all:
        print("specify --id, --match or --all", file=sys.stderr)
        return 1

    if args.limit:
        rows = rows[: args.limit]

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    replayed, skipped = 0, 0
    for r in rows:
        if args.dry_run:
            print("would replay dl=%s job_type=%s dedup_key=%s" % (r["id"], r["job_type"], r["dedup_key"]))
            replayed += 1
            continue
        new_id = q.replay_dead_letter(r["id"])
        if new_id:
            replayed += 1
        else:
            skipped += 1
        if interval:
            time.sleep(interval)

    print(json.dumps({
        "replayed": replayed,
        "skipped": skipped,
        "dry_run": args.dry_run,
        "rate_per_s": args.rate,
        "note": ("replayed jobs keep their ORIGINAL dedup key, so anything whose effect already "
                 "landed deduplicates instead of double-firing -- a rising dedup count during "
                 "replay is expected and is the safety net working"),
    }, indent=2))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="jobq")
    ap.add_argument("--db", required=True)
    ap.add_argument("--lease", type=float, default=30.0)
    sub = ap.add_subparsers(dest="command", required=True)

    sub.add_parser("stats")

    dlq = sub.add_parser("dlq")
    dlq_sub = dlq.add_subparsers(dest="dlq_command", required=True)

    p_list = dlq_sub.add_parser("list")
    p_list.add_argument("--limit", type=int, default=50)

    p_group = dlq_sub.add_parser("group")
    p_group.add_argument("--limit", type=int, default=20)

    p_replay = dlq_sub.add_parser("replay")
    p_replay.add_argument("--id", type=int, default=None)
    p_replay.add_argument("--match", default=None, help="replay only errors containing this text")
    p_replay.add_argument("--all", action="store_true")
    p_replay.add_argument("--limit", type=int, default=None)
    p_replay.add_argument("--rate", type=float, default=20.0,
                          help="replays per second; keeps a recovering downstream alive")
    p_replay.add_argument("--dry-run", action="store_true")

    args = ap.parse_args(argv)
    q = JobQueue(args.db, lease_seconds=args.lease)

    if args.command == "stats":
        return cmd_stats(q, args)
    if args.dlq_command == "list":
        return cmd_dlq_list(q, args)
    if args.dlq_command == "group":
        return cmd_dlq_group(q, args)
    return cmd_dlq_replay(q, args)


if __name__ == "__main__":
    sys.exit(main())
