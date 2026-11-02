#!/usr/bin/env python3
"""Benchmarks for the device-event pipeline.

Local (no services needed; SQLite + in-memory SQS-semantics queue):
    python bench.py

Real backends (Postgres + SQS via LocalStack; see docker-compose.yml):
    python bench.py --store postgres --dsn postgresql://postgres:postgres@localhost:5432/events \
                    --queue sqs --sqs-endpoint http://localhost:4566 --events 20000

Two scenarios:
  throughput  one consumer, preloaded queue, no faults, batch size 1 vs 10
  chaos       every fault injected at once; verifies zero loss / zero duplicates
"""
import argparse
import json
import math
import os
import platform
import random
import sqlite3
import sys
import tempfile
import threading
import time

from pipeline.consumer import Consumer, ConsumerStats
from pipeline.faults import FlakyQueue, FlakyStore
from pipeline.models import POISON_BODIES, generate
from pipeline.msgqueue import InMemoryQueue, SqsQueue
from pipeline.producer import Producer
from pipeline.store import PostgresStore, SqliteStore


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, math.ceil(p / 100 * len(xs)) - 1)]


def make_store(a):
    if a.store == "sqlite":
        return SqliteStore(os.path.join(tempfile.mkdtemp(prefix="bench-"), "events.db"),
                           synchronous=a.sqlite_sync)
    s = PostgresStore(a.dsn)
    s.reset()
    return s


def make_queue(a, vt):
    if a.queue == "memory":
        return InMemoryQueue(visibility_timeout=vt, max_receive_count=5)
    import boto3
    c = boto3.client("sqs", region_name="us-east-1", endpoint_url=a.sqs_endpoint,
                     aws_access_key_id="test", aws_secret_access_key="test")
    tag = f"{int(time.time() * 1000)}-{random.randrange(10**6)}"
    dlq = c.create_queue(QueueName=f"evt-dlq-{tag}")["QueueUrl"]
    dlq_arn = c.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    url = c.create_queue(QueueName=f"evt-{tag}", Attributes={
        "VisibilityTimeout": str(max(1, math.ceil(vt))),
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"})})["QueueUrl"]
    return SqsQueue(url, dlq, client=c)


def preload(q, events, extra_bodies=()):
    res = Producer(q, sleep=lambda s: None).publish(events)
    if extra_bodies:
        inner = getattr(q, "inner", q)
        for i in range(0, len(extra_bodies), 10):
            inner.send_batch(list(extra_bodies[i:i + 10]))
    return res


def scenario_throughput(a, batch_size):
    q = make_queue(a, vt=30)
    store = make_store(a)
    events = list(generate(a.throughput_events, seed=42))
    preload(q, events)
    c = Consumer(q, store, batch_size=batch_size)
    t0 = time.perf_counter()
    c.run_until_drained()
    dt = time.perf_counter() - t0
    lat = [x * 1000 for x in c.stats.batch_latencies]
    ok = store.all_ids() == {e.event_id for e in events}
    return {
        "scenario": "throughput", "batch_size": batch_size, "events": len(events),
        "seconds": round(dt, 2), "events_per_sec": round(len(events) / dt),
        "batch_latency_ms": {"p50": round(pct(lat, 50), 2), "p95": round(pct(lat, 95), 2),
                             "p99": round(pct(lat, 99), 2)},
        "all_events_stored_exactly_once": ok and store.count() == len(events),
    }


def scenario_chaos(a, n_consumers):
    n = a.chaos_events
    vt = 0.05 if a.queue == "memory" else 2
    inner_q = make_queue(a, vt=vt)
    q = FlakyQueue(inner_q, send_fail_rate=0.05, dup_rate=0.05, seed=1)
    events = list(generate(n, seed=7))
    pub = Producer(q, sleep=lambda s: None).publish(events)
    dropped = {json.loads(b)["event_id"] for b in pub.dropped}
    expected = {e.event_id for e in events} - dropped
    poison = POISON_BODIES * 25                      # 100 malformed messages
    for i in range(0, len(poison), 10):
        inner_q.send_batch(poison[i:i + 10])

    store = FlakyStore(make_store(a), fail_before_rate=0.10, fail_after_rate=0.05, seed=2)
    consumers = []
    for i in range(n_consumers):
        rng = random.Random(1000 + i)
        consumers.append(Consumer(inner_q, store, batch_size=10, max_attempts=4,
                                  crash_after_commit=lambda r=rng: r.random() < 0.05, rng=rng))
    t0 = time.perf_counter()
    threads = [threading.Thread(target=c.run_until_drained) for c in consumers]
    [t.start() for t in threads]
    [t.join() for t in threads]
    dt = time.perf_counter() - t0

    total = ConsumerStats()
    [total.merge(c.stats) for c in consumers]
    have = store.all_ids()
    return {
        "scenario": "chaos", "consumers": n_consumers, "valid_events": n, "poison_messages": len(poison),
        "injected": {
            "producer_send_failures": q.injected_send_failures,
            "duplicate_messages_sent": q.injected_duplicates,
            "db_errors_before_write": store.injected_before,
            "db_errors_after_commit_lost_ack": store.injected_after,
            "consumer_crashes_after_commit": total.crashes,
        },
        "absorbed": {
            "producer_retries": pub.retries,
            "producer_dropped": len(pub.dropped),
            "db_retries_in_process": total.store_retries,
            "queue_redeliveries": getattr(inner_q, "redeliveries", None),
            "replayed_rows_skipped_by_on_conflict": total.duplicates_skipped,
        },
        "result": {
            "rows_in_db": len(have), "expected_rows": len(expected),
            "missing": len(expected - have), "unexpected_extra": len(have - expected),
            "dead_letter_queue_depth": inner_q.dlq_depth(),
            "seconds_to_drain": round(dt, 2),
        },
        "invariants_hold": (have == expected and store.count() == len(expected)
                            and inner_q.dlq_depth() == len(poison)),
    }


def save(path, results):
    with open(path, "w") as f:
        json.dump(results, f, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", choices=["sqlite", "postgres"], default="sqlite")
    ap.add_argument("--dsn", default="postgresql://postgres:postgres@localhost:5432/events")
    ap.add_argument("--queue", choices=["memory", "sqs"], default="memory")
    ap.add_argument("--sqs-endpoint", default="http://localhost:4566")
    ap.add_argument("--sqlite-sync", default="FULL", choices=["FULL", "NORMAL"])
    ap.add_argument("--throughput-events", type=int, default=20000)
    ap.add_argument("--chaos-events", type=int, default=100000)
    ap.add_argument("--events", type=int, help="shortcut: sets both event counts")
    ap.add_argument("--skip-throughput", action="store_true", help="run only the chaos scenarios")
    ap.add_argument("--out", default="results.json")
    a = ap.parse_args()
    if a.events:
        a.throughput_events = a.chaos_events = a.events

    env = {
        "store": a.store + (f" (sqlite {sqlite3.sqlite_version}, WAL, synchronous={a.sqlite_sync})"
                            if a.store == "sqlite" else ""),
        "queue": a.queue, "python": platform.python_version(), "cpus": os.cpu_count(),
        "platform": platform.platform(),
    }
    print(json.dumps(env), file=sys.stderr)
    results = {"environment": env, "runs": []}
    for bs in (() if a.skip_throughput else (1, 10)):
        r = scenario_throughput(a, bs)
        results["runs"].append(r)
        print(json.dumps(r), flush=True)
        save(a.out, results)
    for nc in (1, 4):
        r = scenario_chaos(a, nc)
        results["runs"].append(r)
        print(json.dumps(r), flush=True)
        save(a.out, results)
    if not all(r.get("invariants_hold", r.get("all_events_stored_exactly_once")) for r in results["runs"]):
        sys.exit("INVARIANT VIOLATION")


if __name__ == "__main__":
    main()
