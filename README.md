# Device Event Pipeline: queue to Postgres

Simulated camera events (`motion`, `person_detected`, `tamper`, `offline`, ...) flow
**producer -> queue -> consumer -> Postgres**. The goal is a pipeline you can kill, flake, and
replay without losing or duplicating a row.

```
producer --(batches of 10, retry failed entries w/ jittered backoff)--> queue (SQS semantics)
queue --> consumer --(validate)--> ONE txn: INSERT ... ON CONFLICT (event_id) DO NOTHING --> delete msgs
                 |                                   (delete only AFTER commit)
                 +-- invalid msg --> dead-letter queue immediately
queue redrive policy (maxReceiveCount=5) --> DLQ for valid messages that never succeed
```

## Guarantees (and the mechanism behind each)

| Failure | What happens | Why it is safe |
|---|---|---|
| Producer send partially fails | only the failed entries are retried (backoff + full jitter) | no re-send of entries that succeeded |
| Producer re-sends after lost ack | duplicate messages reach the queue | `event_id` PRIMARY KEY + `ON CONFLICT DO NOTHING` |
| DB error before write | retried in-process, then left for redelivery | messages not deleted until commit |
| DB commit succeeds but ack is lost | retry re-sends rows already stored | replay is a no-op |
| Consumer dies between commit and delete | message redelivered after visibility timeout | replay is a no-op |
| Malformed message | dead-lettered immediately with a reason | retrying can never help; doesn't block the batch |
| Valid message that never succeeds | queue redrives to DLQ after 5 receives | alarm on DLQ depth (`infra/main.tf`) |

This is **at-least-once delivery + idempotent writes**, which gives exactly-once *effect* in the
table. It is not exactly-once delivery.

## Run it

```bash
python -m unittest discover -s tests -t . -v      # 19 tests, no dependencies
python bench.py                                    # local: SQLite + in-memory SQS-semantics queue

# real backends
docker compose up -d
pip install -r requirements.txt
python bench.py --store postgres --queue sqs --events 20000 --out results/postgres_sqs.json
```

## Measured results

**Environment: SQLite 3.45 (WAL, synchronous=FULL) + in-memory queue, 1 vCPU sandbox, Python 3.12.
These are NOT Postgres or SQS numbers.** 3 runs each; raw output in `results/`.

Correctness under simultaneous faults (100,000 events + 100 malformed messages, run 1 of 3):

| Injected | Count |
|---|---|
| Duplicate messages sent | 5,022 |
| Producer send failures | 5,160 |
| DB errors before write | 1,257 |
| DB errors after commit (lost ack) | 615 |
| Consumer crashes between commit and delete | 557 |

Result: **100,000 / 100,000 rows stored, 0 missing, 0 duplicates, DLQ = exactly the 100 malformed
messages.** 15,794 replayed rows were absorbed by `ON CONFLICT`. Same outcome in 3/3 runs, with 1
and with 4 concurrent consumers.

Throughput, no faults, 50,000 events, one consumer:

| Batch size | Events/sec (3 runs) | Batch latency p50 | Batch latency p99 |
|---|---|---|---|
| 1 | 4,512 - 5,326 | 0.15 ms | 0.55 - 0.83 ms |
| 10 | 14,535 - 15,483 | 0.48 - 0.50 ms | 5.8 - 6.2 ms |

Batching 10 messages per transaction is ~3x faster because one commit is amortized over 10 rows.
The absolute rates mean little (single core, local SQLite); the ratio and the correctness results
are the transferable findings.

## Not yet verified

- `PostgresStore` and the boto3 path of `SqsQueue` have not run against real Postgres / SQS / LocalStack.
  The SQS adapter is tested against a stub client that checks request shapes only.
- `infra/main.tf` has not been through `terraform validate`.
- Threads share a GIL, so the 4-consumer run shows concurrency safety, not CPU scaling.
