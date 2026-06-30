# 09 — Exactly-once and failure handling

## Why this matters

Streaming systems that crash and resume must produce *correct* output. "Exactly-once" is the gold standard — every input event produces exactly one output, no matter how many times the engine retries. This note explains what Spark guarantees, what it doesn't, and how to bridge the gap.

## The three semantics

| Guarantee | Meaning | Where it shows up |
|---|---|---|
| At-most-once | Each event is processed ≤ 1 time. May lose data. | Almost never wanted |
| At-least-once | Each event is processed ≥ 1 time. May duplicate. | Default for most sinks |
| Exactly-once | Each event is reflected in output exactly 1 time. | Achievable with idempotent sinks |

Spark guarantees:
- **Sources** are replayable for supported sources (Kafka, Delta, file): the same offsets can be re-read.
- **Computation** is deterministic given the same input.
- **Sinks** are the variable. Spark gives at-least-once unless the sink supports idempotency or transactions.

## Where the magic happens

```mermaid
sequenceDiagram
    participant E as Engine
    participant Src as Source
    participant Off as offsets/
    participant Sink
    participant Com as commits/
    E->>Src: Read offsets [start, end] for batch N
    E->>Off: WAL: persist (N, start, end)
    E->>E: Process batch N
    E->>Sink: Write batch N output (tagged with batch_id=N)
    Sink-->>E: ack
    E->>Com: WAL: mark batch N committed
```

Crash recovery:
- Crash **before** `Off` step → on restart, batch N hasn't happened. Replay.
- Crash **between** `Off` and `Sink` → on restart, Spark replays batch N. If sink is idempotent (dedup by `batch_id`), no duplicates.
- Crash **between** `Sink` and `Com` → on restart, Spark replays batch N. Same dedup logic applies.
- Crash **after** `Com` → batch N done; resume with batch N+1.

So exactly-once = at-least-once from Spark + idempotent sink.

[LS Ch.8 §"Fault Tolerance"]

## Sinks ranked by exactly-once support

| Sink | Native exactly-once | Mechanism |
|---|---|---|
| **Delta** | ✅ | `batch_id` recorded in commit log; duplicates rejected |
| **File** (Parquet etc.) | ✅ | `_spark_metadata/` records committed files |
| **Kafka (transactional)** | ✅ | Kafka transactions + Spark transactional sink config |
| **Kafka (default)** | ❌ at-least-once | No transactions; duplicates possible |
| **foreachBatch** | depends | You handle `batch_id` |
| **foreach** | ❌ at-least-once | Per-row, no batch tracking |
| **JDBC** | ❌ | At-least-once; rows can duplicate on retry |
| **console, memory** | N/A | Test sinks |

## Making any sink exactly-once

If the sink isn't natively exactly-once, you have two strategies:

### Strategy 1: idempotent keys

Make the sink reject duplicate rows. E.g. write to Postgres with `INSERT ... ON CONFLICT DO NOTHING` on a unique key like `(event_id)`. The retry inserts the same row twice; the database silently drops the second.

```python
def upsert_pg(batch_df, batch_id):
    batch_df.write.format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "events_staging") \
        .mode("append") \
        .save()
    # Then in Postgres, a job promotes staging to main with INSERT ON CONFLICT
```

Or stamp the row with `batch_id` and require `(batch_id, partition_offset)` to be unique.

### Strategy 2: transactional writes per batch_id

In `foreachBatch`, write using a sink-specific transaction that records `batch_id`:

```python
def write_idempotent(batch_df, batch_id):
    (batch_df.write
        .format("delta")
        .mode("append")
        .option("txnAppId", "streaming_silver_job")
        .option("txnVersion", str(batch_id))
        .save("/tables/silver"))
```

Delta records (txnAppId, txnVersion) in the commit log. A retry with the same pair is a no-op.

The same idea works in any database that lets you record "we've already processed batch N" atomically with the data write.

## Restart guarantees

When you restart a streaming query:

1. Spark reads the **last committed batch** from `commits/`.
2. Resumes from `offsets/` for the next batch.
3. State store is restored from the snapshot.
4. Watermark is restored.

**You should be able to restart at any time without data loss or duplication** if you've set up exactly-once correctly.

### What doesn't survive

- **Code changes** that alter the query plan in incompatible ways. Adding a new aggregation = new state = checkpoint incompatible.
- **`spark.sql.shuffle.partitions` changes** — state is sharded by this; changing it requires reseeding state.
- **Source schema changes** — depending on the source, may break.

For backwards-incompatible changes: start a new query with a new checkpoint, optionally backfill from history.

## Operational practices

### Always set
- `checkpointLocation` — non-negotiable.
- `query.exception()` — wrap in monitoring; raise alert on non-None.

### Often forgotten
- Distinct checkpoint per query.
- Idempotent sink (use Delta when possible).
- `maxOffsetsPerTrigger` (or equivalent) to bound batch size.
- Watermark on stateful operations.

### Monitor

```python
query.lastProgress    # most recent batch metrics
query.recentProgress  # list of last few
query.status          # "TRIGGER_ACTIVE", "GETTING_OFFSETS_FROM_SOURCE", etc.
```

Key metrics to alert on:
- `inputRowsPerSecond < expected` for too long → upstream silent.
- `processedRowsPerSecond < inputRowsPerSecond` → falling behind.
- `numRowsDroppedByWatermark > 0` → unexpected lateness.
- `batchDuration > triggerInterval` → can't keep up.
- `stateOperators[].memoryUsedBytes` growing → unbounded state.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Duplicates in sink after restart | Non-idempotent sink without batch_id dedup | Add idempotency |
| Data loss after restart | `startingOffsets=latest` + no checkpoint + old data unread | Restart from a known offset, accept the loss |
| Query won't restart: "Failed to load checkpoint" | Spark version upgrade / incompatible state | Test compatibility first; or restart with new checkpoint |
| Increasingly slow batches | Growing state | Watermark; RocksDB state store |
| Query silently stops | Driver disconnected, query exception | Wrap in `awaitTermination(timeout)` + restart logic |
| State store size > disk | RocksDB writes spill exceeded disk | More executors, smaller state per partition |
| Wrong output after schema evolution | Old checkpoint with old plan | Restart with new checkpoint after backfill |

## A production checklist

- [ ] Distinct, durable `checkpointLocation` per query.
- [ ] Idempotent sink (Delta is the default-right choice).
- [ ] `maxOffsetsPerTrigger` or equivalent rate limit.
- [ ] Watermark on every stateful op; appropriate delay.
- [ ] Monitoring on `lastProgress` metrics + alerting.
- [ ] Auto-restart on failure (e.g. Databricks job retry, or wrapper script).
- [ ] Periodic backfill/replay procedure documented.
- [ ] Tested restart from arbitrary failure points.

## References

- [LS Ch.8 §"Fault Tolerance", §"End-to-end Exactly-Once Semantics"]
- Spark docs: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics
- 📺 [Achieving Exactly-Once Semantics in Spark Streaming — Databricks](https://www.youtube.com/results?search_query=spark+structured+streaming+exactly+once)
- "Streaming Systems" by Akidau, Chernyak, Lax (O'Reilly) — Ch.4 on consistency semantics in general
