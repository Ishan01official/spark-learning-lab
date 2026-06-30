# 05 — Stateful aggregations

## Why this matters

Anything that needs to remember information across batches is *stateful*: aggregations, deduplications, joins, sessions, custom mapGroupsWithState. State store sizing and watermarks are the operational levers.

## What's "state" in Structured Streaming

The state store holds:
- Aggregation intermediates: `(key → partial_aggregate)`
- Deduplication tracking: `(key → seen_marker)`
- Join buffers: rows waiting for their counterpart
- Custom state from `mapGroupsWithState` / `flatMapGroupsWithState`

State is partitioned (default 200 partitions, controlled by `spark.sql.shuffle.partitions`) and stored:
- **In memory** during execution
- **On disk** in the checkpoint location (HDFS, S3, or local)
- **Replicated** to a peer executor if `spark.sql.streaming.stateStore.providerClass` is set to RocksDB (recommended for large state)

## Built-in stateful ops

### 1. Windowed aggregation

```python
counts = (stream.withWatermark("event_time", "10 minutes")
                .groupBy(F.window("event_time", "5 minutes"), "user_id")
                .agg(F.sum("amount").alias("total"),
                     F.count("*").alias("n")))
```

State: one entry per (user_id, window) for windows that haven't yet been retired by the watermark.

### 2. Streaming `dropDuplicates`

```python
deduped = (stream.withWatermark("event_time", "1 hour")
                 .dropDuplicates(["event_id"]))
```

State: one entry per `event_id` until it falls behind the watermark. Without watermark, state grows forever — almost always a bug.

### 3. `groupBy` without window (running aggregations)

```python
# Running count per user, no window
running = stream.groupBy("user_id").count()
```

State: one entry per `user_id`, forever — no watermark can prune this. Acceptable only if `user_id` cardinality is bounded and small.

Use `update` or `complete` output mode (not `append` — there's no time at which a key's count is "final").

### 4. Arbitrary state — `mapGroupsWithState` / `flatMapGroupsWithState`

For state machines, custom session logic, anything the built-ins can't express:

```python
from pyspark.sql.functions import col
from pyspark.sql.streaming import GroupState, GroupStateTimeout

# Pseudocode — Python API is via applyInPandasWithState in recent Spark
def update_session(user_id, events_iter, state: GroupState):
    if state.hasTimedOut:
        # emit the session and remove state
        session = state.get
        state.remove()
        yield (user_id, session)
    else:
        session_state = state.getOption.getOrElse(initial_state)
        for ev in events_iter:
            session_state.update_with(ev)
        state.update(session_state)
        state.setTimeoutDuration("30 minutes")

result = stream.groupByKey(lambda r: r.user_id) \
               .flatMapGroupsWithState(update_session,
                                       output_mode="update",
                                       timeout_conf=GroupStateTimeout.EventTimeTimeout)
```

In PySpark, the Pandas-flavor is `applyInPandasWithState` — same idea, but the function receives a Pandas DataFrame per group per batch.

This is power-tool territory: arbitrarily complex per-key state machines, with control over timeouts and emission. Production CDC-flattening pipelines and custom session windows live here.

[LS Ch.8 §"Arbitrary Stateful Operations"]

## RocksDB state store (highly recommended for production)

By default, Spark's state store is an in-memory hash map backed by snapshot files. For state sizes above a few GB per executor, this struggles.

Switch to RocksDB:

```python
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
```

RocksDB:
- Spills to local disk; supports state much larger than memory.
- Snapshots are smaller and faster.
- Used in Databricks runtime by default for many workloads.

[HPS Ch.10 §"RocksDB State"]

## Inspecting state

```python
query.lastProgress
# {
#   "stateOperators": [
#     {
#       "numRowsTotal": 12345,
#       "numRowsUpdated": 200,
#       "memoryUsedBytes": 5_000_000,
#       "numRowsDroppedByWatermark": 0,
#       "customMetrics": {...}
#     }
#   ],
#   ...
# }
```

The Spark UI's Structured Streaming tab also visualizes this per query.

## Scaling state

Some rules of thumb:

| State size per partition | What to do |
|---|---|
| < 100 MB | Default in-memory store fine |
| 100 MB – 1 GB | RocksDB state store strongly recommended |
| 1 – 10 GB | RocksDB + tune `spark.sql.streaming.stateStore.rocksdb.compactOnCommit` |
| > 10 GB | Consider whether the design is right; can you partition more, use shorter watermarks? |

State is sharded across `spark.sql.shuffle.partitions` partitions. More partitions = less per-partition state but more state-store overhead. 200 (default) is a good starting point; bump if any partition exceeds 1 GB.

## Patterns

### Pattern: streaming dedup with TTL

```python
deduped = (stream.withWatermark("event_time", "24 hours")
                 .dropDuplicates(["event_id", "event_time"]))
```

State holds each `event_id` for 24 hours. After that, the same event_id is treated as new (acceptable if events don't truly repeat over multi-day spans).

### Pattern: bounded per-key state via window

```python
# Top events per user per hour
counts = (stream.withWatermark("event_time", "5 minutes")
                .groupBy(F.window("event_time", "1 hour"), "user_id", "event_type")
                .count())
```

State for a window only persists for ~ `(window + watermark)` after the window starts. Predictable, bounded.

### Pattern: session windows

```python
# Built-in session window (Spark 3.2+)
sessions = (stream.withWatermark("event_time", "10 minutes")
                  .groupBy(F.session_window("event_time", "10 minutes"), "user_id")
                  .count())
```

Or for full control, `mapGroupsWithState`.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| State growing without bound | No watermark on aggregation/dedup | Add `withWatermark` |
| OOM after running for hours | State exceeds memory store capacity | Switch to RocksDB |
| Slow micro-batches | Large state, lots of snapshot writing | RocksDB + tune compaction; raise checkpoint interval |
| Output rows for a window appear twice | `outputMode("update")` re-emits per batch (expected) | Use `append` if you want once-per-window |
| `numRowsDroppedByWatermark > 0` | Late events past the watermark | Either acceptable (it's bounding state) or widen watermark |
| `Conflict between session window and append output mode` | Older Spark version | Use `update` mode, or upgrade |

## References

- [LS Ch.8 §"Stateful Operations", §"mapGroupsWithState"]
- [HPS Ch.10] — best deep dive
- Spark docs: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#types-of-time-windows
- 📺 [State Store in Structured Streaming — Databricks](https://www.youtube.com/results?search_query=structured+streaming+state+store+rocksdb)
