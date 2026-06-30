# 06 — Stream-stream and stream-static joins

## Why this matters

Real pipelines almost always join. "Enrich each event with user country" is a stream-static join. "Match clicks to impressions within 5 minutes" is a stream-stream join. Each has its own constraints.

## Stream-static joins (the easy case)

A streaming DataFrame joined to a regular (batch) DataFrame.

```python
events = spark.readStream.format("kafka").option(...).load()
users  = spark.read.format("delta").load("/tables/users")     # batch DF

enriched = events.join(users, "user_id", "left")
```

What you need to know:
- The static side is **read into memory at query start**.
- Spark broadcasts it if small (under `autoBroadcastJoinThreshold`).
- The static side is **frozen** for the lifetime of the query — changes to `users` won't be picked up until you restart the query.
- Supported join types: inner, left-outer (with stream on left), right-outer (with stream on right), full-outer is NOT supported.

### Refreshing the static side

Three patterns:
1. **Restart the query periodically** (every hour, say). Simple but adds latency at restart.
2. **`foreachBatch` + re-read the static side per batch** — paying read cost each batch but always current.
3. **Make the static side a stream too** (Delta as source) and use a stream-stream join.

## Stream-stream joins (the harder case)

Both sides are streaming. Spark must keep some state from each side until it sees the join counterpart.

```python
clicks = stream_clicks.withWatermark("click_time", "30 seconds")
impressions = stream_impressions.withWatermark("imp_time", "30 seconds")

joined = clicks.join(
    impressions,
    F.expr("""
        clickId = impressionId
        AND click_time >= imp_time
        AND click_time <= imp_time + INTERVAL 5 MINUTES
    """))
```

### The two requirements

For Spark to bound state in a stream-stream join, you must provide:

1. **Watermarks on both sides.** Without these, state is unbounded.
2. **A time constraint in the join condition.** This tells Spark how far apart in event-time the two sides can be.

Without both, the join is rejected at query analysis time.

### How it works under the hood

```mermaid
sequenceDiagram
    participant CL as Clicks stream
    participant E as Engine
    participant IS as Impressions stream
    participant SS as State (clicks + imps within window)
    participant Out as Output
    CL->>E: click row
    E->>SS: lookup matching impressions
    SS-->>E: match found → emit; or store click for later
    IS->>E: impression row
    E->>SS: lookup matching clicks
    SS-->>E: match found → emit; or store impression for later
    E->>SS: expire rows older than watermark + window bound
```

State grows with `# rows × time_bound`. A 1M events/min stream with a 5-minute join window → ~5M rows of state per side. Bound your time window aggressively if you can.

### Outer joins on streams

Streaming outer joins need extra care: the engine has to wait until the watermark passes to decide a row has no match. Late-arriving matches will be dropped.

```python
# left outer: every click emitted, with impression NULL if no match found in window + watermark grace
joined = clicks.join(impressions, join_condition, "leftOuter")
```

Two consequences:
- The unmatched row is emitted only when the watermark moves past `(click_time + window_bound)`.
- That's typically `window + watermark_delay` after the row's event time.

So for a 5-minute join window + 30-second watermark, unmatched clicks appear in the output ~5.5 minutes after the click happened.

## Stream-stream join types supported

| Join type | Supported | Notes |
|---|---|---|
| Inner | ✅ | Both watermarks + time-bound required |
| Left outer | ✅ | Same; unmatched appears after grace |
| Right outer | ✅ | Same |
| Full outer | ✅ (3.5+) | Both sides need watermark |
| Left semi / anti | ✅ (3.5+) | Same |

Older Spark versions had less complete coverage; check the docs for your version.

## Multi-way joins

You can chain joins, but state cost compounds.

```python
result = (stream_a
    .withWatermark("ts_a", "30s")
    .join(stream_b.withWatermark("ts_b", "30s"), join_cond_ab)
    .join(stream_c.withWatermark("ts_c", "30s"), join_cond_bc))
```

Each join adds state. For most pipelines: do enrichment with stream-static joins where possible, and only use stream-stream joins for the truly time-correlated pieces.

## Patterns

### Pattern: enrich with a slow-changing dimension

```python
# Read the dimension as a stream from Delta — picks up updates automatically
users_stream = (spark.readStream.format("delta")
                .option("ignoreChanges", "true")
                .load("/tables/users"))

# Join event stream with user dim (both watermarked)
enriched = (events.withWatermark("ts", "30 seconds")
            .join(users_stream.withWatermark("updated_at", "1 hour"),
                  "user_id"))
```

### Pattern: stream-static with `foreachBatch` for refresh

```python
def join_and_write(batch_df, batch_id):
    # Re-read the static side every batch
    users = spark.read.format("delta").load("/tables/users")
    enriched = batch_df.join(F.broadcast(users), "user_id", "left")
    enriched.write.format("delta").mode("append").save("/tables/silver")

(events.writeStream
    .foreachBatch(join_and_write)
    .option("checkpointLocation", "/ck/silver")
    .start())
```

Slightly more expensive per batch but always current. Used a lot in Databricks Lakehouse pipelines.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Stream-stream join rejected at analysis | Missing watermark on a side or no time constraint | Add both |
| State growing on join | Time bound too large | Tighten the time window in the predicate |
| Outer join rows missing | Spark waiting for watermark to advance past grace period | Verify watermark progressing; check `lastProgress` |
| Static side stale | Static DF cached/broadcast at query start | Restart query, or use `foreachBatch` to re-read |
| Slow stream-stream join | Many state-store partitions; large state | Increase `shuffle.partitions`, switch to RocksDB |
| Duplicates in output | Multi-side late match emitted twice (rare) | Verify watermarks and time bounds; restart with cleared state if state corrupt |

## References

- [LS Ch.8 §"Streaming Joins"]
- [HPS Ch.10 §"Joins on Streams"]
- Spark docs: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#stream-stream-joins
- 📺 [Stream-Stream Joins in Apache Spark — Databricks](https://www.youtube.com/results?search_query=spark+stream+stream+joins+databricks)
