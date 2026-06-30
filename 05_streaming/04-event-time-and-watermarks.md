# 04 — Event time and watermarks

## Why this matters

Real streams have late data. A user's "click" event might reach Kafka 30 seconds, 30 minutes, or 3 hours after the click actually happened. Aggregating by *event time* (when it happened) rather than *processing time* (when Spark saw it) is what gives you correct answers — but only if you tell Spark how late is too late. That's what watermarks are for.

## Three notions of time

| Time | What it is |
|---|---|
| **Event time** | When the event happened in the real world (in the record's payload) |
| **Ingestion time** | When the message arrived at Kafka/the broker |
| **Processing time** | When Spark started processing it |

Production aggregation = event time. Processing time is fine for ordering/throughput, never for "how many sales did we make on Tuesday".

## Without a watermark — unbounded state

If you `groupBy("user_id").count()` on a stream, Spark must keep state for *every user_id forever*. State grows linearly with cardinality. For 100M users, that's gigabytes of state per executor. Eventually OOM.

With a window:
```python
stream.groupBy(F.window("event_time", "5 minutes"), "user_id").count()
```

state grows with `# users × # windows`. Windows accumulate forever unless Spark knows when to forget one — which is what watermark tells it.

## Watermark — "no more events older than this"

```python
counts = (stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"), "user_id")
    .count())
```

Reading this: "events more than 10 minutes late are dropped; windows whose end is more than 10 minutes behind the latest seen event_time can be finalized and the state forgotten".

Watermark advances based on the maximum event_time seen so far:
```
current_watermark = max(event_time seen) - 10 minutes
```

Any record with `event_time < current_watermark` is **dropped silently** (counted in `numDroppedDueToWatermark` metric, but not added to any aggregation).

```mermaid
gantt
    title Watermark progression
    dateFormat HH:mm
    axisFormat %H:%M

    section Events arriving
    event @ 10:05 :a1, 10:05, 1s
    event @ 10:08 :a2, 10:08, 1s
    event @ 10:01 (late, OK) :a3, 10:10, 1s
    event @ 09:55 (too late!) :crit, a4, 10:18, 1s

    section Watermark (delay=10m)
    wm = 09:55 :wm1, 10:05, 3m
    wm = 09:58 :wm2, 10:08, 10m
    wm = 10:08 :wm3, 10:18, 5m
```

## Window types

```python
# Tumbling — non-overlapping fixed size
F.window("event_time", "5 minutes")

# Sliding — fixed size, advancing every N
F.window("event_time", "5 minutes", "1 minute")

# Session — bounded by inactivity gap
F.session_window("event_time", "10 minutes")
```

Use case:
- **Tumbling**: "events per 5-minute bucket"
- **Sliding**: "rolling 5-minute count, updated each minute"
- **Session**: "user sessions, ended after 10 min of inactivity"

All accept a watermark.

## Choosing watermark delay

| Delay | Trade-off |
|---|---|
| Short (e.g. 1 min) | Lower latency, less state, but more late data dropped |
| Long (e.g. 1 hour) | More late data captured, larger state, longer to emit windows |
| Very long (e.g. 1 day) | State explosion likely; consider whether this is the right pipeline |

A common heuristic: set watermark to **2–3× the 99th-percentile lateness** you observe in production.

## Watermark with appendMode

```python
# This pattern: aggregation + watermark + append mode
# windows are emitted exactly once, when watermark passes their end.
counts = (stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"))
    .count())

(counts.writeStream.outputMode("append").format("delta").start(...))
```

This is the canonical "ever-correct aggregation" pattern: each window's count is emitted once, after enough time has passed that no more events are expected.

For `update` mode, windows are emitted every batch while still open; for `complete`, the whole result is re-emitted.

## Watermark on joins

Stream-stream joins need watermarks on **both** sides:

```python
events.withWatermark("event_time", "30 seconds") \
    .join(
        impressions.withWatermark("imp_time", "30 seconds"),
        F.expr("""
            event_time BETWEEN imp_time AND imp_time + INTERVAL 5 MINUTES
            AND user_id = imp_user_id
        """))
```

Without the watermark + time-bounded predicate, Spark would need to keep all of both streams forever — unbounded state. The predicate bounds how late one side can be vs the other.

## What gets dropped

Records dropped due to watermark are NOT errors. They're counted, and you can monitor:

```python
# Inspect dropped records
query.lastProgress["stateOperators"][0]["numRowsDroppedByWatermark"]
```

If this number is non-zero, you're either:
- Receiving genuinely late data (consider relaxing watermark), or
- Receiving out-of-order data the watermark is correctly catching (good).

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| State store growing without bound | No watermark, or watermark column not actually event time | Add `withWatermark("event_time", ...)` |
| All rows dropped, no output | Watermark advanced past all events (clock skew?) | Check event_time values vs the watermark progress |
| Windows never close (append mode) | Watermark not advancing because max(event_time) not progressing | Verify source actually emits new event times |
| `outputMode("append")` rejects query | Aggregation without watermark | Add watermark |
| Watermark delay too short, data dropped silently | Underestimated lateness | Look at `numRowsDroppedByWatermark`; widen |
| `withWatermark` ignored | Set on the wrong DataFrame (e.g. after the aggregation) | Apply BEFORE `groupBy`/`window` |

## References

- [LS Ch.8 §"Event-Time and Watermarking"]
- [HPS Ch.10 §"Stateful Operations"]
- Spark docs: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
- 📺 [Watermarking in Apache Spark Structured Streaming — Databricks](https://www.youtube.com/results?search_query=structured+streaming+watermark+databricks)
- "Streaming Systems" by Tyler Akidau et al. (O'Reilly) — Ch.3 on watermarks (concept-level, not Spark-specific)
