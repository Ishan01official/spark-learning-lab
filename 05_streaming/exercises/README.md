# Module 05 — Structured Streaming exercises

---

## 1. Output mode matrix

For each combination, predict whether Spark accepts the query, and why. Then verify by writing it (use the `rate` source).

| Query | append | update | complete |
|---|:---:|:---:|:---:|
| `stream.select(...)`  (no agg) | ? | ? | ? |
| `stream.groupBy("k").count()` (no watermark) | ? | ? | ? |
| `stream.withWatermark(...).groupBy(window(...)).count()` | ? | ? | ? |
| `stream.dropDuplicates(["id"])` (no watermark) | ? | ? | ? |
| `stream.withWatermark(...).dropDuplicates(["id"])` | ? | ? | ? |

Document any error messages you got.

---

## 2. Lateness experiment

Modify `examples/03_watermark_windowed_agg.py` to:

- Generate events where 1% of rows have event_time set to *60 seconds in the past* (very late).
- Set the watermark to 10s.
- Count `numRowsDroppedByWatermark` over a minute.

Then change the watermark to 90s and re-run. How many fewer rows are dropped? What's the cost in terms of state-store size?

---

## 3. Restart behavior

Run `examples/02_file_source_to_delta.py`. Let it process a few batches. Then `Ctrl+C` it.

- Without changing anything, restart it. Does it reprocess files it already wrote? (It shouldn't — verify by checking row counts.)
- Now delete the checkpoint directory and restart. What happens? Why is this almost always a disaster in production?

---

## 4. Exactly-once via foreachBatch

Write a foreachBatch function that writes to a Postgres-like table. Make it idempotent using one of:

- A unique key on `(event_id)` with `INSERT ... ON CONFLICT DO NOTHING`.
- A `(batch_id, partition, offset)` triple as primary key.

Demonstrate that re-running the same batch twice produces the same final state in the target. (Stub Postgres with a local SQLite or even a Python dict for the exercise.)

---

## 5. Stream-stream join

Build a click-vs-impression matcher:

- Two synthetic streams: impressions every 100ms, clicks every 200ms.
- A click matches an impression if `impression_id == imp_id` AND `click_time` is between `imp_time` and `imp_time + 5 minutes`.
- Inner join, watermark on both sides.

Observe:
- State growth over time. Does it stabilize?
- What happens if you remove the watermark? (You get an analysis error.)
- What happens if you remove the time predicate but keep the watermarks?

---

## 6. Tune for cost

You have a streaming job that processes ~500 events/sec on a 4-core local mode setup. The trigger is `processingTime="1 second"`. The batches are taking 3 seconds.

- What metrics in `query.lastProgress` confirm "falling behind"?
- Three knobs you'd try first, in order, to fix this.
- At what point would you consider switching to `availableNow` cron'd by Airflow instead of a long-running stream?

---

## 7. Build a bronze-silver-gold pipeline

Build three streaming queries:

- **Bronze**: file source → Delta. Just dump raw rows with `topic`, `partition`, `value`, `kafka_ts`.
- **Silver**: Delta source (bronze) → Delta. Parse JSON, validate schema, drop bad rows to a dead-letter table.
- **Gold**: Delta source (silver) → Delta. 1-minute tumbling window aggregations with watermark.

Each must have its own checkpoint location. Each must be restartable independently. Show the lineage between them in a diagram.

---

## 8. Production runbook

Document a runbook for a Structured Streaming production deployment, covering:

- Monitoring (which `lastProgress` fields you alert on, thresholds).
- Restart procedure on transient failure.
- Restart procedure on incompatible code change.
- Backfill procedure (replaying historical data).
- How you detect and act on each of:
  - Source backpressure
  - Sink slowness
  - State explosion
  - Late-data spike
