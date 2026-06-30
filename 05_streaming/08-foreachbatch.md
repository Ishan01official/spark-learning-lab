# 08 — `foreachBatch` and arbitrary sinks

## Why this matters

Sometimes the built-in sinks don't fit. You need to MERGE into Delta, write to two sinks atomically, hit a REST API, or push to a database that doesn't have a Spark sink. `foreachBatch` is the universal escape hatch — it gives you a regular DataFrame per micro-batch and lets you do whatever batch operations you want with it.

## The signature

```python
def write_function(batch_df: DataFrame, batch_id: int) -> None:
    # Treat batch_df like any regular DataFrame
    ...

(stream.writeStream
    .foreachBatch(write_function)
    .option("checkpointLocation", "/ck/somewhere")
    .start())
```

Each micro-batch produces a `(batch_df, batch_id)` pair. `batch_id` is monotonically increasing and stable across restarts — it's your idempotency token.

[LS Ch.8 §"foreachBatch"]

## What you can do inside

Anything that works on a normal DataFrame:

- `batch_df.write.format(...).save(...)` — any batch sink.
- `DeltaTable.forPath(...).merge(batch_df, ...).execute()` — Delta MERGE (the most common use).
- `batch_df.write.format("jdbc")...` — write to RDBMS.
- HTTP calls in a `foreach` (be careful about rate limits and per-task connections).
- Cache and write to multiple sinks.

## Common patterns

### Pattern 1: streaming MERGE into Delta

Built-in Delta sinks support `append` natively but not `MERGE`. The streaming MERGE pattern is:

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/tables/users")

def merge_to_target(batch_df, batch_id):
    (target.alias("t")
       .merge(batch_df.alias("s"), "t.user_id = s.user_id")
       .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
       .whenNotMatchedInsertAll()
       .execute())

(updates_stream.writeStream
    .foreachBatch(merge_to_target)
    .option("checkpointLocation", "/ck/users_merge")
    .start())
```

This is *the* canonical CDC pipeline pattern.

### Pattern 2: write to multiple sinks atomically-ish

```python
def fanout(batch_df, batch_id):
    batch_df.persist()           # avoid recomputing the upstream pipeline
    try:
        batch_df.write.format("delta").mode("append").save("/tables/silver")
        batch_df.write.format("delta").mode("append").save("/tables/audit")
        # If you need a Kafka sink too:
        batch_df.selectExpr("CAST(user_id AS STRING) AS key", "to_json(struct(*)) AS value") \
                .write.format("kafka").options(**KAFKA_OPTS).save()
    finally:
        batch_df.unpersist()
```

Not atomic across the three sinks — if the second one fails, the first already wrote. Mitigation: use Delta's idempotent writes (txnAppId + txnVersion = batch_id) so a retry doesn't duplicate.

### Pattern 3: idempotent foreachBatch with txn-style sinks

```python
def write_with_idempotency(batch_df, batch_id):
    (batch_df.write
        .format("delta")
        .mode("append")
        .option("txnAppId", "streaming_silver")
        .option("txnVersion", str(batch_id))
        .save("/tables/silver"))
```

A retried batch with the same `batch_id` is a Delta no-op. Exactly-once even if the streaming engine retries.

### Pattern 4: rate-limited external API call

```python
import requests

def push_to_api(batch_df, batch_id):
    # Collect to driver — only OK for small batches!
    rows = batch_df.limit(1000).collect()
    for row in rows:
        requests.post("https://api.example.com/events",
                      json=row.asDict(),
                      timeout=5)
        # add throttling here as needed

(small_stream.writeStream
    .foreachBatch(push_to_api)
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/ck/api")
    .start())
```

For larger batches: use `df.foreachPartition` to parallelize across executors.

### Pattern 5: enrichment by re-reading static side per batch

```python
def enrich(batch_df, batch_id):
    users = spark.read.format("delta").load("/tables/users")  # fresh read
    enriched = batch_df.join(F.broadcast(users), "user_id", "left")
    enriched.write.format("delta").mode("append").save("/tables/silver")

(events_stream.writeStream
    .foreachBatch(enrich)
    .option("checkpointLocation", "/ck/silver")
    .start())
```

Pays the read cost each batch but the dimension is always current.

## `foreach` vs `foreachBatch`

| | `foreach` | `foreachBatch` |
|---|---|---|
| Granularity | Per row | Per micro-batch DataFrame |
| API | `ForeachWriter` class with `open`/`process`/`close` | Function `(df, id) -> None` |
| Use when | True per-row external system (e.g. push notification) | Almost everything else |
| Performance | One handle per partition (worker) | One DataFrame operation |
| Exactly-once | Up to the user (per-row) | Up to the user (per-batch) |

For 99% of cases, `foreachBatch` is the right choice.

## `foreach` example (the rare case)

```python
class HttpWriter:
    def open(self, partition_id, epoch_id):
        import requests
        self.session = requests.Session()
        return True
    def process(self, row):
        self.session.post("https://api.example.com/events", json=row.asDict())
    def close(self, error):
        self.session.close()

(stream.writeStream.foreach(HttpWriter()).start())
```

## Caveats

- `foreachBatch` is **at-least-once by default**. The same `(batch_df, batch_id)` may be invoked twice on failure. Use `batch_id` as your idempotency token in the sink.
- The `batch_df` is a streaming-context DataFrame, but the operations inside are batch — you CAN use batch-only operations like `collect()`, `cache()`, `count()`.
- Don't start new streaming queries inside `foreachBatch`. Batch jobs only.
- Errors raised inside the function fail the streaming query.

## Performance tips

- **Always `persist`** if you write the same `batch_df` to multiple sinks.
- **`unpersist`** in a `finally` clause to free memory.
- For tiny batches, `foreachBatch` has more per-batch overhead than the native sinks. For high-frequency micro-batches at >100 rows/s sustained, the native sinks are usually faster.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Each restart re-runs the latest batch | Batch failed before sink ack but after checkpoint commit; expected | Make idempotent with `batch_id` |
| Same row in sink twice | Non-idempotent operations + retry | Use idempotency token; or transactional sink |
| `foreachBatch` slow per batch | Reading dimension table per batch is too costly | Cache it once outside; or restart query periodically |
| OOM in foreachBatch | Calling `.collect()` on a huge batch | Use partition-parallel processing instead |
| MERGE inside foreachBatch is slow | Target not Z-ORDERed by merge key | Z-ORDER the target |
| Random Spark error inside the function | Tried to start a streaming query inside | Use batch ops only |

## References

- [LS Ch.8 §"foreachBatch"]
- Spark docs: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch
- *Delta Definitive Guide* Ch.7 — streaming MERGE patterns
- 📺 [Production Streaming with foreachBatch — Databricks](https://www.youtube.com/results?search_query=spark+foreachbatch+databricks)
