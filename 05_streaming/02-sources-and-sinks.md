# 02 — Sources and sinks

## Why this matters

A streaming pipeline is just a pair: where reads come from (source) and where writes go to (sink). Choose well and the pipeline is reliable, exactly-once, and easy to operate. Choose badly and you'll spend more time fixing duplicates than building features.

## Sources

| Source | Guarantees | Typical use |
|---|---|---|
| **Kafka** | Exactly-once with offsets | Production de-facto standard |
| **File** (Parquet/JSON/CSV/Delta) | Exactly-once via file naming | Log files, batch landing zones |
| **Delta** (as a stream) | Exactly-once | Bronze → silver pipelines |
| **rate** | N/A — test only | Generating synthetic load |
| **socket** | None | Local toy demos only |
| **Kinesis** (Databricks-bundled) | Exactly-once | AWS-native pipelines |

### Kafka — the canonical source

```python
events = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "user_events")
    .option("startingOffsets", "latest")    # or "earliest", or {"topic":{"0":42}}
    .option("maxOffsetsPerTrigger", 50000)  # back-pressure
    .load())

# `events` has columns: key, value (both binary), topic, partition, offset, timestamp
# Almost always cast value -> string -> JSON parse
parsed = events.select(
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp"),
    F.from_json(F.col("value").cast("string"), event_schema).alias("e"),
).select("topic", "partition", "offset", "timestamp", "e.*")
```

Key options:
- `startingOffsets`: `"earliest"` for backfill, `"latest"` for live, or explicit per-partition offsets.
- `endingOffsets`: only for batch reads (`spark.read`). Streaming runs forever.
- `maxOffsetsPerTrigger`: cap per-batch fetch — back-pressure.
- `kafka.group.id`: typically NOT set; Spark manages offsets itself.

### File source

```python
files = (spark.readStream
    .format("parquet")
    .schema(my_schema)               # mandatory for non-self-describing formats
    .option("maxFilesPerTrigger", 10)
    .load("s3://landing/events/"))
```

- File source watches a directory for new files. **A file is processed once** — it's the file path that defines the offset.
- For new files only: don't move/rewrite existing files in that directory. Don't use it as the source.
- Schema must match — partial-write files will fail.
- For very large directories: `option("cleanSource", "archive")` and `option("sourceArchiveDir", ...)` can move processed files away to keep listing fast.

### Delta as a source (CDF-style streaming)

```python
silver = (spark.readStream
    .format("delta")
    .option("ignoreDeletes", "true")           # treat DELETE as no-op
    .option("ignoreChanges", "true")           # treat UPDATE as new rows (no diff)
    .load("/tables/bronze_events"))
```

Or with Change Data Feed (CDF) — proper row-level changes:

```python
changes = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 100)            # or "startingTimestamp"
    .load("/tables/bronze_events"))
# `changes` has _change_type ('insert','update_preimage','update_postimage','delete')
```

CDF is the canonical pattern for bronze → silver → gold lakehouse pipelines.

[*Delta Definitive Guide* Ch.7]

## Sinks

| Sink | Guarantees | Notes |
|---|---|---|
| **Delta** | Exactly-once | Default for lakehouse |
| **Kafka** | At-least-once by default; exactly-once with transactions | Most common output |
| **File** (Parquet etc.) | Exactly-once via `_spark_metadata/` | Older; Delta is usually better |
| **foreachBatch** | Depends on sink | Bridge to anything (JDBC, REST, custom) |
| **foreach** | At-least-once | Per-row writer; expensive |
| **console** | None | Debug only |
| **memory** | None | In-driver table; test only |

### Delta sink

```python
(stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/bronze_events")
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")
    .start("/tables/bronze_events"))
```

Delta + Spark Structured Streaming = exactly-once natively. The checkpoint records batch offsets; the Delta commit log records what files were written for which batch. On restart, Spark replays from the last committed batch; Delta refuses duplicate commits for the same batch ID.

### Kafka sink

```python
(stream.selectExpr("CAST(key AS STRING) AS key",
                   "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("topic", "user_events_enriched")
    .option("checkpointLocation", "/checkpoints/kafka_sink")
    .start())
```

The DataFrame must have `key` and `value` columns (both castable to binary/string). `topic` can be either an option (single topic) or a column in the DataFrame (per-row routing).

For exactly-once to Kafka, you need Kafka transactions + Spark's transactional sink — supported but operationally heavier.

### foreachBatch — the escape hatch

When the built-in sinks don't fit:

```python
def write_to_postgres(batch_df, batch_id):
    (batch_df.write.format("jdbc")
        .option("url", "jdbc:postgresql://...")
        .option("dbtable", "events")
        .mode("append")
        .save())

(stream.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/checkpoints/pg_sink")
    .start())
```

`foreachBatch` gives you a regular DataFrame inside a callback. You can:
- Write to any batch sink.
- Do a MERGE into Delta.
- Call external APIs (rate-limit yourself).
- Write to multiple sinks atomically using `batch_df.persist()` + two writes.

For exactly-once, you need to handle `batch_id` idempotency yourself — write to a transactional sink, or use `txnAppId`/`txnVersion` on Delta. See note 08.

## Sources/sinks Ishan uses

For an AWS S3 + SNS architecture:
- **S3 file source** when SNS triggers cause new files to land. The Lambda already writes the file; the streaming job picks it up.
- **Delta sink** for the bronze layer — exactly-once, time travel.
- For larger scale, **Kinesis** (managed Kafka equivalent) is the AWS-native swap-in.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| File source missing new files | Wrong directory, hidden files, schema mismatch | Check the source path, `printSchema` first |
| Kafka offsets reset on restart | Checkpoint location changed or lost | Always set + preserve `checkpointLocation` |
| `Cannot find checkpoint location` | Path doesn't exist | Pre-create or `mkdirs` it |
| Duplicates in sink after restart | Non-idempotent sink, no batch_id handling | Use Delta sink, or idempotent foreachBatch |
| Stream processes all old data on restart | Wrong `startingOffsets` (`earliest`) | Use `latest` for new streams; checkpoint for resume |

## References

- [LS Ch.8 §"Sources and Sinks"]
- Kafka integration guide: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
- 📺 [Apache Spark Streaming with Kafka — Confluent](https://www.youtube.com/results?search_query=spark+structured+streaming+kafka+confluent)
- *Delta Lake: The Definitive Guide* Ch.7 — streaming with Delta
