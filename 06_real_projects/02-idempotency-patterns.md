# 02 — Idempotency in ETL

## Why this matters

Every production ETL job will be retried — by Airflow, by Lambda, by a developer running it again because something broke. If running the same job twice produces different results than running it once, you've got a bug waiting to bite you.

This note catalogs the patterns that make ETL idempotent at each layer of the medallion.

## The idempotency contract

An idempotent operation, formally: `f(state) = f(f(state))`.

For ETL specifically: **running the same job with the same inputs leaves the system in the same final state, regardless of how many times it ran.**

Five patterns. Pick the one that fits your sink.

## Pattern 1: replace-partition

For partitioned tables, the simplest idempotency is to make a job *own* a partition. Re-running replaces that partition.

```python
day_df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"event_date = '{batch_date}'") \
    .save("/tables/events")
```

Works for: batch ETL with clearly-defined date partitions.

Trade-offs:
- One job = one partition. Don't process multiple days at once.
- If a job writes to multiple partitions, retry replaces all of them — usually fine, sometimes not.

## Pattern 2: Delta txnAppId + txnVersion

Delta has a built-in idempotency token. Re-running with the same `(txnAppId, txnVersion)` is a no-op at the commit level.

```python
batch_df.write.format("delta") \
    .mode("append") \
    .option("txnAppId", "lambda_supplier_terms") \
    .option("txnVersion", str(event_id)) \
    .save("/tables/supplier_terms")
```

Works for: Lambda / SNS / SQS jobs where the message itself has a stable ID.

This is the pattern Ishan uses in his supplier terms Lambda. The SNS message ID becomes `txnVersion`; retries of the same SNS message are silently dropped.

## Pattern 3: MERGE with stable keys

For row-level updates, MERGE on a stable primary key is idempotent by construction.

```python
target.alias("t").merge(
    source.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll(condition="s.updated_at > t.updated_at") \
 .whenNotMatchedInsertAll() \
 .execute()
```

Re-running merges the same rows; updates that already happened are still "newer" than themselves only if you use `>` (strict). With `>=`, a row is updated to itself — also idempotent, but a wasted write.

Works for: silver-layer dimension upserts, CDC-style streams.

Caveats:
- Source duplicates: if the source batch has the same key twice with different values, MERGE may pick either; deduplicate first.
- Watermarks for late data: use `condition="s.updated_at > t.updated_at"` to ensure late-arriving older versions don't overwrite newer state.

## Pattern 4: unique constraints + INSERT IGNORE

For non-Delta sinks (Postgres, MySQL, etc.):

```sql
INSERT INTO events (event_id, ...)
VALUES (?, ...)
ON CONFLICT (event_id) DO NOTHING
```

Re-inserts of the same `event_id` are silently dropped.

Works for: any RDBMS sink with a unique column you can rely on (event_id, message_id, hash of payload).

Caveats:
- Slow if you have many duplicates — every row attempts an insert.
- A unique key on a synthetic ID is cheaper than a hash, but requires upstream to provide one.

## Pattern 5: content hash as key

If you can't trust the upstream to provide a stable ID, derive one:

```python
df = batch_df.withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in stable_cols]), 256)
)

# Then MERGE on row_hash or use it as a unique key downstream
```

Works for: completely uncontrolled upstreams.

Trade-offs:
- Changing the hash columns is a one-way migration.
- A change to any "stable" column produces a "new" row even if it's logically the same.

## Idempotency at each layer

| Layer | Recommended pattern | Why |
|---|---|---|
| Bronze (append-only) | `replaceWhere` by partition, OR `txnAppId/txnVersion` | Avoid double-writing the same source batch |
| Silver | MERGE with stable keys + condition on updated_at | Handles late data, upserts cleanly |
| Gold | Full rebuild from silver (idempotent by overwrite) | Simplest; trade compute for safety |

## Tracking what's been processed

Even with idempotent writes, you usually want to know "did batch X complete?". Two patterns:

### Process-tracking table

```sql
CREATE TABLE process_log (
    job_name STRING,
    batch_id STRING,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    row_count BIGINT,
    PRIMARY KEY (job_name, batch_id)
);
```

The job:
1. Marks the batch as started.
2. Runs the work (idempotent).
3. Marks the batch as completed.

A retry sees "completed_at IS NOT NULL" and skips. (Combine with idempotent writes for safety — the marker can lie if the job crashed between commit and update.)

### Use the Delta history

For Delta sinks, the history itself is the log. Query it:

```python
DeltaTable.forPath(spark, "/tables/silver_events") \
    .history() \
    .filter("operationParameters.txnVersion = '...'") \
    .count() > 0  # this batch was already processed
```

Useful for sanity-checking without a separate state store.

## When you can't be idempotent

Some operations genuinely can't be made idempotent without major redesign:

- **External side effects** — sending an email, charging a credit card. Use the outbox pattern: write the intent idempotently, have a separate process drain it.
- **Approximate aggregations** — HyperLogLog cardinality across runs. Accept drift, or rebuild.
- **Auto-incrementing IDs** — replays create new IDs. Avoid these in ETL.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Duplicates after Airflow retry | Source ran twice; sink had no dedup | Pick one of the 5 patterns |
| MERGE wins update with stale data | Source has same key twice in one batch | Dedup the source first |
| `replaceWhere` wiped a wrong partition | Wrong predicate, no test | Always parameterize, always test |
| Process log says "completed" but data missing | Logging before write completed | Log only after the commit succeeds |
| Hash-based keys change after schema evolution | Added/removed a "stable" column | Lock the hash columns; migrate explicitly |

## References

- *Delta Lake: The Definitive Guide* — Ch.7 on streaming idempotency
- Pattern catalog: martinfowler.com (search "idempotent receiver", "outbox pattern")
- [DAS Ch.9 §"Idempotent ETL"]
