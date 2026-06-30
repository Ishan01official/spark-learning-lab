# 04 — Writing Delta tables

## Why this matters

Reading Delta tables is just `spark.read.format("delta").load(...)`. Writing is where the nuances live — modes, partitioning, idempotency, schema handling, and the choice between paths and managed tables.

## Setup

Every example here assumes:

```python
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

builder = (SparkSession.builder
    .appName("delta-write")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

## Creating a table

### Path-based (external)

```python
df.write.format("delta").save("/tables/orders")
# Or as a "table" pointing to the same path:
spark.sql("CREATE TABLE orders USING DELTA LOCATION '/tables/orders'")
```

### Managed (catalog-owned)

```python
df.write.format("delta").saveAsTable("orders")
# Spark / metastore picks the storage location, owns the lifecycle
```

| | Path-based | Managed |
|---|---|---|
| Storage location | You choose | Catalog (Spark warehouse / Unity Catalog) |
| `DROP TABLE` | Removes metadata, keeps files | Removes metadata AND files |
| Portability | Move bucket → re-register | Tied to catalog |
| Use when | Multi-tool access, external owners | Single-platform, want lifecycle control |

## The four write modes

```python
df.write.format("delta").mode("append").save(path)
df.write.format("delta").mode("overwrite").save(path)
df.write.format("delta").mode("ignore").save(path)
df.write.format("delta").mode("errorifexists").save(path)  # default
```

| Mode | Behavior |
|---|---|
| `append` | Add new files, schema must match (or be evolution-compatible) |
| `overwrite` | Replace ALL data; new schema becomes the table schema |
| `ignore` | If table exists, do nothing; else create |
| `errorifexists` | Fail if table exists (default) |

### Dynamic partition overwrite

To overwrite only the partitions touched by the new data (instead of all partitions):

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Now this only overwrites partitions present in `df`
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save("/tables/events")
```

Without this, `overwrite` wipes the entire table. Almost always a bug.

Delta also offers `replaceWhere` for explicit control:

```python
df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "event_date >= '2024-01-01' AND event_date < '2024-02-01'") \
    .save("/tables/events")
```

`replaceWhere` lets you replace any data matching a predicate, not just whole partitions. Safer than dynamic overwrite — explicit boundary.

## Partitioning

```python
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save("/tables/events")
```

Rules of thumb (same as plain Parquet, see module 03 note 05):
- ≤ ~10,000 partitions total.
- ~ few GB per leaf partition.
- First column = most-filtered.
- Don't partition by `user_id` (high cardinality).

Delta-specific note: you can **change** the partitioning later via `replaceWhere` + `overwrite` per-partition, but it's expensive. Pick well the first time.

## Specifying the schema with constraints

```python
spark.sql("""
CREATE TABLE orders (
    order_id    BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    amount      DECIMAL(18,2) NOT NULL,
    status      STRING NOT NULL,
    created_at  TIMESTAMP,
    CONSTRAINT amount_positive CHECK (amount > 0),
    CONSTRAINT status_valid CHECK (status IN ('NEW','PAID','SHIPPED','REFUNDED'))
)
USING DELTA
PARTITIONED BY (DATE(created_at))
LOCATION '/tables/orders'
""")
```

`NOT NULL` and `CHECK` constraints are enforced on every write. Violations fail the write.

[*Delta Definitive Guide* Ch.5]

## Idempotent writes

A common pattern: a Lambda / Airflow task gets retried, and you don't want duplicates.

### Pattern 1: `replaceWhere` for partition-isolated batches

```python
# Each batch represents a single date — replace that exact partition.
batch_df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"event_date = '{batch_date}'") \
    .save("/tables/events")
```

Retrying a failed batch overwrites whatever (if anything) was written by the previous attempt. Idempotent.

### Pattern 2: `txnAppId` + `txnVersion` (true idempotency)

```python
# Delta will skip a write if the same (appId, version) was already committed.
batch_df.write.format("delta") \
    .mode("append") \
    .option("txnAppId", "lambda_supplier_terms") \
    .option("txnVersion", str(batch_id)) \
    .save("/tables/supplier_terms")
```

This is the right pattern for SNS/SQS-triggered Lambdas: use the event ID as `txnVersion`. Delta records the (appId, version) in the commit; a retry with the same pair is a no-op.

### Pattern 3: MERGE with a key (see note 06)

The most flexible — works for upserts with arbitrary join logic.

## Inspecting a table

```python
DeltaTable.forPath(spark, "/tables/orders").detail().show(truncate=False)
# Returns: format, id, name, location, createdAt, lastModified, partitionColumns,
#          numFiles, sizeInBytes, properties, minReaderVersion, minWriterVersion

DeltaTable.forPath(spark, "/tables/orders").history().show(truncate=False)
# Returns one row per commit with version, timestamp, userName, operation, operationParameters, ...
```

## Scale notes

- Writing too many small files (small batches, no compaction) → log grows fast, reads slow. Combat with `OPTIMIZE` (see note 08).
- A single `append` of 10 GB partitioned by date → 50–500 files. Fine.
- A streaming job appending micro-batches every 10s → 8640 commits/day. Schedule `OPTIMIZE` daily.
- Decimal vs Float for currency: use `DECIMAL(18,2)`. Float introduces rounding errors that auditors hate.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Whole table wiped after `mode("overwrite")` | Forgot `partitionOverwriteMode=dynamic` or `replaceWhere` | Restore via time travel; set the config |
| `AnalysisException: cannot resolve column` | Schema drift from upstream | Use `mergeSchema=true` or update schema explicitly |
| `ConstraintViolationException` | Row violates a CHECK or NOT NULL | Fix data, or relax constraint |
| Slow append of 1 row | Object storage commit overhead | Batch writes; don't write per-row |
| Duplicate rows after retry | No idempotency mechanism | Use `txnAppId/txnVersion` or `replaceWhere` |
| Table won't write — protocol error | Delta library version mismatch with table protocol | Upgrade `delta-spark` |

## References

- *Delta Lake: The Definitive Guide* — Ch.4 "Writing to Delta Tables", Ch.5 "Constraints"
- [LS Ch.9 §"Writing Tables"]
- 📺 [Idempotent Writes in Delta Lake — Databricks](https://www.youtube.com/results?search_query=delta+lake+idempotent+writes+databricks)
