# 05 — Time travel and versioning

## Why this matters

A Delta table is an immutable sequence of versions. Time travel lets you query any past version — for audit, debugging, recovering from a bad write, or reproducing yesterday's ML training set.

## Two ways to address a version

### By version number

```python
# Read version 42
df = spark.read.format("delta") \
    .option("versionAsOf", 42) \
    .load("/tables/orders")

# SQL
spark.sql("SELECT * FROM delta.`/tables/orders` VERSION AS OF 42")
```

### By timestamp

```python
df = spark.read.format("delta") \
    .option("timestampAsOf", "2024-09-15 09:00:00") \
    .load("/tables/orders")

# SQL
spark.sql("SELECT * FROM delta.`/tables/orders` TIMESTAMP AS OF '2024-09-15 09:00:00'")
```

The timestamp resolves to "the latest commit whose timestamp ≤ this".

## Listing history

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/tables/orders")
dt.history().show(truncate=False)
```

Sample output:

| version | timestamp | userName | operation | operationParameters | readVersion | isolationLevel | isBlindAppend |
|--:|---|---|---|---|--:|---|---|
| 5 | 2024-09-15 11:30 | etl | MERGE | { predicate: "t.id = s.id" } | 4 | WriteSerializable | false |
| 4 | 2024-09-15 09:00 | etl | WRITE | { mode: "Append" } | 3 | WriteSerializable | true |
| 3 | 2024-09-14 23:00 | etl | OPTIMIZE | { predicate: "" } | 2 | WriteSerializable | true |
| ... |

`operationParameters` is your audit log. Save this somewhere for compliance.

## Common time-travel uses

### 1. Recover from a bad write

You ran an ETL that produced bad data. Restore.

```python
dt.restoreToVersion(4)

# Or via SQL:
# RESTORE TABLE delta.`/tables/orders` TO VERSION AS OF 4
```

This creates a *new* commit that re-materializes version 4 as the current state. The earlier history isn't deleted — you can still see versions 5, 6, etc. in `history()`, but `SELECT * FROM table` now returns version 4's contents.

### 2. Reproducible analytics / ML

Pin your training set:

```python
training_df = spark.read.format("delta") \
    .option("versionAsOf", 1234) \
    .load("/tables/features")

# Model can be retrained byte-for-byte from this exact data
```

Store the version number in your ML metadata. Now "rerun yesterday's experiment" is unambiguous.

### 3. Diff two versions

```python
v_old = spark.read.format("delta").option("versionAsOf", 9).load("/tables/orders")
v_new = spark.read.format("delta").option("versionAsOf", 10).load("/tables/orders")

added   = v_new.subtract(v_old)
removed = v_old.subtract(v_new)
```

For row-level CDC, use Change Data Feed (CDF) — see *Definitive Guide* Ch.7 — which records each row change.

### 4. Audit "who changed what"

```python
(dt.history()
   .filter(F.col("timestamp").between("2024-09-15", "2024-09-16"))
   .select("version", "timestamp", "userName", "operation", "operationParameters")
   .show(50, truncate=False))
```

## Limits on how far back you can go

Time travel works as long as:
1. The transaction log entry still exists, AND
2. The data files referenced by that version still exist on disk.

VACUUM (note 08) deletes files that were `remove`'d more than `delta.deletedFileRetentionDuration` ago (default 7 days). Once a version's files are vacuumed, that version becomes un-queryable — but its log entry still exists.

Log entries are pruned by `delta.logRetentionDuration` (default 30 days). Logs older than that are deleted; you can't time-travel to them at all.

Two settings to know:

| Property | Default | What it does |
|---|---|---|
| `delta.logRetentionDuration` | 30 days | How long to keep commit JSONs |
| `delta.deletedFileRetentionDuration` | 7 days | How long to keep `remove`'d data files |

Set them via:

```sql
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 90 days',
  'delta.deletedFileRetentionDuration' = 'interval 14 days'
)
```

If you need year-long time travel: set both to ~365 days. Costs more storage; gains audit reach.

## CLONE — branching a table

```sql
-- Deep clone: independent copy with full history
CREATE TABLE orders_archive DEEP CLONE orders;

-- Shallow clone: metadata only, shares files (read-only safe)
CREATE TABLE orders_test SHALLOW CLONE orders;
```

Shallow clones are great for testing migrations or schema changes without touching production. Deep clones are how you snapshot a table for archival.

## Scale notes

- Time travel doesn't cost anything per query — it's just reading older log entries.
- Long retention costs storage: a table with 1 TB and 1 daily snapshot for a year ≈ 365 TB raw, or much less if days are mostly the same files (Delta only stores the diff effectively, since unchanged files are shared across versions).
- The "free" version: `DESCRIBE HISTORY` is cheap even on huge tables since it just reads the log directory.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `IllegalArgumentException: file ... no longer exists` | Tried to time-travel past `deletedFileRetentionDuration` | Extend retention before vacuuming; or restore from backup |
| Time travel returns "table not found" | Tried before table creation | Use earliest valid version (0) |
| `RESTORE` failed | New protocol incompatible with current writer | Upgrade Delta library, or use shallow clone first |
| Different results for same `timestampAsOf` on retry | Wall clock vs commit time confusion (rare) | Use `versionAsOf` for exact reproducibility |
| Table grew unexpectedly after retention bump | Log + data retention applied retroactively | Expected; budget storage accordingly |

## References

- *Delta Lake: The Definitive Guide* — Ch.6 "Time Travel"
- Delta docs: https://docs.delta.io/latest/delta-batch.html#-deltatimetravel
- [LS Ch.9 §"Time Travel"]
- 📺 [Time Travel in Delta Lake — Databricks Academy](https://www.youtube.com/results?search_query=delta+lake+time+travel+databricks)
