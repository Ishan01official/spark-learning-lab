# 08 — `OPTIMIZE`, `Z-ORDER`, and `VACUUM`

## Why this matters

Three maintenance operations. The first two make queries faster; the third reclaims storage. None are optional in production — a Delta table without periodic OPTIMIZE/VACUUM will eventually slow down and bloat.

## `OPTIMIZE` — compact small files

Append-heavy tables accumulate many small files (one per micro-batch, one per writer, one per partition per batch). Small files = many seeks, slow scans, big log.

```sql
OPTIMIZE delta.`/tables/events`;

-- Or limit to a partition for cheaper incremental maintenance:
OPTIMIZE delta.`/tables/events` WHERE event_date >= '2024-09-01';
```

What it does:
1. Identifies "small" files (default: < 1 GB).
2. Reads them, writes new files at the target size (default 1 GB).
3. Atomically swaps: `remove` old + `add` new in one commit.

**Cost**: roughly 1× the data being compacted — read it, rewrite it. Schedule during off-peak.

**Benefit**: a table that was 50,000 files × 5 MB becomes 250 files × 1 GB. Queries that previously did 50,000 file opens now do 250.

### Knobs

```sql
ALTER TABLE events SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',     -- Spark adjusts file size at write time
  'delta.autoOptimize.autoCompact'   = 'true'      -- Auto-compact after each write
);
```

Auto-optimize avoids the small-files problem proactively at the cost of slower writes (extra shuffles). Suited for streaming or many-small-writers workloads. Not in OSS Delta as of writing — Databricks-specific; check current OSS support.

[*Delta Definitive Guide* Ch.9]

## `Z-ORDER` — multi-dimensional clustering

`Z-ORDER BY` is a clustering algorithm that co-locates rows with similar values across multiple columns. After Z-ORDER:
- Files contain narrow ranges of the Z-ordered columns.
- Min/max stats in the transaction log become highly selective.
- Filters can skip far more files.

```sql
OPTIMIZE delta.`/tables/events`
  ZORDER BY (user_id, event_type);
```

The same files-per-stage rewrite as OPTIMIZE, but the rows are *sorted* into a space-filling curve over the Z-ordered columns. Now a `WHERE user_id = 42 AND event_type = 'click'` query skips most files at the log level.

### When Z-ORDER helps

- Columns frequently in `WHERE` clauses — especially equality or narrow-range predicates.
- High-cardinality columns (user_id, device_id, sku) — too cardinal to partition on.
- Multiple "join key" columns where you want both to have good min/max stats.

### When it doesn't

- Columns already used as partition columns — Z-ORDER won't help inside a partition that already filters small.
- Wide-range scans — Z-ORDER reduces files read, not bytes per file.
- Aggregation-heavy queries that read everything — no skip helps.

### Z-ORDER vs liquid clustering (newer)

Delta 3.x introduced **liquid clustering** — designed to replace partitioning + Z-ORDER with a single declarative clustering scheme that adapts as data arrives.

```sql
CREATE TABLE events (...)
USING DELTA
CLUSTER BY (user_id, event_type);
```

Liquid is the future direction; Z-ORDER is still widely used and supported. For new tables on Delta 3.2+, consider liquid clustering first.

## `VACUUM` — actually delete old files

Time travel relies on keeping old files. When you no longer need to time-travel past a certain point, VACUUM physically deletes the files that no version references anymore.

```sql
VACUUM delta.`/tables/events`;   -- retains last 7 days by default
VACUUM delta.`/tables/events` RETAIN 168 HOURS;
VACUUM delta.`/tables/events` DRY RUN;   -- list what would be deleted
```

What it does:
1. For each file currently in the table, check if it's referenced by any commit ≤ retention horizon.
2. Files not referenced by any retained version (and older than retention) are deleted.

### The famous warning

```sql
-- DON'T DO THIS IN PRODUCTION
VACUUM delta.`/tables/events` RETAIN 0 HOURS;
```

This deletes files that are no longer in the *current* version — but those files might still be referenced by versions you haven't expired yet. Time travel breaks. Concurrent reads break (they had file handles open).

To disable the safety check (don't), set:
```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
```

`RETAIN 168 HOURS` (7 days) is the default. Match `delta.deletedFileRetentionDuration`.

### The two retentions

| Setting | Default | What it covers |
|---|---|---|
| `delta.logRetentionDuration` | 30 days | How long old commit JSONs stay |
| `delta.deletedFileRetentionDuration` | 7 days | How long files marked `remove` stay on disk |

VACUUM only affects data files (the second). The log itself is cleaned up automatically when checkpoints are written.

## Recommended schedule

For a busy production table:

```text
- Continuous:    auto-optimize (if available) at write time
- Hourly/Daily:  OPTIMIZE (incremental, recent partitions)
- Weekly:        OPTIMIZE ZORDER BY (hot query keys) on whole table
- Weekly:        VACUUM with RETAIN 168 HOURS
- Yearly:        Review delta.logRetentionDuration / deletedFileRetentionDuration vs audit needs
```

For a small or rarely-changing table:

```text
- Monthly:       OPTIMIZE
- Monthly:       VACUUM
```

## Scale notes

- OPTIMIZE on a 1 TB table: roughly 1 hour on a 20-node cluster (read + write 1 TB). Schedule overnight.
- Z-ORDER adds ~20–50% to OPTIMIZE cost (sort overhead).
- VACUUM is metadata-cheap (a few list+delete operations per file). The bottleneck is object-store API rate limits — VACUUM on a million files = many minutes.
- Auto-optimize adds ~10–20% to write latency. Worth it for many small writers.

## Industry use case — Ishan's pattern

For an SNS-triggered Lambda writing Delta files into a data lake bronze layer:

```python
# Lambda writes one small Parquet per event via Delta
write_to_delta_bronze(...)
```

Without compaction, after a year of 1 event/min, you'd have ~500K files of a few KB each. Reads would crawl.

The standard remedy is a separate scheduled Spark job:

```python
# Daily Airflow / Lambda Step Functions job
spark.sql("OPTIMIZE bronze_events WHERE event_date = current_date() - 1")
spark.sql("VACUUM bronze_events RETAIN 168 HOURS")
```

Keeps bronze readable; deletes old files within retention.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Slow OPTIMIZE | Trying to compact too much at once | Use `WHERE` to scope to recent partitions |
| OPTIMIZE collides with writers | Files being rewritten while another job writes | Schedule during low traffic; or use partition predicate |
| VACUUM removed files needed for time travel | RETAIN was too short, or zero | Restore from backup; set retention generously |
| Reads hung after VACUUM | Reader still holding handles to vacuumed files | Wait for retention horizon; use longer retention |
| Z-ORDER doesn't seem to help | Column has low cardinality, or query doesn't filter on it | Verify the query plan shows reduced files-read |
| Too many small files even after OPTIMIZE | Writer using `coalesce(1)` per partition or many writers | Tune writer-side partitioning; auto-optimize |

## References

- *Delta Lake: The Definitive Guide* — Ch.9 "Optimization"
- Delta docs: https://docs.delta.io/latest/optimizations-oss.html
- [LS Ch.9 §"Compacting Files"]
- 📺 [Delta Lake OPTIMIZE and VACUUM — Databricks](https://www.youtube.com/results?search_query=delta+lake+optimize+vacuum+databricks)
- Liquid clustering: https://docs.delta.io/latest/delta-clustering.html
