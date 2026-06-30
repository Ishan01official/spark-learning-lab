# 01 — Why Delta

## Why this matters

Before Delta (and Iceberg / Hudi), Spark on a data lake had a real problem: **Parquet doesn't have a transaction.** That means:

- A failed `write` leaves partial files behind. The next read sees them.
- Two writers to the same path race. One overwrites the other.
- "Delete this row" required rewriting the whole table.
- "What did the table look like last Tuesday" was an offline restore.
- Schema changes meant either a coordinated migration or accepting query failures.

Delta solves all of this by adding a **transaction log** on top of Parquet — no new storage engine, no proprietary format, just metadata and discipline.

## The four guarantees

| | Plain Parquet | Delta |
|---|---|---|
| **A**tomicity | A failed write leaves half-written files | Either all new files visible or none |
| **C**onsistency | Reader can see a half-completed write | Reader sees one consistent version |
| **I**solation | Two writers can clobber each other | Optimistic concurrency control |
| **D**urability | Once written, fine | Same; backed by object storage |

Plus features that are *not* ACID but only possible with a transaction log:

- **Time travel** — query the table at version N or at timestamp T.
- **Schema evolution & enforcement** — add columns safely; reject incompatible writes.
- **`MERGE INTO`** — upserts/deletes/conditional inserts without rewriting whole partitions.
- **CDF (Change Data Feed)** — read row-level changes between two versions, like a database WAL.

## The pattern Delta replaces

```
# Old way: "atomic" overwrite via rename
df.write.parquet("s3://bucket/table_tmp/")
# atomically rename table_tmp -> table
# (impossible on S3, which has no atomic rename)
```

S3 doesn't have atomic rename, so before Delta, "atomic overwrite" on a data lake was a polite fiction. Delta makes it real by writing new files alongside the old ones and *atomically appending a log entry* that marks which files are now live.

## What Delta is *not*

- **Not a database.** No indexes (only Z-ordering, which is data layout), no foreign keys, no constraint checks beyond column nullability and CHECK constraints.
- **Not row-store.** Still Parquet underneath, columnar, optimized for analytics.
- **Not real-time OLTP.** Latency is in seconds (write a file, append a log entry), not milliseconds.
- **Not a query engine.** It's a storage format. Query engines (Spark, Trino, DuckDB, Snowflake) read it.

## When you should use Delta

- Any time you'd otherwise use Parquet for a *mutable* dataset.
- Any time you need rollback, audit, or "what changed since X".
- Any time multiple writers might touch the same table.
- CDC ingestion / SCD2 pipelines.

## When you might skip Delta

- Truly immutable archival data (append-only, never updated, no time travel needed).
- Tiny datasets where the overhead of the transaction log dominates.
- Read-only consumers where the source already provides ACID (queried via Lakehouse Federation or external catalog).

## References

- *Delta Lake: The Definitive Guide* — Ch.1
- [LS Ch.9 §"The Need for a New Data-Source Format"]
- 📺 [What is Delta Lake — Databricks Academy](https://www.youtube.com/results?search_query=what+is+delta+lake+databricks)
- 📺 [Delta Lake: An Open Approach to ACID Transactions — Michael Armbrust](https://www.youtube.com/results?search_query=delta+lake+michael+armbrust+spark+summit)
