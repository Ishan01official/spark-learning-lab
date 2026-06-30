# 05 — Partitioning strategies

## Why this matters

"Partition" is overloaded in Spark — it means three different things:
1. **In-memory partition** — a chunk of data inside an RDD/DataFrame, processed by one task.
2. **Shuffle partition** — output partitions of an exchange, controlled by `spark.sql.shuffle.partitions`.
3. **On-disk partition** — directory layout when you write with `partitionBy`.

Each has its own knobs and gotchas. Mixing them up causes most "why is my job slow" tickets.

## In-memory partitioning

A DataFrame's parallelism = its current partition count. You influence it via:

| Operation | Effect |
|---|---|
| `spark.read.parquet(...)` | Partitions = #files × #row-groups (roughly); each input partition ~128 MB by default |
| `df.repartition(N)` | Full shuffle → exactly N partitions, hash-distributed |
| `df.repartition(N, "col")` | Full shuffle → N partitions, hash on `col` |
| `df.repartition("col")` | Shuffle → `spark.sql.shuffle.partitions` partitions, hash on col |
| `df.coalesce(N)` | No shuffle, just merges existing partitions into N (N ≤ current). Skews possible. |
| Wide transformation (join, groupBy) | Output gets `spark.sql.shuffle.partitions` |

### `repartition` vs `coalesce`

| | `repartition(N)` | `coalesce(N)` |
|---|---|---|
| Shuffle? | Yes, full | No, narrow |
| Can increase partitions? | Yes | No |
| Result balanced? | Yes (hash) | No (concatenated) |
| Cost | Expensive | Cheap |
| Use case | Before a heavy job; key-skew fix | Before writing to reduce small files |

### The 128 MB rule

A "good" partition size is 100–200 MB after decompression. Rules of thumb:
- **Too small** (e.g. 5 MB): task scheduling overhead dominates; thousands of tasks doing nothing useful.
- **Too large** (e.g. 1 GB): risk of OOM, single straggler, no parallelism for that chunk.

You can compute target partitions: `partitions ≈ total_input_bytes / 128MB`.

## Shuffle partitions

The output of every wide transformation. Default is 200; almost always wrong.

```python
spark.conf.set("spark.sql.shuffle.partitions", 400)
```

How to pick a value:
- Sum of cores in cluster × 2 to 4 (gives each core a few partitions of work).
- For a 64-core cluster: 200 is fine. For 1000 cores: 2000–4000.
- AQE coalesces post-shuffle so being too high is now mostly free.

[LS Ch.7 §"Configuring Spark"]

## On-disk partitioning (write side)

```python
df.write.partitionBy("country", "event_date").parquet("events/")
```

Layout:
```
events/
  country=US/event_date=2024-09-01/part-00000.snappy.parquet
  country=US/event_date=2024-09-02/part-00000.snappy.parquet
  country=DE/event_date=2024-09-01/part-00000.snappy.parquet
```

### Rules of thumb

- **Cardinality < ~10,000 total partitions** total across the table. Past that, listing the directory becomes the bottleneck.
- **Each leaf partition ~ a few GB**. Tiny partitions = the small-files problem.
- **First column = most-filtered**. Queries with `WHERE event_date = '...'` benefit from `partitionBy("event_date")` even without country.
- **Avoid high-cardinality columns** like `user_id`. Use bucketing instead.

### When `partitionBy` hurts

- Daily partition on a table with 100 rows/day → 365 tiny files per year.
- Partitioning by `event_time` (timestamp) — every row becomes its own partition.
- Wide partitions (one column has 80% of the data) — partition pruning helps everyone else, but that one partition is still a problem.

## Bucketing

```python
df.write.bucketBy(64, "user_id").sortBy("user_id").saveAsTable("orders_bucketed")
```

Bucketing fixes the high-cardinality problem: it pre-shuffles by hash(user_id) into a fixed number of files. Joins on `user_id` between two same-bucketed tables avoid the shuffle entirely (`BucketedHashJoin`).

Cost: bucketing is **only** honored when reading via `saveAsTable`/`spark.table()` — file-path reads ignore bucket metadata.

[HPS Ch.7 §"Bucketing"]

## Combined pattern (production)

```python
# Big fact table: partition by date for pruning, bucket by user for joins
(df
 .write
 .partitionBy("event_date")
 .bucketBy(128, "user_id")
 .sortBy("user_id")
 .mode("overwrite")
 .saveAsTable("events"))
```

You get both partition pruning (`WHERE event_date BETWEEN ...`) and shuffle-free joins on `user_id`.

## Scale notes

| Input size | Recommended partition count |
|---|---|
| < 1 GB | 4–16 |
| 1–10 GB | 32–128 |
| 10–100 GB | 200–800 |
| 100 GB–1 TB | 800–4000 |
| > 1 TB | 4000+ |

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Thousands of files written, each KB-sized | Wrote 200 shuffle partitions per `partitionBy` group | `coalesce(1)` per group, or `df.repartition("country", "event_date").write.partitionBy(...)` |
| "Too many open files" on the executor | `partitionBy` with high cardinality | Reduce cardinality or `spark.sql.sources.maxConcurrentWrites` |
| `coalesce(1)` OOM on driver | All data funneled to one task before write | Use `repartition(1)` (shuffle, not collapse) |
| Job is fast then slow at the end | One straggler partition (skew) | Repartition with salt, or enable AQE skew handling |
| Reading 50 columns of a 200-col table takes forever | Forgot Parquet — reading row-oriented | Convert to Parquet/ORC |

## References

- 📺 [Partitioning in Apache Spark — Holden Karau](https://www.youtube.com/results?search_query=spark+partitioning+holden+karau)
- [LS Ch.4 §"Partitioning"], [LS Ch.7 §"Optimizing and Tuning Spark"]
- [HPS Ch.7 §"Bucketing and Partitioning"]
- [DAS Ch.3 §"Partitioning Strategies"]
