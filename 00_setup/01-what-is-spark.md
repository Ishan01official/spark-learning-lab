# 01 — What is Spark?

## Why this matters

If you can't explain what Spark *is* in one sentence, you can't pick the right tool for a problem. Half of Spark's bad reputation comes from people using it on 5 GB of data and writing about how slow it is.

## What it is

**Apache Spark is a distributed execution engine for data.** You hand it code (PySpark, Scala, SQL) and a dataset that doesn't fit on one machine, and it splits the work across many machines, runs it in parallel, and gives you back a result.

That's the whole pitch. Three things in that sentence matter:

- **Distributed** — it runs on a cluster of machines, not one. The cluster can be 2 nodes (your laptop) or 2,000 nodes (a Databricks workspace at a bank).
- **Execution engine** — it doesn't store data. You point it at storage (S3, HDFS, ADLS, GCS, Kafka, JDBC) and it processes what you ask for.
- **For data** — its sweet spot is tabular data. It also does streaming, ML, and graphs, but the daily-driver use case is "I have terabytes of rows; transform them."

## How it works (one paragraph)

You write code on a driver (your laptop, a notebook, or a long-running JVM). The driver builds a logical plan, the Catalyst optimizer rewrites it, and the resulting physical plan is split into stages and tasks. Tasks are shipped to executors (JVM processes running on the cluster), each executor processes a chunk of the data in parallel, and results come back to the driver (or are written to storage).

Module [`01_fundamentals/01-cluster-architecture-deep-dive.md`](../01_fundamentals/01-cluster-architecture-deep-dive.md) draws the picture.

## When to use Spark vs not

**Use Spark when:**

- Your data is bigger than fits comfortably in RAM on one machine (rule of thumb: ≥ 100 GB at rest, or ≥ 10 GB in memory after parsing).
- You're already on a Spark stack (Databricks, EMR, Synapse, Dataproc, Glue).
- Your transformations are tabular: joins, aggregations, windows, SQL.
- You need a unified engine for batch + streaming + ML against the same datasets.

**Don't use Spark when:**

- Your data fits in one pandas DataFrame (under ~10 GB on a beefy machine). pandas / Polars / DuckDB will be 5–50× faster with zero cluster overhead.
- You need millisecond OLTP queries — Spark is throughput-optimized, not latency-optimized. Use a database.
- You only need to move files. `aws s3 cp` exists.

This is the single most important judgement call. *Learning Spark 2e* spends Chapter 1 making it and [HPS Ch.1] hammers on "Spark is not magic, it has overhead." Internalize this before going further.

## Scale notes

| Scenario | Reasonable tool |
|---|---|
| 1 GB CSV, one-off analysis | pandas / DuckDB |
| 50 GB Parquet on S3, daily job | Spark (small cluster), or DuckDB on a big EC2 |
| 5 TB partitioned data, hourly transform | Spark on a managed cluster |
| 100 TB+, multi-tenant warehouse | Spark + Delta/Iceberg, or Snowflake/BigQuery |
| 1 KB row, sub-100ms read | Postgres / DynamoDB. Not Spark. |

## Failure modes (already, at the conceptual stage)

- **"Spark is slow on my 1 GB file"** — that's because Spark has 2–5 seconds of startup overhead and shuffles cost a fixed amount regardless of data size. Below ~10 GB, plain pandas wins.
- **"Spark crashed with OOM on my 500 GB job"** — usually a *single executor* ran out of memory, often because of skew (one partition has 90% of the rows) or a huge `collect()` to the driver. Module 03 covers the diagnosis.

## References

- [LS Ch.1 — "Introduction to Apache Spark"]
- [HPS Ch.1 — "Introduction to High Performance Spark"]
- [DAS Ch.1 — "Introduction to Spark and PySpark"]
- 📺 [Apache Spark Core—Practical Optimization — Daniel Tomes (Databricks)](https://www.youtube.com/watch?v=daXEp4HmS-E) — long but the single best free Spark talk. Skim now, revisit after Module 03.
