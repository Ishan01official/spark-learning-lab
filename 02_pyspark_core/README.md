# 02 — PySpark Core APIs

The DataFrame API end-to-end: from `SparkSession` to writing results. This is the module you will return to most often once you start writing real jobs.

## What you should be able to do after this

- Configure a `SparkSession` for local dev and for a real cluster.
- Read and write every common format (CSV, JSON, Parquet, ORC, JDBC, Avro) and reason about the cost differences.
- Declare a schema explicitly and explain why `inferSchema` is dangerous on large inputs.
- Build columns and expressions fluently — `F.col`, `F.when`, `F.expr`, casts, `withColumn`, `selectExpr`.
- Use the most important built-in functions across string, date, numeric, and collection categories.
- Aggregate with `groupBy().agg()`, distinguishing per-key and global aggregations.
- Pick the right **join strategy** (broadcast hash / sort-merge / shuffle hash) and handle skew.
- Write window functions for ranking, lead/lag, running totals, and sessionization.
- Mix DataFrame API with Spark SQL — and know when SQL is the cleaner tool.
- Write UDFs only when needed, prefer Pandas (vectorized) UDFs when they are.

## Notes

1. [SparkSession — the entry point](01-spark-session.md)
2. [Reading data — CSV, JSON, Parquet, ORC, JDBC, Avro](02-reading-data.md)
3. [Writing data — modes, partitioning, bucketing](03-writing-data.md)
4. [Schemas and data types](04-schemas-and-types.md)
5. [Columns and expressions](05-columns-and-expressions.md)
6. [Built-in functions tour](06-built-in-functions.md)
7. [Aggregations](07-aggregations.md)
8. [Joins — strategies, broadcast, skew](08-joins-deep-dive.md)
9. [Window functions](09-window-functions.md)
10. [Spark SQL and the metastore](10-spark-sql.md)
11. [UDFs and Pandas UDFs](11-udfs-and-pandas-udfs.md)

## Examples

Each note has runnable code in `examples/`. Run them in order; later examples assume the data written by earlier ones.

## Book references for this module

- **Learning Spark 2e** — Ch.3 (Structured APIs), Ch.4 (Spark SQL & DataFrames), Ch.5 (Spark SQL & External Data Sources), Ch.6 (Spark SQL & Datasets), Ch.8 (Structured Streaming — only the DataFrame parts here).
- **High Performance Spark 2e** — Ch.3 (DataFrames, Datasets, Spark SQL), Ch.4 (Joins), Ch.5 (Effective Transformations).
- **Data Algorithms with Spark** — Ch.2-4 for canonical patterns (group/aggregate, join, window).
- **Databricks Certified Associate Developer for Apache Spark Using Python** — the bulk of cert topics live in this module.
