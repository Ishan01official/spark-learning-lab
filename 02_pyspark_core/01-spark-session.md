# 01 — SparkSession: the entry point

## Why this matters

Every PySpark program starts with a `SparkSession`. The flags you set here decide your shuffle parallelism, memory layout, Hive integration, AQE behavior, and whether your job is reproducible across environments. Most "why is this slow?" answers start with a misconfigured session.

## What it is

`SparkSession` (since Spark 2.0) is the unified entry point. It wraps the older `SparkContext`, `SQLContext`, and `HiveContext`. From it you get:

- `spark.read` — the DataFrameReader.
- `spark.sql("SELECT ...")` — run SQL against registered tables.
- `spark.table("db.table")` — load a managed table.
- `spark.conf` — runtime configuration.
- `spark.catalog` — databases, tables, functions, temp views.
- `spark.sparkContext` — escape hatch to the lower-level RDD API.

## How to build one — line by line

```python
from pyspark.sql import SparkSession                       # the class
spark = (
    SparkSession.builder                                    # builder pattern
    .appName("orders-etl")                                  # name shown in Spark UI + cluster manager
    .master("local[*]")                                     # local: use all cores. omit in real clusters.
    .config("spark.sql.shuffle.partitions", "200")          # post-shuffle partition count (default 200)
    .config("spark.sql.adaptive.enabled", "true")           # AQE: dynamically coalesces / handles skew at runtime
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.session.timeZone", "UTC")            # avoid locale surprises in date casts
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # faster than Java
    .config("spark.sql.parquet.compression.codec", "snappy")
    .enableHiveSupport()                                    # only if you need a real metastore. local: optional.
    .getOrCreate()                                          # returns existing session if one is active
)
spark.sparkContext.setLogLevel("WARN")                      # quiet INFO-level firehose
```

**Why `getOrCreate()`?** In notebooks and Databricks the session is already created for you. Calling `getOrCreate()` returns that existing session; calling `new SparkSession()` would not. Always use the builder.

## When to set what

| Config | Local dev | Small cluster | Production |
| --- | --- | --- | --- |
| `spark.sql.shuffle.partitions` | 8–16 (lower than 200 default; small data) | 200 | 2–4× total executor cores |
| `spark.sql.adaptive.enabled` | true | true | true (Spark 3.2+) |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | 10–100 MB | 100 MB if memory allows |
| `spark.sql.files.maxPartitionBytes` | 128 MB (default) | 128–256 MB | 128 MB (avoid huge tasks) |
| `spark.executor.memory` | n/a (driver only) | 4–8 GB | 8–32 GB |
| `spark.executor.cores` | n/a | 4 | 4–5 |
| `spark.serializer` | Kryo | Kryo | Kryo |

## Scale notes

| Setting | Effect at scale |
| --- | --- |
| `shuffle.partitions=200` on 10 GB shuffle | each task ~50 MB → fine |
| `shuffle.partitions=200` on 5 TB shuffle | each task ~25 GB → OOM. Raise to 5000–10000. |
| `shuffle.partitions=200` on 50 MB shuffle | each task 250 KB → scheduling overhead dwarfs work. Lower to 8–16, or rely on AQE coalesce. |
| `autoBroadcastJoinThreshold=10MB` on a 200 MB lookup | broadcast not used → falls back to sort-merge → unnecessary shuffle. Bump threshold or hint `broadcast()`. |

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| `WARN SparkSession … getOrCreate returned existing` | a session is already active (notebook) | use the existing `spark` |
| `pyspark.sql.utils.AnalysisException: Hive support not enabled` | called `spark.sql("CREATE TABLE ... USING parquet PARTITIONED BY ...")` without metastore | add `.enableHiveSupport()` and set warehouse dir |
| `Cannot modify the value of a Spark config: spark.executor.memory` | tried to set static config after session start | set in builder, or via `spark-submit --conf` |
| `Initial job has not accepted any resources` | cluster manager can't allocate (wrong master URL, no executors) | check `--master`, K8s/YARN cluster health |

## Stopping and restarting

```python
spark.stop()                  # release executors and slots
```
Always call `spark.stop()` at the end of standalone scripts. In notebooks/Databricks, don't.

## References

- 📚 [LS Ch.4 §"The SparkSession"]
- 📚 [HPS Ch.3 §"SparkSession Entry Point"]
- 📚 [DAS Ch.1 §"Spark Programming Model"]
- 📺 [Apache Spark SparkSession — Databricks (YouTube)](https://www.youtube.com/results?search_query=apache+spark+sparksession+databricks)
- 📺 [Spark Configuration Properties — official docs walkthrough](https://spark.apache.org/docs/latest/configuration.html)
