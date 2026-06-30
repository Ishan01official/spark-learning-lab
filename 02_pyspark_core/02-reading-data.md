# 02 — Reading data: CSV, JSON, Parquet, ORC, JDBC, Avro

## Why this matters

The biggest single optimization in most pipelines is *choosing the right input format*. CSV vs Parquet on the same data is routinely a 10–100× difference in scan time and dollars.

## The unified reader API

```python
df = (spark.read
        .format("parquet")      # or csv / json / orc / jdbc / avro / delta
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(my_schema)      # optional but recommended
        .load("s3://bucket/path/"))
```

Convenience methods exist: `spark.read.parquet(...)`, `spark.read.csv(...)`, etc. They are just sugar over the above.

## Format-by-format

### CSV — convenient, slow

```python
df = (spark.read
        .option("header", "true")            # first row is column names
        .option("inferSchema", "false")      # ALWAYS false on real data
        .option("delimiter", ",")            # default is comma
        .option("quote", '"')                # default is "
        .option("escape", "\\")              # default is backslash
        .option("multiLine", "false")        # set true if quoted fields contain newlines
        .option("mode", "PERMISSIVE")        # PERMISSIVE | DROPMALFORMED | FAILFAST
        .option("columnNameOfCorruptRecord", "_corrupt")  # required for PERMISSIVE w/ schema
        .option("nullValue", "")             # how nulls are encoded in input
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(orders_schema)               # critical for performance
        .csv("data/orders.csv"))
```

**Why `inferSchema=false` matters:** with inference, Spark scans the *entire file* once just to figure out types, then scans again to read. Two full passes. Worse: inference can pick `string` when you wanted `int`, breaking arithmetic downstream silently.

**Industry use case:** CSV is *only* acceptable as an arrival format (vendors send CSV). Convert to Parquet on landing and never read the CSV again.

### JSON — flexible, slow, schema-prone

```python
df = (spark.read
        .option("multiLine", "true")         # one JSON object spans multiple lines
        .option("mode", "PERMISSIVE")
        .schema(events_schema)               # nested StructType
        .json("s3://bucket/events/*.json"))
```

Two layouts exist:
- **NDJSON / JSONL** — one JSON object per line. `multiLine=false`. Parallelizable.
- **JSON array** — single `[ {...}, {...} ]` document. `multiLine=true`. **Not parallelizable** — one task reads the whole file.

**Industry use case:** API ingest, semi-structured event streams. Always flatten and re-write as Parquet for analysis.

### Parquet — the default for data lakes

```python
df = spark.read.parquet("s3://bucket/orders/")
```
Properties that make Parquet the right choice:

- **Columnar** — reading 3 columns out of 100 reads only those 3 columns' bytes from disk (projection pushdown).
- **Compressed** — typically Snappy (fast) or ZSTD (smaller). 3–10× smaller than equivalent CSV.
- **Self-describing** — schema lives inside each file footer; no inference needed.
- **Statistics** — min/max/null counts per row group enable *predicate pushdown* — Spark skips entire row groups that can't match the filter.
- **Splittable** — multi-GB files parallelize cleanly across executors.

Read with explicit columns + filter to maximize pushdown:
```python
df = (spark.read.parquet("s3://bucket/orders/")
        .select("order_id", "customer_id", "amount")    # projection pushdown
        .filter(F.col("amount") > 100))                 # predicate pushdown
```

**Industry use case:** every analytics workload in modern data platforms (Databricks, Snowflake external tables, EMR, Synapse Spark).

### ORC — Parquet's cousin

```python
df = spark.read.orc("hdfs://cluster/warehouse/orders/")
```
Functionally equivalent to Parquet for most Spark use cases. Slightly better on string-heavy Hive workloads; Parquet dominates everywhere else. If you don't already have ORC, pick Parquet.

### JDBC — relational sources

```python
df = (spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://db.example.com:5432/prod")
        .option("dbtable", "(SELECT id, amount, ts FROM orders WHERE ts > '2024-01-01') AS o")
        .option("user", user)
        .option("password", pw)
        .option("driver", "org.postgresql.Driver")
        .option("partitionColumn", "id")     # required for parallel reads
        .option("lowerBound", 1)
        .option("upperBound", 10_000_000)
        .option("numPartitions", 16)         # 16 parallel connections
        .option("fetchsize", 10000)
        .load())
```

**Without `partitionColumn` + bounds**, JDBC reads use **one connection** — one executor pulls the whole table. With them, Spark issues 16 parallel `WHERE id BETWEEN ? AND ?` queries.

**Industry use case:** initial load (bootstrap) from OLTP into the lake; never for streaming or hot incremental loads — use CDC tools (Debezium, AWS DMS) instead.

### Avro — row-based, schema-evolving

```python
df = spark.read.format("avro").load("s3://bucket/events/")
```
Requires the `spark-avro` package. Row-oriented (good for write-heavy event streams, weaker for analytical reads than Parquet). Often the wire format for Kafka.

## Reading hierarchies and wildcards

```python
spark.read.parquet(
    "s3://bucket/orders/year=2024/month=*/day=*/",   # glob
    "s3://bucket/orders/year=2025/")                  # multi-path

# Or with options:
(spark.read
   .option("recursiveFileLookup", "true")     # recurse all subdirs
   .option("pathGlobFilter", "*.parquet")     # only files matching glob
   .parquet("s3://bucket/orders/"))
```

When the path uses Hive-style `key=value/` directories, Spark *infers them as columns* automatically — `year`, `month`, `day` appear as columns even though they aren't inside the files.

## Scale notes

| Format | 100 MB | 10 GB | 1 TB |
| --- | --- | --- | --- |
| CSV (gzip) | 30 s, 1 task | 30 min, splittable only if not gzip | unusable |
| JSON (multiLine) | 20 s | 30 min, 1 task per file | unusable |
| Parquet | 3 s | 1–2 min | 20–60 min |
| ORC | 3 s | 1–2 min | 20–60 min |
| JDBC (1 conn) | 60 s | hours | days |
| JDBC (16 partitioned) | 10 s | 5–15 min | hours |

Numbers assume a small cluster (~32 cores). They are orders of magnitude, not benchmarks.

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| All work on one task / OOM on driver | `gzip` CSV (not splittable) or `multiLine=true` JSON | repartition right after read; convert to Parquet |
| `_corrupt_record` column appears | bad row in PERMISSIVE mode | inspect, then DROPMALFORMED or FAILFAST in next run |
| `inferSchema` job takes minutes before any real work | second-pass inference | declare schema explicitly |
| JDBC reads hang for hours | no partitioning options | add `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` |
| `UnsupportedOperationException: schema mismatch` reading Parquet | schemas drift across files | set `spark.sql.parquet.mergeSchema=true` (slow) or unify upstream |
| Mysterious empty result | `mode=DROPMALFORMED` ate everything | switch to PERMISSIVE, count `_corrupt_record` |

## References

- 📚 [LS Ch.5 §"DataFrameReader" / "Data Sources"]
- 📚 [HPS Ch.3 §"Loading Data" / Ch.7 (Going Beyond Scala)] for JDBC
- 📚 [DAS Ch.2 §"Reading Data into RDDs and DataFrames"]
- 📺 [Apache Spark — Parquet Deep Dive (YouTube searches: "Parquet format Databricks")](https://www.youtube.com/results?search_query=parquet+format+databricks+deep+dive)
- 📺 [JDBC tuning in Spark (YouTube: "spark jdbc partitioning")](https://www.youtube.com/results?search_query=spark+jdbc+partitioning)
