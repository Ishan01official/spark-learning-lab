# Learning Spark, 2e — chapter notes

Damji, Wenig, Das, Lee. O'Reilly, 2020. 400 pages.

The foundational book for the Spark DataFrame API. Suitable as a first read, covers everything the Associate Developer cert tests on.

---

## Ch.1 — Introduction to Apache Spark

**Key concepts**
- Spark's lineage from MapReduce; what made it faster (in-memory, DAG).
- The unified analytics engine: SQL, streaming, ML, graph — same engine.
- Components: Spark Core, Spark SQL, MLlib, Structured Streaming, GraphFrames.
- Cluster managers: standalone, YARN, Kubernetes, Mesos (legacy).
- Programming languages: Scala, Java, Python, R.

**Takeaway**: Spark is the engine; you talk to it through the DataFrame API in whatever language. The DataFrame API became the single canonical interface in 2.0.

**This lab**: module 00 covers the same ground at a faster pace.

---

## Ch.2 — Downloading Apache Spark and Getting Started

**Key concepts**
- Local install + the M&M counting example.
- `spark-shell` (Scala), `pyspark` (Python REPL), `spark-submit`.
- Anatomy of a Spark application: driver, executors, tasks.

**Most useful API**: `spark-submit` flags.

**Takeaway**: A Spark app is a driver process orchestrating executor processes.

**This lab**: module 00 `03-local-setup.md`.

---

## Ch.3 — Apache Spark's Structured APIs

**Key concepts**
- Why structured APIs (DataFrames, Datasets) — they enable Catalyst.
- The high-level types: Row, Column, DataFrame.
- Schema inference vs explicit schemas.
- Creating DataFrames from various sources.

**Most useful APIs**:
```python
spark.createDataFrame(data, schema)
df.schema
df.printSchema()
```

**Takeaway**: DataFrames have schemas; that's what unlocks optimization.

**This lab**: module 01 `04-DataFrames-vs-RDDs.md`, module 02 entirely.

---

## Ch.4 — Spark SQL and DataFrames

**Key concepts**
- Catalog: tables, views, databases.
- SQL on DataFrames: `createOrReplaceTempView`, `spark.sql(...)`.
- Reading and writing files (formats, options).
- Managed vs unmanaged (external) tables.

**Most useful APIs**:
```python
df.createOrReplaceTempView("name")
spark.sql("SELECT * FROM name WHERE ...")
spark.read.option(...).format(...).load(path)
df.write.mode(...).format(...).save(path)
```

**Takeaway**: SQL and DataFrame API compile to the same Catalyst plans — pick whichever reads more naturally.

**This lab**: module 02 `10-spark-sql.md`.

---

## Ch.5 — Spark SQL and DataFrames: Interacting with External Data Sources

**Key concepts**
- JDBC, Avro, Parquet, ORC, CSV, JSON specifics.
- Pushdown of filters and projections.
- Multi-file reads; partition discovery.

**Most useful APIs**:
```python
# JDBC
df = spark.read.format("jdbc").option("url", ...).option("dbtable", ...).load()
# Partition by column (parallel JDBC reads)
.option("partitionColumn", "id").option("lowerBound", 0).option("upperBound", 1000000).option("numPartitions", 10)
```

**Takeaway**: Parquet is the default for a reason. JDBC reads need partitioning to scale.

**This lab**: module 02 `02-reading.md`, `03-writing.md`.

---

## Ch.6 — Spark SQL and Datasets

**Key concepts**
- Dataset = typed DataFrame (Scala/Java only — Python skips this).
- Encoders, compile-time type safety.
- Tradeoffs vs DataFrames (lose Catalyst's flexibility in some cases).

**Takeaway** (Python users): Skim. Datasets are a Scala API; PySpark only has DataFrame.

---

## Ch.7 — Optimizing and Tuning Spark Applications

**Key concepts**
- Spark UI deep dive: Jobs, Stages, Storage, Environment, Executors, SQL.
- AQE: coalesce, switch join strategy, skew handling.
- Caching: levels, when, when not.
- Common slowness patterns.

**Most useful APIs**:
```python
df.cache(); df.persist(StorageLevel.MEMORY_AND_DISK)
df.explain("formatted")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**Takeaway**: Read the Spark UI early. Most "Spark is slow" turns out to be skew, bad join strategy, or caching misuse.

**This lab**: module 03 entirely.

---

## Ch.8 — Structured Streaming

**Key concepts**
- The model: stream = unbounded table.
- Sources (Kafka, file, rate), sinks (Delta, console, file, foreachBatch).
- Triggers, output modes, checkpointing.
- Stateful operations: windows, aggregation, dedup, joins.
- Watermarks.
- Exactly-once.

**Most useful APIs**:
```python
spark.readStream.format(...).load()
stream.writeStream.format(...).outputMode(...).option("checkpointLocation", ...).start()
stream.withWatermark("ts", "10 minutes").groupBy(F.window("ts", "5 min")).count()
```

**Takeaway**: Streaming IS batch — same Catalyst, same DataFrame ops. Differences are in checkpointing, watermarks, triggers, output mode.

**This lab**: module 05 entirely.

---

## Ch.9 — Building Reliable Data Lakes with Apache Spark

**Key concepts**
- Why lakehouse > traditional data lake (transactions, schema enforcement).
- Delta Lake basics: transactions, schema enforcement, time travel.
- Reading and writing Delta tables.
- OPTIMIZE and Z-ORDER.
- Streaming with Delta.

**Most useful APIs**:
```python
df.write.format("delta").save(path)
spark.sql("OPTIMIZE ... ZORDER BY (col)")
DeltaTable.forPath(spark, path).history()
```

**Takeaway**: Delta turns S3/HDFS into a transactional warehouse. The combination of "object storage + Delta + Spark" is the modern data platform.

**This lab**: module 04 entirely.

---

## Ch.10 — Machine Learning with MLlib

**Key concepts** (skim if focus is data engineering)
- MLlib's DataFrame-based pipeline API.
- Transformers, estimators, pipelines.
- Feature engineering, model training, evaluation.

**Takeaway**: Spark MLlib is functional but most teams use external (sklearn, XGBoost, ML platforms) and only use Spark for ETL.

---

## Ch.11 — Managing, Deploying, and Scaling Machine Learning Pipelines with Apache Spark

(Same — skim for non-ML use cases.)

---

## Ch.12 — Epilogue: Apache Spark 3.0

**Key concepts**
- AQE (covered above).
- Dynamic partition pruning.
- ANSI SQL compatibility.
- New built-in functions.

**Takeaway**: Spark 3.0 was the biggest leap since 2.0. Spark 3.5+ continues the trend; Spark 4.0 (preview) takes ANSI further.

**This lab**: module 09 `01-spark-3.5-features.md`.

---

## Overall takeaways for the cert

Chapters 3, 4, 5, 7, 8, 9 are the heart of Associate Developer exam material. The book's code examples are great drill material; work through Ch.4 and Ch.5's exercises specifically.
