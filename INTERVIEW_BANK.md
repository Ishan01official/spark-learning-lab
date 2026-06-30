# Interview Bank

This is the master interview system. Each answer starts simple, then moves into production and interview depth.

## How To Use

1. Read the question.
2. Answer out loud in 60 seconds.
3. Read the short answer.
4. Expand with the deep answer.
5. Connect it to a repo file.

## Beginner

### Question: What is Apache Spark?

Short answer: Spark is a distributed processing engine for large-scale data.

Deep answer: Spark splits work across a cluster. The driver plans the job, executors run tasks on partitions, and Spark can process batch, SQL, machine learning, and streaming workloads using one execution engine.

Example: A 500 GB sales dataset does not fit comfortably in pandas on one laptop. Spark reads the data in partitions and processes those partitions in parallel.

Common mistake: Calling Spark a database. Spark is compute; storage is usually files, tables, or external systems.

Production example: A nightly ETL job reads raw cloud files, cleans them, and writes Delta tables for BI.

Follow-up questions: What is the driver? What is an executor? What is a partition?

Interviewer is testing: Whether you understand Spark's role in a data platform.

Related files: `00_setup/01-what-is-spark.md`, `ARCHITECTURE.md`.

### Question: Why is Spark lazy?

Short answer: Spark waits until an action is called so it can optimize the full execution plan.

Deep answer: Transformations build a logical plan. When an action runs, Catalyst optimizes the plan, Spark creates a physical plan, and the DAG scheduler splits it into stages and tasks.

Example: `df.filter(...).select(...)` does not run immediately. `df.count()` triggers the job.

Common mistake: Thinking every line of DataFrame code scans the data.

Production example: Spark can push filters and column pruning before reading a large Parquet table.

Follow-up questions: What is an action? What is a DAG? Where do you see this in Spark UI?

Interviewer is testing: Execution model and performance awareness.

Related files: `01_fundamentals/05-lazy-evaluation-and-dag.md`.

## Intermediate

### Question: Why prefer DataFrames over RDDs?

Short answer: DataFrames give Spark schema information, so Spark can optimize execution.

Deep answer: RDDs expose low-level distributed collections. DataFrames expose structured columns, types, and expressions. Catalyst can optimize DataFrames with predicate pushdown, projection pruning, join planning, and whole-stage code generation.

Example: `df.groupBy("country").count()` can be optimized as a relational plan. A Python RDD lambda is mostly opaque to Catalyst.

Common mistake: Using RDDs for normal ETL because they feel closer to Python.

Production example: DataFrame joins and aggregations are easier to tune and inspect in the SQL tab.

Follow-up questions: When would RDDs still be useful? Why does Python not have typed Datasets?

Interviewer is testing: API choice and optimization model.

Related files: `01_fundamentals/04-dataframes-vs-rdds.md`.

### Question: What is the difference between narrow and wide transformations?

Short answer: Narrow transformations do not require data movement across partitions; wide transformations do.

Deep answer: A narrow transformation can compute each output partition from one input partition. A wide transformation, such as `groupBy` or many joins, requires shuffle so rows with the same key meet on the same executor.

Example: `select` is narrow. `groupBy("customer_id").count()` is wide.

Common mistake: Assuming all transformations have similar cost.

Production example: A wide join on a skewed key can dominate the runtime of an entire pipeline.

Follow-up questions: What is shuffle? How do you see shuffle read/write in Spark UI?

Interviewer is testing: Performance fundamentals.

Related files: `01_fundamentals/06-narrow-vs-wide-transformations.md`, `03_optimization/07-shuffle-tuning.md`.

## Senior

### Question: How do you debug a slow Spark job?

Short answer: Start with Spark UI, find the slow stage, inspect shuffle, skew, spill, task time, and input size.

Deep answer: Compare stages by duration. If one stage dominates, inspect task distribution. Long tail tasks suggest skew. High shuffle read/write suggests expensive joins or aggregations. Spill suggests memory pressure. Bad scan metrics may mean no pruning. Then inspect the physical plan with `explain()`.

Example: A join stage has 199 tasks finishing in 20 seconds and one task taking 25 minutes. That is likely skew.

Common mistake: Changing executor memory first without identifying the bottleneck.

Production example: A customer table has a default customer id used by millions of records. Salting or data cleanup can fix the hot key.

Follow-up questions: What is spill? What is AQE skew join? When would you broadcast?

Interviewer is testing: Operational debugging maturity.

Related files: `13_debugging_playbook/README.md`, `14_spark_ui_lab/README.md`.

## Architect

### Question: How would you design a retail lakehouse?

Short answer: Use a medallion architecture with raw Bronze ingestion, cleaned Silver entities, and Gold business marts.

Deep answer: Land immutable raw data from POS, ecommerce, inventory, and clickstream. Use Bronze for replayable source records, Silver for conformed customers/products/orders/events, and Gold for revenue, inventory, marketing, and ML feature tables. Govern access through catalog permissions, validate data quality in Silver, and monitor freshness and failures.

Example: Orders stream to Bronze, customer updates arrive through CDC, Silver applies dedup and SCD2, Gold produces daily revenue and customer lifetime value.

Common mistake: Writing directly from raw data to dashboards with no replay, quality, or ownership boundary.

Production example: Backfilling a bad week should replay Bronze into Silver/Gold without duplicating records.

Follow-up questions: How do you partition tables? How do you handle late data? How do you enforce access control?

Interviewer is testing: End-to-end platform design and tradeoff communication.

Related files: `10_architecture/01_lakehouse_system_designs.md`, `16_cloud_lakehouse/README.md`.

## Coding

### Question: Write a deduplication pattern in PySpark.

Short answer: Use a window over the business key ordered by event time or ingest time, keep row number 1.

Deep answer: Dedup needs a deterministic rule. Choose keys, choose ordering, add `row_number`, filter to the survivor, and quarantine ambiguous records if the rule is not reliable.

Example:

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
deduped = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)
```

Common mistake: Calling `dropDuplicates(["order_id"])` without controlling which record survives.

Production example: CDC feeds often contain multiple updates per key. Keep the latest by source commit timestamp.

Follow-up questions: What if two records have the same timestamp? What if the key is skewed?

Interviewer is testing: Practical PySpark patterns and deterministic thinking.

Related files: `06_real_projects/dedup-at-scale/`.

## Troubleshooting

### Question: A job fails with executor OOM. What do you check?

Short answer: Check the failed stage, task input size, shuffle size, spill, skew, cached data, and executor memory settings.

Deep answer: OOM can be caused by oversized partitions, skew, collecting data to the driver, too many cached DataFrames, large broadcast joins, or Python UDF memory overhead. Fix the root cause before scaling compute.

Example: A single partition has 20 GB after a skewed groupBy. Repartitioning alone may not fix it if one hot key still owns most rows.

Common mistake: Increasing memory until the job passes once.

Production example: Add salting for hot keys, reduce selected columns before shuffle, and avoid caching wide intermediate data.

Follow-up questions: Driver OOM vs executor OOM? What does spill mean?

Interviewer is testing: Failure diagnosis and practical tuning.

Related files: `13_debugging_playbook/02_job_slow_or_oom.md`.

## Databricks

### Question: Job cluster or all-purpose cluster?

Short answer: Use job clusters for scheduled production jobs and all-purpose clusters for interactive development.

Deep answer: Job clusters are created for a job run and terminated after completion, improving isolation and cost control. All-purpose clusters are useful for notebooks and exploration but can accumulate state and cost.

Example: A daily Gold table refresh should use a job cluster. An analyst exploring data in a notebook can use an all-purpose cluster.

Common mistake: Running production jobs forever on a shared interactive cluster.

Production example: Use cluster policies to enforce node types, runtime versions, autotermination, and access modes.

Follow-up questions: How do cluster policies help? What is Photon? What is Unity Catalog?

Interviewer is testing: Managed platform operational knowledge.

Related files: `15_databricks_production/README.md`.

## Delta Lake

### Question: What does the Delta transaction log do?

Short answer: It records table commits so readers and writers see reliable table versions.

Deep answer: Delta tables store data in files and table state in `_delta_log`. Each commit records added files, removed files, metadata changes, and protocol information. This enables ACID behavior, time travel, schema enforcement, and reliable MERGE/UPDATE/DELETE.

Example: A failed write should not expose partial files as a committed table version.

Common mistake: Thinking Delta is just Parquet with a different file extension.

Production example: Time travel can restore a table version after a bad pipeline deployment.

Follow-up questions: What does VACUUM remove? What can break time travel?

Interviewer is testing: Lakehouse storage reliability.

Related files: `04_delta_lake/02-transaction-log.md`.

## Streaming

### Question: Why do streaming jobs need checkpoints?

Short answer: Checkpoints store progress and state so a streaming job can restart safely.

Deep answer: Structured Streaming tracks offsets, committed batches, and state store data in the checkpoint location. Without it, a restart cannot know what data was already processed or how to recover stateful aggregations.

Example: A Kafka stream writes to Delta. After restart, checkpoint offsets prevent rereading old Kafka messages as new data.

Common mistake: Deleting checkpoint folders to "fix" a streaming job without understanding duplicate risk.

Production example: Checkpoint paths should be stable, unique per query, and stored in reliable cloud storage.

Follow-up questions: What is watermarking? What is output mode? What is exactly-once?

Interviewer is testing: Streaming correctness.

Related files: `05_streaming/03-triggers-modes-checkpoints.md`.

## System Design

### Question: Design a near-real-time analytics pipeline.

Short answer: Ingest events continuously, write Bronze, process to Silver with watermarks, aggregate to Gold, and monitor latency and quality.

Deep answer: Use Kafka or cloud event streams as source, checkpointed Structured Streaming or Auto Loader for ingestion, Delta tables for storage, watermarks for late data, idempotent `foreachBatch` or MERGE for upserts, and dashboards from Gold tables. Define SLAs for freshness, completeness, and cost.

Example: Clickstream events update 5-minute funnel metrics for product analytics.

Common mistake: Promising "real time" without defining latency, late data handling, and replay.

Production example: Keep raw Bronze events for replay when parsing logic changes.

Follow-up questions: How do you recover from bad data? How do you backfill? How do you control costs?

Interviewer is testing: Architecture, correctness, and operational tradeoffs.

Related files: `10_architecture/01_lakehouse_system_designs.md`, `05_streaming/`.
