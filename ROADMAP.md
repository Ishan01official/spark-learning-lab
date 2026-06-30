# ROADMAP

Tick a box only after you have:
1. Read the notes.
2. Run every example in `examples/`.
3. Done at least one exercise.
4. Written the 5-sentence Feynman summary at the bottom of the module README.

## Tier 1 — Foundations

- [ ] **00_setup** — Spark explained, local install, Databricks Community, first job
- [ ] **01_fundamentals** — Cluster, RDD, DataFrame, DAG, lazy eval, narrow vs wide

## Tier 2 — Daily PySpark

- [ ] **02_pyspark_core**
  - [ ] SparkSession
  - [ ] Reading data (CSV / JSON / Parquet / ORC / JDBC)
  - [ ] Writing data (modes, partitioning, formats)
  - [ ] Schema and types
  - [ ] Column operations and built-in functions
  - [ ] Aggregations
  - [ ] Joins (broadcast, sort-merge, shuffle hash, skew)
  - [ ] Window functions
  - [ ] Spark SQL
  - [ ] UDFs and Pandas UDFs

- [ ] **03_optimization**
  - [ ] Catalyst optimizer
  - [ ] Tungsten engine
  - [ ] Reading explain plans
  - [ ] Adaptive Query Execution (AQE)
  - [ ] Partitioning strategies
  - [ ] Caching and persistence
  - [ ] Shuffle tuning
  - [ ] Skew handling (salting, AQE skew join)
  - [ ] Broadcast joins
  - [ ] Memory tuning
  - [ ] Spark UI walkthrough
  - [ ] Debugging common failures

## Tier 3 — Production stack

- [ ] **04_delta_lake**
- [ ] **05_streaming**
- [ ] **06_real_projects**
  - [ ] ETL pipeline pattern
  - [ ] Slowly Changing Dimensions Type 2
  - [ ] Dedup at scale
  - [ ] Data quality framework

## Tier 4 — Sharpening

- [ ] **07_interview_prep**
  - [ ] Databricks Certified Associate Developer mock exam 1
  - [ ] Mock exam 2
  - [ ] Troubleshooting scenarios
- [ ] **08_notes_from_books**
  - [ ] Learning Spark 2e — chapter notes
  - [ ] High Performance Spark 2e — chapter notes
  - [ ] Data Algorithms with Spark — chapter notes
- [ ] **09_latest_updates**

## Stretch projects (`06_real_projects/projects/`)

- [ ] `orders-etl/` — daily batch ETL from raw S3 → curated Delta
- [ ] `scd2-customers/` — SCD Type 2 with Delta MERGE
- [ ] `clickstream-stream/` — Structured Streaming + watermark + Kafka
- [ ] `dq-framework/` — declarative data quality checks (Great Expectations style)
