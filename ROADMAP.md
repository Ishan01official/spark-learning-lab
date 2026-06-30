# Master Roadmap

Use this roadmap as the main path from absolute beginner to Spark architect.

Only tick an item after you have:

1. Read the note.
2. Run the code.
3. Broken one thing on purpose.
4. Debugged the failure.
5. Written a short explanation in your own words.

## Level 0 - Setup And Prerequisites

Goal: become comfortable enough with the tools that Spark itself can be the hard part.

- [ ] Python basics for PySpark: functions, lists, dictionaries, comprehensions, modules, virtual environments.
- [ ] SQL basics: `SELECT`, `WHERE`, `GROUP BY`, joins, windows, nulls, CTEs.
- [ ] Linux and terminal basics: paths, environment variables, processes, ports, logs.
- [ ] Git and GitHub basics: clone, branch, commit, push, pull request, README hygiene.
- [ ] Java/JDK basics: why Spark needs the JVM, `JAVA_HOME`, Java version checks.
- [ ] Local Spark setup: `make setup`, `make smoke`, Spark UI on port `4040`.
- [ ] Databricks workspace setup: notebooks, clusters, jobs, repos, DBFS, secrets.
- [ ] Docker basics if useful: image, container, volume, environment variables.

Repo modules:

- `00_setup/`
- `17_sql_for_spark/`
- `18_python_for_pyspark/`
- `20_learning_strategy/`

## Level 1 - Spark Fundamentals

Goal: understand the execution model before writing complex PySpark.

- [ ] What Spark is and why it exists.
- [ ] Spark vs Hadoop MapReduce.
- [ ] Cluster architecture: driver, executors, cluster manager.
- [ ] Jobs, stages, tasks, partitions.
- [ ] DAG and lazy evaluation.
- [ ] Transformations vs actions.
- [ ] Narrow vs wide transformations.
- [ ] RDD vs DataFrame vs Dataset.
- [ ] SparkSession.
- [ ] Spark UI basics.

Repo modules:

- `01_fundamentals/`
- `14_spark_ui_lab/`

## Level 2 - Daily PySpark

Goal: write the PySpark code used in normal data engineering work.

- [ ] Read and write CSV, JSON, Parquet, Delta, JDBC.
- [ ] Schema inference vs explicit schema.
- [ ] DataFrame transformations: `select`, `filter`, `where`, `withColumn`.
- [ ] Conditional logic with `when` and `otherwise`.
- [ ] Aggregations with `groupBy`.
- [ ] Joins and join types.
- [ ] Window functions.
- [ ] Explode and nested data.
- [ ] Arrays, structs, maps.
- [ ] Null handling.
- [ ] Date and timestamp functions.
- [ ] UDFs and why to avoid them when built-ins exist.
- [ ] Spark SQL and temp views.
- [ ] Common PySpark patterns.

Repo modules:

- `02_pyspark_core/`
- `17_sql_for_spark/`

## Level 3 - Production Data Engineering

Goal: build reliable pipelines, not just passing notebooks.

- [ ] Medallion architecture.
- [ ] Bronze, Silver, Gold layers.
- [ ] Batch ETL.
- [ ] Incremental loads.
- [ ] CDC concepts.
- [ ] SCD Type 1 and Type 2.
- [ ] Deduplication.
- [ ] Data quality checks.
- [ ] Schema validation.
- [ ] Error handling.
- [ ] Logging.
- [ ] Retry patterns.
- [ ] Idempotent pipelines.
- [ ] Partitioning strategy.
- [ ] File sizing strategy.
- [ ] Small file problem.
- [ ] Production folder structure.
- [ ] Configuration-driven pipelines.

Repo modules:

- `06_real_projects/`
- `13_debugging_playbook/`

## Level 4 - Spark Optimization

Goal: diagnose cost, slowness, skew, memory pressure, and bad plans.

- [ ] Catalyst optimizer.
- [ ] Tungsten engine.
- [ ] Adaptive Query Execution.
- [ ] Predicate, projection, and partition pruning.
- [ ] Broadcast, shuffle hash, and sort merge joins.
- [ ] Shuffle partitions.
- [ ] `repartition` vs `coalesce`.
- [ ] `cache` vs `persist`.
- [ ] Skew handling and salting.
- [ ] Bucketing.
- [ ] Dynamic partition pruning.
- [ ] Executor and driver memory.
- [ ] Spill and garbage collection.
- [ ] Spark UI debugging.
- [ ] Query plans and `explain()`.
- [ ] Whole-stage code generation.
- [ ] Performance anti-patterns.

Repo modules:

- `03_optimization/`
- `14_spark_ui_lab/`
- `11_case_studies/`

## Level 5 - Delta Lake

Goal: operate a reliable lakehouse storage layer.

- [ ] Why Delta Lake exists.
- [ ] Transaction log and ACID.
- [ ] Time travel.
- [ ] `MERGE`, `UPDATE`, `DELETE`.
- [ ] `VACUUM`, `OPTIMIZE`, `ZORDER`.
- [ ] Schema enforcement and evolution.
- [ ] Change Data Feed.
- [ ] Delta table design.
- [ ] Delta performance tuning.
- [ ] Delta troubleshooting.

Repo modules:

- `04_delta_lake/`
- `06_real_projects/`

## Level 6 - Structured Streaming

Goal: build and operate streaming jobs with state, checkpoints, and late data.

- [ ] Batch vs streaming.
- [ ] Structured Streaming model.
- [ ] Sources and sinks.
- [ ] Kafka basics.
- [ ] Checkpointing.
- [ ] Watermarking and late arriving data.
- [ ] Output modes and trigger types.
- [ ] Exactly-once concepts.
- [ ] Stateful streaming.
- [ ] Stream-stream joins.
- [ ] Streaming deduplication.
- [ ] Streaming troubleshooting.
- [ ] Monitoring streaming jobs.

Repo modules:

- `05_streaming/`
- `13_debugging_playbook/`

## Level 7 - Databricks Production Stack

Goal: move from local Spark to managed production Spark.

- [ ] Workspace basics.
- [ ] Clusters, jobs, workflows, notebooks, repos.
- [ ] DBFS and cloud storage.
- [ ] Unity Catalog.
- [ ] Access modes and cluster policies.
- [ ] Job clusters vs all-purpose clusters.
- [ ] Databricks Runtime and Photon.
- [ ] Lakeflow Declarative Pipelines / Delta Live Tables concepts.
- [ ] Auto Loader.
- [ ] Secrets and service principals.
- [ ] CI/CD basics.
- [ ] Monitoring Databricks jobs.
- [ ] Common Databricks production failures.

Repo modules:

- `15_databricks_production/`
- `09_latest_updates/`

## Level 8 - Cloud And Architecture

Goal: design lakehouse platforms, not just single jobs.

- [ ] Spark on cloud.
- [ ] Data lake and lakehouse architecture.
- [ ] Storage layout.
- [ ] IAM and security basics.
- [ ] Cost optimization.
- [ ] Multi-environment design.
- [ ] Dev/test/prod setup.
- [ ] Orchestration with Airflow, ADF, or Databricks Workflows.
- [ ] Observability.
- [ ] Data contracts.
- [ ] Governance and lineage.
- [ ] Disaster recovery.
- [ ] SLA/SLO thinking.
- [ ] Architect-level tradeoffs.

Repo modules:

- `10_architecture/`
- `16_cloud_lakehouse/`

## Level 9 - Real-World Projects

Goal: build portfolio-grade systems with code, diagrams, tests, exercises, and interview explanations.

- [ ] Basic ETL pipeline.
- [ ] Sales analytics pipeline.
- [ ] Incremental load pipeline.
- [ ] SCD Type 2 customer dimension.
- [ ] Deduplication at scale.
- [ ] Data quality framework.
- [ ] Clickstream pipeline.
- [ ] Streaming Kafka pipeline.
- [ ] Delta Lake medallion pipeline.
- [ ] Performance tuning case study.
- [ ] Skewed join debugging project.
- [ ] Small files optimization project.
- [ ] Production incident RCA project.
- [ ] End-to-end lakehouse project.
- [ ] Architect-level data platform design.

Repo modules:

- `06_real_projects/`
- `11_case_studies/`
- `PROJECT_INDEX.md`

## Level 10 - Interview Preparation

Goal: answer beginner, coding, senior, production, and architect questions with examples.

- [ ] Beginner questions.
- [ ] Intermediate questions.
- [ ] Senior questions.
- [ ] Architect questions.
- [ ] Coding questions.
- [ ] Scenario questions.
- [ ] Troubleshooting questions.
- [ ] Databricks questions.
- [ ] Delta Lake questions.
- [ ] Streaming questions.
- [ ] Optimization questions.
- [ ] System design questions.

Repo modules:

- `07_interview_prep/`
- `12_certification_prep/`
- `INTERVIEW_BANK.md`

## Weekly Operating Rhythm

- Monday: read one concept and run one example.
- Tuesday: modify the example and inspect Spark UI.
- Wednesday: solve one exercise.
- Thursday: read one debugging or optimization note.
- Friday: answer five interview questions out loud.
- Saturday: build project work.
- Sunday: write a short weekly retrospective.
