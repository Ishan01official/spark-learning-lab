# Resource Map

Curated resources for learning Spark, PySpark, Delta Lake, Databricks, and lakehouse architecture.

Last checked: 2026-07-01.

Prefer official documentation first. Books are mapped by learning value only; this repo does not copy copyrighted book content.

## Official Spark Resources

| Title | Link | Type | Level | Best for | Related module | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Apache Spark Documentation | https://spark.apache.org/docs/latest/ | official doc | beginner to advanced | canonical Spark entry point | `00_setup/`, `01_fundamentals/` | Spark docs describe Spark as a unified engine and link to SQL, PySpark, streaming, deployment, monitoring, and tuning guides. |
| Spark SQL, DataFrames, and Datasets Guide | https://spark.apache.org/docs/latest/sql-programming-guide.html | official doc | beginner to advanced | DataFrame and SQL model | `02_pyspark_core/`, `17_sql_for_spark/` | Use for DataFrame concepts, data sources, SQL reference, and tuning pages. |
| PySpark API Reference | https://spark.apache.org/docs/latest/api/python/ | official doc | intermediate | exact API behavior | `02_pyspark_core/` | Best when checking function signatures and edge cases. |
| Structured Streaming Guide | https://spark.apache.org/docs/latest/streaming/index.html | official doc | intermediate | streaming model and APIs | `05_streaming/` | Spark 4 docs split streaming into smaller pages. |
| Spark Monitoring and Web UI | https://spark.apache.org/docs/latest/monitoring.html | official doc | intermediate | Spark UI and metrics | `14_spark_ui_lab/` | Use while debugging slow or failed jobs. |
| Spark Tuning Guide | https://spark.apache.org/docs/latest/tuning.html | official doc | advanced | memory, serialization, tuning | `03_optimization/` | Read after you can interpret Spark UI metrics. |
| Spark on Kubernetes | https://spark.apache.org/docs/latest/running-on-kubernetes.html | official doc | advanced | Kubernetes deployment | `16_cloud_lakehouse/` | Good for platform engineers and architects. |

## Delta Lake Resources

| Title | Link | Type | Level | Best for | Related module | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Delta Lake Documentation | https://docs.delta.io/ | official doc | beginner to advanced | Delta core concepts | `04_delta_lake/` | Official docs cover ACID, time travel, merge, batch/stream unification, and connectors. |
| Delta Lake Quick Start | https://docs.delta.io/latest/quick-start.html | official doc | beginner | local Delta setup | `04_delta_lake/examples/` | Use to validate local Delta dependencies. |
| Delta Change Data Feed | https://docs.delta.io/latest/delta-change-data-feed.html | official doc | advanced | CDC patterns | `04_delta_lake/`, `06_real_projects/` | Useful for incremental pipelines. |
| Delta Optimizations | https://docs.delta.io/latest/optimizations-oss.html | official doc | advanced | file skipping, compaction, tuning | `04_delta_lake/`, `03_optimization/` | Pair with small-files case studies. |

## Databricks Resources

| Title | Link | Type | Level | Best for | Related module | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Databricks Documentation | https://docs.databricks.com/ | official doc | beginner to architect | managed Spark platform | `15_databricks_production/` | Main entry for workspace, jobs, clusters, security, and runtime docs. |
| Databricks Workflows | https://docs.databricks.com/aws/en/jobs/ | official doc | intermediate | orchestration | `15_databricks_production/` | Learn job clusters, retries, schedules, and task dependencies. |
| Unity Catalog | https://docs.databricks.com/aws/en/data-governance/unity-catalog/ | official doc | advanced | governance and permissions | `15_databricks_production/`, `16_cloud_lakehouse/` | Required for modern Databricks platform design. |
| Auto Loader | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/ | official doc | intermediate | scalable file ingestion | `15_databricks_production/`, `05_streaming/` | Important for cloud landing zones. |
| Lakeflow Declarative Pipelines | https://docs.databricks.com/aws/en/ldp/ | official doc | advanced | managed declarative pipelines | `15_databricks_production/` | Successor naming around DLT-style managed pipelines. |
| Databricks Certifications | https://www.databricks.com/learn/certification | official page | beginner to senior | certification paths | `12_certification_prep/` | Use for exam objectives and registration. |

## Books

| Title | Link | Type | Level | Best for | Related module | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Learning Spark, 2nd Edition | https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/ | book | beginner to intermediate | structured Spark foundation | `00_setup/` to `05_streaming/` | Use with `BOOK_MAP.md`; do not copy text into notes. |
| High Performance Spark | https://www.oreilly.com/library/view/high-performance-spark/9781491943199/ | book | advanced | internals and performance | `03_optimization/` | Best after you have run real slow jobs. |
| Data Algorithms with Spark | https://www.oreilly.com/library/view/data-algorithms-with-spark/9781492082378/ | book | intermediate to advanced | algorithmic data engineering patterns | `06_real_projects/` | Good for joins, dimensions, and scalable algorithms. |
| Delta Lake: The Definitive Guide | https://www.oreilly.com/library/view/delta-lake-the/9781098151942/ | book | intermediate | Delta table design | `04_delta_lake/` | Use as a companion for Delta production patterns. |
| Fundamentals of Data Engineering | https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/ | book | architect | platform thinking | `10_architecture/`, `16_cloud_lakehouse/` | Strong for architecture and lifecycle framing. |

## Talks, Videos, And Repos

| Title | Link | Type | Level | Best for | Related module | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Databricks Academy | https://www.databricks.com/learn/training/home | video/course | beginner to advanced | guided Databricks learning | `15_databricks_production/` | Use official training before random playlists. |
| Spark Summit / Data + AI Summit YouTube | https://www.youtube.com/@Databricks | video | intermediate to architect | conference talks | all modules | Search by topic: AQE, Delta, streaming, Photon, Unity Catalog. |
| Apache Spark GitHub | https://github.com/apache/spark | repo | advanced | source code and examples | `09_latest_updates/` | Use for examples, issues, and release changes. |
| Delta Lake GitHub | https://github.com/delta-io/delta | repo | advanced | Delta internals and issues | `04_delta_lake/` | Useful when debugging version compatibility. |

## How To Add A Resource

Use this format:

```text
| Title | Link | Type | Level | Best for | Related module | Notes |
```

Rules:

- Prefer official docs.
- Add "Last checked" date when the file is updated.
- Explain why the resource is useful.
- Do not copy paid book text.
