# 04 — Delta Lake

Spark's storage layer for production: ACID transactions, time travel, schema evolution, and `MERGE` on top of Parquet. Delta is now the default table format on Databricks and the de-facto standard on cloud lakehouses.

## What you should be able to do after this

- Explain what the Delta transaction log is and how it provides ACID guarantees on top of object storage.
- Write Delta tables and query historical versions.
- Implement upserts, SCD Type 2, and deletes with `MERGE INTO`.
- Tune Delta with `OPTIMIZE`, `Z-ORDER`, `VACUUM`, and partitioning.
- Evolve schemas safely on a live table.
- Recover from the most common Delta failure modes (concurrent writers, vacuum-too-aggressive, file listing on huge tables).

## Notes

1. [Why Delta — the data-lake problem it solves](01-why-delta.md)
2. [The transaction log (`_delta_log`)](02-transaction-log.md)
3. [ACID semantics in practice](03-acid-semantics.md)
4. [Writing Delta tables](04-writing-delta.md)
5. [Time travel and versioning](05-time-travel.md)
6. [MERGE INTO — upserts and CDC](06-merge-into.md)
7. [Schema evolution and enforcement](07-schema-evolution.md)
8. [OPTIMIZE, Z-ORDER, and VACUUM](08-optimize-zorder-vacuum.md)
9. [Delta vs Iceberg vs Hudi](09-format-comparison.md)

## Book references

- *Delta Lake: The Definitive Guide* — O'Reilly, 2024 (Armbrust et al.) — the canonical text.
- [LS Ch.9] — "Building Reliable Data Lakes with Apache Spark" — Delta basics.
- [DAS Ch.10] — Delta in algorithmic pipelines.
- Databricks Delta docs: https://docs.delta.io/

## Setup

This module assumes `delta-spark` is installed (it's in `requirements.txt`). To run any example:

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (SparkSession.builder
    .appName("delta-demo")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

Every example in this module uses this helper.
