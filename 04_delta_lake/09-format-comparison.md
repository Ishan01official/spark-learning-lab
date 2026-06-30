# 09 — Delta vs Iceberg vs Hudi

## Why this matters

Delta isn't the only lakehouse table format. Apache Iceberg (originated at Netflix, now CNCF) and Apache Hudi (originated at Uber) solve the same Parquet-doesn't-have-transactions problem, with different design choices. When you join a new team or evaluate a platform, you need to know which one is in front of you and what it implies.

## TL;DR

| | Delta | Iceberg | Hudi |
|---|---|---|---|
| Origin | Databricks (2019) | Netflix (2017) | Uber (2017) |
| Governance | Linux Foundation | Apache | Apache |
| Storage | Parquet + JSON/Parquet log | Parquet + Avro/JSON manifest | Parquet + Avro timeline |
| ACID | Optimistic concurrency | Optimistic concurrency | MVCC + optional locking |
| Time travel | ✅ version / timestamp | ✅ snapshot ID / timestamp | ✅ commit time |
| Schema evolution | ✅ | ✅ (most thorough) | ✅ |
| Partition evolution | ❌ (rewrite needed) | ✅ in-place | ⚠️ limited |
| MERGE / Upsert | ✅ (`MERGE INTO`) | ✅ (`MERGE INTO`) | ✅ (built around it) |
| CDC reader | ✅ Change Data Feed | ✅ Incremental reads | ✅ Incremental queries |
| Streaming write | ✅ | ✅ | ✅ (native focus) |
| Best ecosystem | Spark (esp. Databricks) | Spark, Trino, Flink, Snowflake | Spark, Flink |
| Default in… | Databricks, Microsoft Fabric | Snowflake, Dremio, Tabular | Onehouse, Uber |

## What's the *real* difference

### Metadata layout

- **Delta**: one JSON file per commit + periodic Parquet checkpoints. Easy to inspect by hand; readable on object storage.
- **Iceberg**: hierarchical metadata — snapshot → manifest list → manifest files → data files. More indirection, but the manifest layer scales better to huge tables (millions of files).
- **Hudi**: a timeline of action files (commit, deltacommit, compaction). Two table types — Copy-on-Write (CoW) and Merge-on-Read (MoR) — each with different write/read trade-offs.

For tables with > 100M files, Iceberg's manifest layer typically scales better than Delta's single log directory.

### Update model

- **Delta**: CoW. An update rewrites the entire affected file. Simple, slower for write-heavy.
- **Iceberg**: CoW by default; *merge-on-read* (MoR) optional — writes a delete file alongside, applies at read.
- **Hudi**: CoW or MoR explicitly chosen at table creation. MoR is what Hudi was designed for: tiny incremental writes + background compaction.

If your workload is heavy updates on a streaming table, Hudi MoR is the most write-optimized of the three.

### Partition evolution

This is Iceberg's signature feature. With Iceberg, you can change a table's partitioning *without rewriting any data*. Old files keep their old partitioning; new files use the new scheme; queries handle both transparently.

Delta requires a full rewrite to change partition columns. Liquid clustering (Delta 3.2+) is a different approach that aims to make partition choice less important up-front.

### Engine support

| Engine | Delta | Iceberg | Hudi |
|---|---|---|---|
| Spark | ⭐ first-class | ⭐ first-class | ⭐ first-class |
| Databricks | ⭐ native | ✅ Uniform | via Spark |
| Snowflake | via Uniform | ⭐ native (external tables) | via Spark |
| Trino / Presto | ✅ | ⭐ first-class | ✅ |
| Flink | ⚠️ via connectors | ✅ | ⭐ Hudi's stream-first design fits Flink |
| BigQuery | ✅ Biglake | ✅ Biglake | ✅ Biglake |
| DuckDB | ✅ extension | ✅ extension | ⚠️ limited |

Iceberg has the broadest engine support; Delta has the deepest Spark/Databricks integration; Hudi has the strongest streaming/MoR story.

## Delta Universal Format (Uniform)

Delta 3.0+ can write metadata in *both* Delta and Iceberg formats simultaneously (via `delta.universalFormat.enabledFormats = 'iceberg'`). The same files are readable as a Delta table by Spark and as an Iceberg table by Snowflake/Trino. This blurs the historical platform-lock distinction.

## When to pick which (in 2024+)

- **All-Databricks / Spark / Fabric shop**: Delta. Best tooling, native, mature.
- **Multi-engine (Spark + Trino + Snowflake)**: Iceberg, or Delta with Uniform. Iceberg's neutrality is a real asset here.
- **High-frequency streaming upserts**: Hudi MoR, or Delta with frequent OPTIMIZE.
- **Existing investment in any of them**: stay. The migration cost rarely pays back unless you're hitting a specific limitation.

## What stays the same

Whatever you pick, you still need:
- Periodic compaction (OPTIMIZE in Delta, REWRITE_DATA_FILES in Iceberg, compaction action in Hudi).
- A retention/vacuum policy.
- Schema versioning discipline.
- Monitoring of commit rate and metadata growth.

The differences are real but smaller than the marketing suggests. The hard part is *operating* any of them — and that's the same across formats.

## References

- *Delta Lake: The Definitive Guide* — Ch.12 "Comparing Table Formats"
- Apache Iceberg docs: https://iceberg.apache.org/docs/latest/
- Apache Hudi docs: https://hudi.apache.org/docs/overview/
- 📺 [Lakehouse Table Formats: Delta vs Iceberg vs Hudi — Data Engineering Weekly](https://www.youtube.com/results?search_query=delta+iceberg+hudi+comparison)
- 📺 [Onehouse benchmarks (vendor-biased but useful)](https://www.youtube.com/results?search_query=onehouse+lakehouse+benchmarks)
- "Lakehouse: A New Generation of Open Platforms" — Armbrust et al., CIDR 2021
