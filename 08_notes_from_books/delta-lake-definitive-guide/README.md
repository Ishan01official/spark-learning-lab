# Delta Lake: The Definitive Guide — chapter notes

Armbrust, Das, Powers, Tomar. O'Reilly, 2024. ~400 pages.

The canonical reference for Delta Lake, written by the people who built it. Covers OSS Delta and Databricks runtime features.

---

## Ch.1 — The Lakehouse Architecture

**Key concepts**
- Why Lakehouse: combining data warehouse correctness with data lake economics.
- Failure modes of pre-Lakehouse architectures (data lake + data warehouse).
- The four pillars: open formats, ACID transactions, schema enforcement, time travel.

**Takeaway**: A "data lake" without transactions is a swamp. Delta turns object storage into a transactional warehouse.

**This lab**: module 04 `01-why-delta.md`, module 06 `01-medallion-architecture.md`.

---

## Ch.2 — Getting Started

**Key concepts**
- Local Delta setup (Spark + delta-spark package).
- The minimum config for Delta to work.
- First create/read/write.
- The Delta API in Python: `DeltaTable.forPath`, `DeltaTable.forName`.

**Most useful**:
```python
from delta import configure_spark_with_delta_pip
builder = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

**This lab**: module 04 `01-why-delta.md` (setup section), examples.

---

## Ch.3 — The Transaction Log

**Key concepts**
- `_delta_log/` directory structure.
- JSON commit files; their actions: `add`, `remove`, `commitInfo`, `protocol`, `metaData`.
- Checkpoints — periodic Parquet snapshots of the log.
- The `_last_checkpoint` pointer.
- Reading a Delta table: list log, apply checkpoint + later commits → set of "live" files.

**Takeaway**: Delta is "Parquet + a transaction log on the side". The log is the source of truth for what files are part of the table.

**This lab**: module 04 `02-transaction-log.md`.

---

## Ch.4 — Diving into the Transaction Log

(Deeper coverage of Ch.3 — protocol versioning, log compaction, multi-cluster writes.)

**Key concepts**
- Reader / writer protocol versions; "table features".
- How OPTIMIZE compacts the log itself.
- Coordination across writers using filesystem atomic-rename semantics.

**Takeaway**: Protocol bumps are forward-incompatible. Plan upgrades carefully.

---

## Ch.5 — Schema Handling

**Key concepts**
- Schema enforcement on every write.
- `mergeSchema=true` and `overwriteSchema=true`.
- ALTER TABLE for column add/drop/rename.
- Column mapping mode (separates logical name from file column name).
- Type widening.

**Takeaway**: Add columns liberally with `mergeSchema=true`. Renames/drops are heavier — need column mapping.

**This lab**: module 04 `07-schema-evolution.md`.

---

## Ch.6 — Basic Operations

**Key concepts**
- CREATE TABLE syntaxes.
- INSERT, UPDATE, DELETE.
- MERGE INTO (covered in Ch.8 in detail).
- READ with versionAsOf / timestampAsOf.

**Takeaway**: SQL DML on object storage. The whole point of Delta.

**This lab**: module 04 `04-writing-delta.md`, `05-time-travel.md`.

---

## Ch.7 — Streaming with Delta

**Key concepts**
- Delta as a streaming source: append-only by default; `ignoreChanges` and `ignoreDeletes` for tables that get updated/deleted.
- Change Data Feed (CDF): emits row-level changes with `_change_type`.
- Delta as a streaming sink: exactly-once natively.
- foreachBatch + MERGE pattern.

**Most useful**:
```python
# CDF read
spark.readStream.format("delta").option("readChangeFeed", "true").option("startingVersion", 100).load(path)

# Streaming MERGE in foreachBatch
def merge_fn(batch, batch_id):
    target.merge(batch, "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
stream.writeStream.foreachBatch(merge_fn).option("checkpointLocation", ...).start()
```

**Takeaway**: CDF + foreachBatch is the modern CDC pattern for the lakehouse. Replace traditional Kafka-Connect-Debezium pipelines.

**This lab**: module 05 `02-sources-and-sinks.md`, `08-foreachbatch.md`, project `clickstream-stream/`.

---

## Ch.8 — Merging with Delta

**Key concepts**
- MERGE syntax: matched / not-matched / not-matched-by-source clauses.
- Multiple WHEN clauses per category.
- Common patterns: upsert, SCD2, CDC merge, dedup.
- MERGE performance: file-level rewriting, hint use, partition predicates.

**Most useful**:
```python
target.merge(source, "t.id = s.id") \
    .whenMatchedDelete(condition="s.op = 'D'") \
    .whenMatchedUpdate(condition="s.op = 'U'", set={"col": "s.col"}) \
    .whenNotMatchedInsert(condition="s.op = 'I'", values={"id": "s.id", "col": "s.col"}) \
    .execute()
```

**Takeaway**: MERGE is the most powerful Delta operation. Master it.

**This lab**: module 04 `06-merge-into.md`, examples 03 and 04.

---

## Ch.9 — Performance and Optimization

**Key concepts**
- OPTIMIZE: compact small files.
- Z-ORDER: multi-dim data skipping.
- Liquid clustering (newer alternative to partitioning + Z-ORDER).
- VACUUM: reclaim storage.
- The retention configs.
- Auto-optimize (Databricks): optimize at write time.

**Most useful**:
```sql
OPTIMIZE table_name ZORDER BY (user_id, event_type);
VACUUM table_name RETAIN 168 HOURS;
ALTER TABLE table_name SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
```

**Takeaway**: OPTIMIZE weekly + Z-ORDER on hot query keys + VACUUM weekly = the production maintenance schedule.

**This lab**: module 04 `08-optimize-zorder-vacuum.md`, example 05.

---

## Ch.10 — Schema Evolution and Column Mapping

(Deeper coverage of Ch.5.)

**Key concepts**
- Column mapping protocol details.
- Type widening table feature.
- Backwards-incompatible changes; protocol bumps.

**Takeaway**: Schema evolution is hard. Plan changes in advance, test on a copy.

---

## Ch.11 — Building a Lakehouse

**Key concepts**
- Medallion architecture (bronze / silver / gold).
- Layer-by-layer design patterns.
- Data quality at each layer.
- Operational concerns: maintenance, monitoring, lineage.

**Takeaway**: The architecture isn't novel — the *implementation* is. Delta makes the lakehouse practical.

**This lab**: module 06 entirely.

---

## Ch.12 — Comparing Table Formats

**Key concepts**
- Delta vs Iceberg vs Hudi: history, design choices.
- When each is the right fit.
- Delta Universal Format (Uniform): cross-format compatibility.
- The convergence: open table formats are becoming interoperable.

**Takeaway**: The "format war" is winding down. Pick based on ecosystem fit (Databricks → Delta, Snowflake → Iceberg) and you can always switch later via Uniform.

**This lab**: module 04 `09-format-comparison.md`.

---

## Ch.13 — Securing the Lakehouse

(Covers Unity Catalog, ACLs, governance. Useful but Databricks-specific.)

---

## Overall takeaway

If you're building anything on object storage and want transactions, *Delta Lake: The Definitive Guide* is the reference. Chapters 3 (transaction log), 7 (streaming), 8 (MERGE), 9 (optimization), and 11 (lakehouse architecture) are essential. The rest is supporting material.
