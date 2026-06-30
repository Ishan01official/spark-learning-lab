# 02 — Spark 4.0 preview: what's coming

Spark 4.0 entered preview in 2024. GA is expected in 2025/26. This note covers what changes and what to expect to need to know.

The headline changes: ANSI mode default-on, the new `VARIANT` type, Spark Connect maturing further, deprecation of some Hadoop-era APIs.

## ANSI mode on by default

This is the biggest change for code that worked before.

### What ANSI mode does

When `spark.sql.ansi.enabled = true`:
- Integer overflow throws an error instead of wrapping silently.
- Cast failures throw instead of returning null. (Use `try_cast` for the old behavior.)
- Division by zero throws (use `try_divide`).
- Array out-of-bounds access throws.
- Date/time arithmetic with invalid values throws.

These are SQL-standard behaviors. Spark historically chose to be permissive; 4.0 chooses to be correct.

### What changes for your code

```python
# 3.x default — silently produces null
df.select(F.col("s").cast("int"))         # "hello" -> null

# 4.0 ANSI default — throws
df.select(F.col("s").cast("int"))         # "hello" -> SparkException

# 4.0 ANSI with safety
df.select(F.try_cast(F.col("s"), "int"))  # "hello" -> null
```

For new code, this is good. For old code, you may need to audit casts.

### Opt out (not recommended)

```python
spark.conf.set("spark.sql.ansi.enabled", "false")
```

Use only for migration. New code should embrace ANSI.

## VARIANT type

A new column type for storing semi-structured data. Think of it as "Spark-native JSONB":

```python
df = df.withColumn("payload", F.parse_json(F.col("raw_json")))
df.select("payload:user_id", "payload:event_type")    # path access
df.where("payload:amount > 100")
```

Why it matters:
- Pre-4.0: you stored JSON as STRING and parsed per query (slow), or you defined a strict schema (rigid).
- 4.0 VARIANT: stored efficiently, queryable with paths, no strict schema needed.

Useful for:
- Bronze layer storage of payloads with evolving schemas.
- Configuration tables.
- Audit logs with varying detail levels.

[*Delta Definitive Guide* Ch.10 — Delta added VARIANT support in 4.0.x]

## Spark Connect: production-ready

Spark Connect went GA in 3.5 but matures further in 4.0:
- More PySpark methods supported (closer to 100% coverage).
- Better error reporting.
- Connection pooling, retry, auth improvements.

See `03-spark-connect.md`.

## Scala 2.13 only

Scala 2.12 support is dropped. For Python users this is mostly invisible — your code is unaffected. But:
- Custom JARs need to be Scala 2.13.
- Older Databricks runtimes (DBR 13.x on Scala 2.12) won't be upgradeable to 4.0 without recompilation.

## Removed APIs

Many long-deprecated APIs go away. Notable ones:
- `RDD.toDebugString` formatting changed slightly.
- Some old RDD-era checkpoint APIs.
- A handful of `mllib.*` (vector RDD) classes — use the DataFrame ML pipeline API.

If you're using public DataFrame / SQL APIs in PySpark, your code is unaffected. If you have custom JARs or RDD-heavy code, audit before upgrading.

## Streaming improvements

- `transformWithState` API stabilizes (replaces `mapGroupsWithState` for new code).
- Better metrics: per-operator state size, watermark progress, dropped-due-to-watermark counts in the Streaming Query UI.
- Improved RocksDB state store.

## Catalyst and AQE

- Cost-based optimization (CBO) is on by default in more scenarios.
- Better statistics use, including from Delta and Iceberg metadata.
- AQE skew handling more aggressive by default.

## Python 3.13 support

Spark 4.0 supports Python 3.10 through 3.13. Older Python (3.8, 3.9) is no longer supported.

If your environment is on 3.9 or earlier, plan a Python upgrade before Spark 4.0.

## SQL syntax additions

A handful of new SQL features:
- `MERGE INTO ... USING ... ON ...` — fully ANSI-compliant syntax in addition to Delta's variant.
- `LATERAL` join syntax.
- More TIMESTAMP functions.

## What you can do now

Even before 4.0 is GA:
- Set `spark.sql.ansi.enabled = true` in your dev clusters. Find and fix issues.
- Replace `cast(...)` with `try_cast(...)` where you depend on null-on-failure.
- Audit any Scala 2.12 dependencies.
- Plan to use VARIANT instead of STRING-storage-of-JSON for new bronze tables.

## Where to follow progress

- Spark 4.0 preview2 docs: https://spark.apache.org/docs/4.0.0-preview2/
- Spark 4.0 release notes (when published): https://spark.apache.org/releases/
- JIRA: https://issues.apache.org/jira/projects/SPARK
- 📺 [Apache Spark 4.0 preview](https://www.youtube.com/results?search_query=apache+spark+4.0+preview)
