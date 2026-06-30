# 01 — Spark 3.5 features worth knowing

Spark 3.5 (Sept 2023) consolidated several improvements that started in 3.0–3.4. Spark 3.5.x (1, 2, 3, ...) are bugfix releases. Most production clusters in 2024–2025 ran 3.5.

## SQL improvements

### New built-in functions

Several long-requested functions made it in:

```python
# Array manipulation
F.array_compact(F.col("arr"))                    # drop nulls
F.array_append(F.col("arr"), F.lit("x"))
F.array_prepend(F.col("arr"), F.lit("x"))
F.array_insert(F.col("arr"), 1, F.lit("x"))      # at position 1

# Map manipulation
F.map_contains_key(F.col("m"), F.lit("k"))
F.map_filter(F.col("m"), lambda k, v: v > 0)

# String
F.contains(F.col("s"), F.lit("foo"))             # was startswith/endswith only
F.endswith(F.col("s"), F.lit("foo"))
F.btrim(F.col("s"))                              # trim both sides, customizable
F.split_part(F.col("s"), ",", 2)
F.regexp_count(F.col("s"), r"\d+")

# Numeric
F.bit_count(F.col("x"))
F.bit_and(F.col("a"), F.col("b"))                # bitwise — already existed; better support
```

[LS Ch.4 §"Built-in Functions" — covers the pre-3.5 set]

### `try_*` variants for safe casts

ANSI mode often errors on overflow, divide-by-zero, etc. The `try_*` family returns null instead:

```python
F.try_cast(F.col("s"), "int")                    # null if not parseable
F.try_divide(F.col("a"), F.col("b"))             # null if b = 0
F.try_add(F.col("a"), F.col("b"))                # null on overflow
F.try_to_timestamp(F.col("s"))
```

## Structured Streaming

### RocksDB state store improvements

The RocksDB state store provider became more production-ready in 3.5:
- Faster checkpointing.
- Smaller state snapshots.
- Better metrics in the streaming progress UI.

```python
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
```

[LS Ch.8 didn't cover this — added in HPS 2e Ch.10]

### `transformWithState` (preview)

A simpler API than `mapGroupsWithState` / `flatMapGroupsWithState` for arbitrary stateful operations. Cleaner state lifecycle, better Python ergonomics. Preview in 3.5, stabilizing in 4.0.

### Trigger improvements

- `Trigger.AvailableNow` (replaces `Trigger.Once`): drains all currently available data into one or more batches and stops. The right pattern for "streaming queries on cron".

```python
.trigger(availableNow=True)
```

## PySpark improvements

### Better pandas integration

PySpark's pandas-on-Spark API (formerly Koalas) is more complete and faster in 3.5. For users who think in pandas idioms:

```python
import pyspark.pandas as ps
psdf = ps.read_parquet("path")
psdf.groupby("country")["amount"].mean()    # pandas-style; runs on Spark
psdf.to_spark()                              # back to Spark DataFrame
```

Useful when porting pandas notebooks; the API is not 100% pandas (some methods missing or behave differently).

### Better type hints in stubs

Spark's Python type stubs got much better in 3.5. IDE auto-complete on `F.col(...)` and DataFrame methods is now reliable.

## Spark Connect

Spark Connect went GA in 3.5 (preview in 3.4). See `03-spark-connect.md` for details.

## Adaptive Query Execution evolution

AQE was on by default since 3.2. In 3.5, it grew:
- **Dynamic join filter pushdown**: at runtime, push the keys from the small side of a join down to the large side as a filter.
- **Better skew handling**: smaller threshold defaults, more aggressive splitting.

You don't usually need to know the details — just `spark.sql.adaptive.enabled = true` and let it work.

## Performance

- Several Catalyst rules now run faster on very long plans (helpful when you chain `withColumn` 100 times — though you still shouldn't).
- Improved Parquet vectorized reader for complex/nested schemas.
- Better Arrow integration (used internally for many cross-language paths).

## Deprecations

In 3.5:
- `Trigger.Once` — use `Trigger.AvailableNow` instead.
- Some older Hadoop integrations (Hadoop 2.x).

Coming in 4.0:
- Scala 2.12 support drops (Scala 2.13 only).
- Some long-deprecated APIs (mostly RDD-era) being removed.

## Migration notes from 3.x earlier

If you're upgrading from 3.0/3.1:
- AQE is on by default. Behavior may differ from before (often faster).
- ANSI mode (`spark.sql.ansi.enabled`) is OFF by default but recommended for new pipelines.
- Some functions are stricter about null inputs.
- The Spark UI looks slightly different — same content.

## Where to find more

- Spark 3.5.0 release notes: https://spark.apache.org/releases/spark-release-3-5-0.html
- API docs (PySpark 3.5): https://spark.apache.org/docs/3.5.0/api/python/index.html
- 📺 [What's New in Apache Spark 3.5 — Databricks](https://www.youtube.com/results?search_query=what%27s+new+apache+spark+3.5+databricks)
