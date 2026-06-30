# 07 — Actions vs transformations (a complete reference)

## Why this matters

Spark's lazy evaluation only works because there's a clear line between operations that *plan* (transformations) and operations that *execute* (actions). Memorize the line and you stop being surprised by when Spark actually does work.

## The rule

> **Transformations** return a DataFrame (or RDD). They're lazy. They extend the plan.
>
> **Actions** return a non-DataFrame value (or write to storage). They trigger execution.

## DataFrame transformations (selected)

All return a new `DataFrame`.

| Category | Operations |
|---|---|
| Projection | `select`, `selectExpr`, `withColumn`, `withColumnRenamed`, `drop`, `cast` |
| Filtering | `filter`, `where`, `dropna`, `fillna`, `replace` |
| Aggregation (lazy until action) | `groupBy(...).agg(...)`, `cube`, `rollup` |
| Sorting | `orderBy`, `sort`, `sortWithinPartitions` |
| Set operations | `union`, `unionByName`, `intersect`, `except`, `distinct`, `dropDuplicates` |
| Joins | `join`, `crossJoin` |
| Windows | `Window.partitionBy(...).orderBy(...)` + `over(window)` |
| Partitioning | `repartition`, `repartitionByRange`, `coalesce` |
| Caching (marks, does not materialize) | `cache`, `persist`, `unpersist` |

## DataFrame actions

These trigger jobs.

| Action | Returns |
|---|---|
| `show(n, truncate)` | None (prints to stdout) |
| `collect()` | `List[Row]` to driver |
| `take(n)` | First n rows to driver |
| `first()` / `head(n)` | First row / n rows to driver |
| `count()` | int |
| `min`/`max`/`sum`/`mean`/`avg` on a DataFrame (without `agg`) | scalar |
| `describe(*cols)` | summary `DataFrame` — *and* triggers compute |
| `summary()` | same as describe + median/quartiles |
| `toPandas()` | `pandas.DataFrame` on driver. **OOM risk.** |
| `toLocalIterator()` | iterator that fetches partition-by-partition (lower driver pressure than collect) |
| `foreach(f)` | runs f on every row, no return |
| `foreachPartition(f)` | runs f on every partition's iterator, no return |
| `write.parquet(...)` / `.csv(...)` / `.json(...)` / `.format(...).save(...)` | None — writes to storage |
| `saveAsTable(name)` | None — writes to the metastore |
| `createOrReplaceTempView(name)` / `createGlobalTempView` | None, but only catalog ops — **no job** |

## Edge cases worth memorizing

- **`cache()` is NOT an action.** It marks the DataFrame for caching. Caching only happens when the *next* action runs. Common bug: people `cache()` then `cache()` something else without ever triggering an action.
- **`printSchema()` is NOT an action.** It uses metadata only.
- **`explain()` is NOT an action.** It runs Catalyst over the plan but doesn't execute it.
- **`createOrReplaceTempView` is NOT an action.** It registers in the catalog without computing.
- **`show()` only computes enough rows to display.** Spark can short-circuit with a LIMIT in the plan.
- **Reading data with `spark.read.parquet(...)` is sort-of-not-an-action.** It runs a tiny job to *read the schema and stats*, but not the data. You'll see a small "schema inference" job for some formats.

## How many jobs does this code produce?

```python
df = spark.read.parquet("orders.parquet")    # 0 or 1 small job (schema)
df2 = df.filter("status='paid'")             # 0
df3 = df2.groupBy("country").count()         # 0
df3.show()                                    # 1 job
df3.count()                                   # 1 more job (re-runs everything)
df3.cache()                                   # 0 (mark only)
df3.count()                                   # 1 job, this time caches
df3.show()                                    # 1 job, but reads from cache
```

Each `show`/`count` is a separate job. Two `count`s in a row that don't share a cache **read the source twice**. This is the single most common source of "why is my notebook so slow" surprise.

## Caching pattern that actually helps

```python
prepared = (
    spark.read.parquet("orders.parquet")
         .filter("status='paid'")
         .select("order_id", "country", "amount")
).cache()                       # MARK

prepared.count()                 # MATERIALIZE the cache

# Now every action below reads from cache, not from S3:
prepared.groupBy("country").count().show()
prepared.agg(F.sum("amount")).show()
prepared.filter("amount > 1000").show()
```

Three actions, but the source is read once. Module 03 has the full caching note.

## When to use which action

- **Always preferred**: `write.<format>(...)` — output goes to storage where downstream can read it.
- **For learning / debugging**: `show(n, truncate=False)`, `printSchema()`, `explain()`.
- **For small final aggregates**: `collect()` is fine on a 100-row result. Catastrophic on 100M rows.
- **For dashboards / iteration**: `toPandas()` for a small-enough result, otherwise `write` + read elsewhere.
- **For side effects on each row** (e.g. writing to a custom sink): `foreachPartition` — never `foreach` for anything that opens a connection.

## References

- [LS Ch.3 §"Operations on DataFrames"]
- [HPS Ch.3 §"DataFrames, Datasets, and Spark SQL"]
- [Spark Python API — DataFrame docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- 📺 [Spark DataFrame API — Databricks Academy](https://www.databricks.com/learn/training/lp/apache-spark-developer-essentials)
