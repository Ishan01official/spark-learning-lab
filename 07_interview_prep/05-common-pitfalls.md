# 05 — Common pitfalls the exam tests

A grab-bag of gotchas. These are the "I knew this!" misses people lose points on. Read these the night before.

## Methods that look the same but aren't

### `count(*)` vs `count("col")` vs `countDistinct("col")`

```python
df.agg(F.count("*"))           # row count
df.agg(F.count("amount"))      # non-null row count
df.agg(F.countDistinct("user_id"))  # unique values
df.agg(F.approx_count_distinct("user_id"))  # approximate; faster
```

`count("*")` ≠ `count("any_col")` when there are nulls.

### `coalesce` (DataFrame) vs `coalesce` (Function)

```python
df.coalesce(5)                              # reduce # partitions, no shuffle
F.coalesce(F.col("a"), F.col("b"), F.lit(0))  # first non-null
```

Same name, different functions.

### `repartition(n)` vs `coalesce(n)`

| | repartition | coalesce |
|---|---|---|
| Shuffles? | Yes | No |
| Can increase? | Yes | No |
| Balanced? | Yes | Combines existing, can be skewed |
| Use for | Re-balancing skew | Reducing partitions before write |

### `filter` vs `where`

Identical. They're aliases. Don't second-guess on the exam.

### `select` vs `selectExpr`

```python
df.select(F.col("a") + 1)
df.selectExpr("a + 1")
```

Same result; `selectExpr` takes SQL strings.

### `dropDuplicates` vs `distinct`

```python
df.distinct()                    # all columns
df.dropDuplicates(["user_id"])   # subset
```

`distinct()` = `dropDuplicates()` with no args.

## Quoting traps

### SQL strings inside `filter`

```python
df.filter("status = 'NEW'")          # OK
df.filter("status = 'NEW' AND amount > 100")   # OK
df.filter(F.col("status") == "NEW")  # OK
```

Note: the SQL form uses single quotes inside the double-quoted string. Don't write `df.filter('status = "NEW"')` — quote escaping bites.

### Column names with spaces

```python
df.select("`my col`")          # backticks needed
df.select(F.col("my col"))     # also works
```

## Ordering matters in some places

### `withColumn` overwrites in order

```python
df.withColumn("a", F.col("a") + 1) \
  .withColumn("a", F.col("a") * 2)
# Final a = (a + 1) * 2
```

Read top to bottom.

### `groupBy` then `count` orders

```python
df.groupBy("country").count().orderBy("country")
# vs
df.groupBy("country").count().orderBy(F.desc("count"))
```

Default sort on `count` column: alphabetic by column name, not numeric value. Always specify `F.desc("count")` if you want top counts first.

## Type coercion

### `cast("decimal")` vs `cast("decimal(18,2)")`

```python
F.col("price").cast("decimal")           # decimal(10, 0) — no fractional
F.col("price").cast("decimal(18, 2)")    # 18 digits, 2 after the decimal
```

`decimal` without precision loses fractional parts!

### `int` vs `bigint`

```python
F.col("count").cast("int")           # 32-bit; max ~2.1B
F.col("count").cast("bigint")        # 64-bit; max ~9.2 × 10^18
```

For row counts in big data, always `bigint`.

## Joins

### `join(df, "col")` deduplicates the key

```python
a.join(b, "user_id")              # one user_id column
a.join(b, a.user_id == b.user_id) # TWO user_id columns!
```

After the second, `df.select("user_id")` is ambiguous → error. Use the first form, or `a.join(b, a.user_id == b.user_id).drop(b.user_id)`.

### Default join type is `inner`

If the question doesn't say, assume inner. `null` keys never match in inner join.

### `null` join behavior

```python
# Inner join: rows with null in the key never match — they're excluded.
# Outer join: nulls preserved as join-side nulls.
# Semi/anti join: null is a valid value? Yes, can match.
```

Use `eqNullSafe` (or `<=>` in SQL) if you want nulls to match each other:

```python
a.join(b, a.user_id.eqNullSafe(b.user_id))
```

## Aggregations and groupBy

### You can't `select` non-aggregated columns after `groupBy`

```python
df.groupBy("country").select("city", F.sum("amount"))   # ERROR
df.groupBy("country", "city").agg(F.sum("amount"))      # OK
```

If the column isn't in the groupBy, it must be aggregated.

### `groupBy().pivot()` requires you to specify the pivot values

```python
df.groupBy("region").pivot("year").agg(F.sum("amount"))  # auto-detects years
df.groupBy("region").pivot("year", [2022, 2023]).agg(F.sum("amount"))  # explicit; faster
```

Auto-detect runs an extra job to find distinct values. Always specify if you know them.

## Window function gotchas

### Window without `orderBy` for row_number is undefined

```python
w = Window.partitionBy("user_id")             # no orderBy
df.withColumn("rn", F.row_number().over(w))   # WARNING / undefined
```

You'll get a warning. The result is non-deterministic.

### Frame default differs by function

```python
w = Window.partitionBy("user_id").orderBy("ts")
F.sum("amount").over(w)        # default frame: unboundedPreceding to currentRow
F.row_number().over(w)         # no frame applicable
F.lag("amount").over(w)        # default offset 1
```

A naked `sum().over(w)` with `orderBy` gives a *running* sum, not a per-partition sum.

## Reading files

### `inferSchema=True` is two passes

```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

Spark reads the file twice — once to detect types, once for data. For large files, always provide the schema explicitly.

### `multiLine=True` for JSON with embedded newlines

By default, JSON reader expects one record per line. For pretty-printed JSON:

```python
df = spark.read.option("multiLine", "true").json("file.json")
```

### `wholeTextFiles` vs `text`

```python
spark.read.text("file.txt")           # one row per line
spark.sparkContext.wholeTextFiles(...)  # one row per file (RDD only)
```

## Caching pitfalls

### `cache()` is lazy

```python
df.cache()              # nothing happens yet
df.count()              # NOW cached
```

If you call `cache()` and then don't use the DF again, you've wasted memory.

### Cache eviction is silent

If cache exceeds available memory, Spark silently evicts and re-computes. Look in the Storage UI tab.

### Cached DF schema is fixed

You can't change a cached DF's schema. Add columns *before* caching.

## Logging / debugging

### `df.show()` triggers execution

```python
df = spark.read.parquet("big.parquet").filter("c > 0")
df.show()              # runs the job
```

Each `show()` is an action and re-runs unless you cache. For debugging, use `df.limit(20).cache().show()` to avoid repeated work.

### `df.printSchema()` is metadata only

No action, no job. Fast.

### `df.explain()` is metadata only

No action. Use it freely to inspect plans without paying compute cost.

## Streaming gotchas

### `outputMode("append")` with aggregation requires watermark

```python
stream.groupBy("k").count().writeStream.outputMode("append")  # ERROR
```

Add `withWatermark` first, or use `complete` / `update` mode.

### `withWatermark` order matters

```python
# Wrong:
stream.groupBy(F.window(...)).count().withWatermark("ts", "10 min")  # ERROR

# Right:
stream.withWatermark("ts", "10 min").groupBy(F.window(...)).count()
```

Apply watermark BEFORE aggregation.

### Checkpoint location is mandatory

```python
.writeStream.format("delta").start(path)  # MISSING checkpointLocation
```

Spark will yell. Always set it.

## Spark configs

### `spark.sql.shuffle.partitions` only affects shuffle stages

It doesn't change read parallelism. For reads, use:
- File source: `spark.sql.files.maxPartitionBytes`.
- RDD: `spark.default.parallelism`.

### Setting configs after SparkContext starts

Most configs are immutable after start. To change at runtime:

```python
spark.conf.set("spark.sql.shuffle.partitions", 400)   # SQL configs: yes
spark.conf.set("spark.executor.memory", "8g")          # NO — executor settings fixed at startup
```

Set executor configs before `.getOrCreate()` or at spark-submit.

## Final note

Most "trick" exam questions test one of two things:
1. **The default behavior** — what happens if you don't specify the optional argument.
2. **The subtle API difference** — two methods that look identical but differ in a corner case.

Read every question carefully. The simple answer is usually right. Don't overthink.
