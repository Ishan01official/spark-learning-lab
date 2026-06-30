# Mock exam 2

60 questions. ~120 minutes. Answers at the end.

This exam leans harder on code-reading and plan interpretation than mock 1.

---

### 1. Given:
```python
df = spark.range(10).withColumn("v", F.col("id") * 2)
df.filter("v > 8").count()
```
Result?
- A) 4
- B) 5
- C) 6
- D) 10

### 2. Given:
```python
df = spark.createDataFrame([(1, 10), (1, 20), (2, 5), (2, 15)], ["k", "v"])
df.groupBy("k").agg(F.max("v") - F.min("v")).show()
```
Output for k=1:
- A) 10
- B) 30
- C) 20
- D) 0

### 3. Given:
```python
df = spark.createDataFrame([(1, None), (2, "x"), (3, None)], ["a", "b"])
df.filter(F.col("b").isNotNull()).count()
```
- A) 0
- B) 1
- C) 2
- D) 3

### 4. Which produces an error?
- A) `df.filter("a > 5")`
- B) `df.filter(F.col("a") > 5)`
- C) `df.filter(df.a > 5)`
- D) `df.filter(a > 5)`

### 5. Given two DFs both partitioned on `id` already, a join on `id` results in:
- A) BroadcastHashJoin
- B) SortMergeJoin (still shuffle)
- C) Skipped shuffle (no exchange needed)
- D) Cross join

### 6. After `df.cache()` and `df.unpersist()`:
- A) DataFrame is gone
- B) Cache freed; DataFrame still usable, recomputes on next action
- C) errors next time
- D) just removes from Storage UI

### 7. Given:
```python
df.write.partitionBy("year", "month").parquet("out/")
```
On reading: `spark.read.parquet("out/year=2024/month=09/")` will:
- A) infer year and month as partition columns
- B) lose year and month columns
- C) error
- D) read but include year/month as data

### 8. `F.broadcast(df)` if df is 1 GB:
- A) broadcasts anyway, possibly OOM driver
- B) Spark ignores the hint
- C) error
- D) broadcasts but warns

### 9. Given the plan:
```
== Physical Plan ==
*(2) HashAggregate(keys=[country], functions=[sum(amount)])
+- Exchange hashpartitioning(country, 200)
   +- *(1) HashAggregate(keys=[country], functions=[partial_sum(amount)])
      +- *(1) FileScan parquet [country, amount]
```
The two HashAggregate stages indicate:
- A) bug
- B) map-side combine + reduce-side aggregate (normal optimization)
- C) re-computation
- D) cache miss

### 10. Given:
```python
df.repartition(100).coalesce(50).rdd.getNumPartitions()
```
- A) 100
- B) 50
- C) 200
- D) error

### 11. Given:
```python
df.coalesce(50).repartition(100).rdd.getNumPartitions()
```
- A) 100
- B) 50
- C) error
- D) 200

### 12. The `_SUCCESS` file in a write output indicates:
- A) write completed without errors
- B) the file is corrupt
- C) deprecated
- D) only for streaming

### 13. Given a streaming DataFrame, which is NOT supported?
- A) `groupBy("k").count()` with `outputMode("complete")`
- B) `withWatermark("ts", "10m").groupBy(window(...)).count()` with `outputMode("append")`
- C) `df.collect()`
- D) `df.writeStream.format("delta")...`

### 14. Default behavior when join column is `null` in inner join:
- A) match other nulls
- B) excluded — null != null
- C) match anything
- D) errors

### 15. The shuffle service:
- A) replaces executor-local shuffle storage
- B) speeds up reads only
- C) is mandatory
- D) only for streaming

### 16. Given:
```python
df = spark.read.parquet("a/")
df.select("c1").count()
df.select("c2").count()
```
The Parquet file is read:
- A) once (caching)
- B) twice (no cache)
- C) only the metadata
- D) projection pruning means only c1 and c2 columns ever read

### 17. Pivot without explicit values:
- A) auto-detects but runs an extra job
- B) fastest method
- C) errors
- D) only first 100 values

### 18. To rename a column conditionally:
- A) `df.withColumnRenamed("old", "new")` (always)
- B) `if "old" in df.columns: df = df.withColumnRenamed("old", "new")`
- C) `df.rename` (pandas style)
- D) cannot be done

### 19. Given:
```python
df.cache()
df.count()
df.unpersist()
df.count()
```
Total scans of underlying data:
- A) 1
- B) 2
- C) 3
- D) 4

### 20. The "DAG" stands for:
- A) Distributed Action Graph
- B) Directed Acyclic Graph
- C) Data Action Group
- D) Data Aggregation Graph

### 21. A stage boundary occurs at:
- A) every action
- B) every wide transformation
- C) every map operation
- D) every read

### 22. Given:
```python
df.write.format("delta").mode("overwrite").option("replaceWhere", "year = 2024").save(path)
```
This:
- A) overwrites the whole table
- B) overwrites only rows where year = 2024
- C) errors if year column doesn't exist
- D) appends with a filter

### 23. The relationship between executor cores and task parallelism:
- A) one task per core at a time
- B) two tasks per core
- C) unlimited tasks per executor
- D) one task per executor

### 24. A typical reason to use `mapPartitions`:
- A) when initialization is expensive (DB connection)
- B) when records are tiny
- C) always faster than map
- D) for streaming only

### 25. Given:
```python
df.groupBy("a").agg(F.first("b"))
```
The `first` function:
- A) returns the first row's b deterministically
- B) returns any b (non-deterministic without sort)
- C) errors
- D) returns null

### 26. Setting `spark.sql.adaptive.skewJoin.enabled = true`:
- A) automatically detects skew and replicates the skewed partition
- B) requires hash hint
- C) only works for SortMergeJoin
- D) only for streaming

### 27. The `dropDuplicates` on a streaming DF needs a watermark because:
- A) otherwise state grows unboundedly
- B) it's required by spec
- C) it doesn't — only `groupBy` needs it
- D) for performance only

### 28. Given:
```python
df.filter("status IN ('NEW', 'PAID', 'SHIPPED')")
```
Equivalent in API:
- A) `df.filter(F.col("status").isin("NEW", "PAID", "SHIPPED"))`
- B) `df.filter(F.col("status").isin(["NEW", "PAID", "SHIPPED"]))`
- C) both
- D) neither

### 29. After `df.repartition(F.col("country"))`:
- A) shuffle to `spark.sql.shuffle.partitions` # of parts, partitioned by country
- B) shuffle to # of distinct countries
- C) no shuffle
- D) error

### 30. A "narrow transformation" property:
- A) only operates on a single column
- B) each input partition maps to one output partition
- C) returns the same DataFrame
- D) no transformations are truly narrow

### 31. Given:
```python
@F.udf("int")
def add_one(x):
    return x + 1
df.withColumn("y", add_one("x"))
```
This:
- A) works
- B) errors — must use F.col("x")
- C) returns string
- D) errors — UDF can't be int

### 32. Given:
```python
df.select(F.expr("a + 1 as a1"))
```
Equivalent to:
- A) `df.selectExpr("a + 1 as a1")`
- B) `df.withColumn("a1", F.col("a") + 1).select("a1")`
- C) both
- D) neither

### 33. `df.collect()` on a 10M-row DataFrame is:
- A) fine; Spark handles it
- B) bad; pulls all data to driver
- C) only first 1000 rows
- D) deprecated

### 34. `spark.range(10).count()` returns:
- A) 0
- B) 9
- C) 10
- D) 11

### 35. `df.coalesce("col")`:
- A) coalesces partitions by column
- B) errors — coalesce takes int
- C) returns first non-null
- D) sorts by column

### 36. Given a 1 TB Parquet table partitioned by `event_date`, the query:
```python
df.filter("event_date = '2024-09-15'").count()
```
- A) scans 1 TB
- B) scans only the 2024-09-15 partition
- C) scans random sample
- D) errors

### 37. `withColumn` chained 100 times:
- A) fast
- B) slow — each call rebuilds the plan; use `select` with all expressions
- C) error
- D) compiles to one Catalyst pass anyway

### 38. The function for "is column equal to X, OR is column null":
- A) `(F.col("c") == X) | F.col("c").isNull()`
- B) `F.col("c").eqNullSafe(X)`
- C) `F.coalesce(F.col("c"), X) == X`
- D) only A

### 39. `df.sample(0.1)` returns:
- A) exactly 10% of rows
- B) approximately 10% of rows
- C) first 10% of rows
- D) random 10 rows

### 40. To make the sample reproducible:
- A) `df.sample(0.1, seed=42)`
- B) `spark.conf.set("spark.sql.sampleSeed", 42)`
- C) impossible
- D) `df.sample(0.1).cache()`

### 41. Window's `rowsBetween(-2, 0)`:
- A) current row + 2 preceding
- B) 2 preceding + 2 following
- C) just current row
- D) errors

### 42. `F.array_distinct("arr_col")`:
- A) returns unique elements in the array
- B) errors on null
- C) flattens nested arrays
- D) sorts and dedupes

### 43. `F.size("arr_col")` on null:
- A) returns -1
- B) returns 0
- C) errors
- D) returns null

### 44. `df.write.format("csv").save(path)`:
- A) writes header by default
- B) does not write header by default
- C) errors — must use `.csv(path)`
- D) deprecated

### 45. `Window.partitionBy().orderBy("ts")` (empty partitionBy):
- A) one global partition — ALL rows shuffled to one task (BAD)
- B) per-partition window
- C) errors
- D) ignored

### 46. Given two stages with the same shuffle, AQE may:
- A) merge them
- B) coalesce small post-shuffle partitions
- C) skip shuffles
- D) only B

### 47. `df.crossJoin(other)`:
- A) every row of df with every row of other
- B) only matching keys
- C) errors if `spark.sql.crossJoin.enabled = false`
- D) A and possibly C depending on config

### 48. A DataFrame join is internally a stage with:
- A) one map task per partition
- B) shuffle + sort + merge for SortMergeJoin
- C) just shuffle for BroadcastHashJoin
- D) B and C

### 49. Given:
```python
df.repartition(1).write.parquet(path)
```
- A) writes 1 file
- B) writes 1 file per executor
- C) deprecated
- D) errors

### 50. `coalesce(1)` before write:
- A) writes 1 file but loses parallelism
- B) keeps all parallelism
- C) errors
- D) writes empty file

### 51. Setting `spark.sql.shuffle.partitions = 1` for a job processing 1 TB:
- A) fast
- B) slow — one task processes everything
- C) errors
- D) AQE will fix it

### 52. The order of operations Catalyst applies (simplified):
- A) physical → logical → analyzed
- B) parsed → analyzed → optimized logical → physical
- C) physical → optimized
- D) no order; all simultaneous

### 53. `df.write.option("compression", "snappy").parquet(path)`:
- A) Snappy compression (default for Parquet)
- B) overrides default
- C) errors — Parquet has no compression
- D) only for streaming

### 54. `df.persist()` without args:
- A) same as `df.cache()`
- B) only memory
- C) error
- D) only disk

### 55. To register a user-defined function for SQL queries:
- A) `spark.udf.register("name", f, ReturnType())`
- B) `@F.udf("name")` decorator
- C) `df.register("name", f)`
- D) impossible

### 56. After `df.createOrReplaceTempView("v")`:
- A) view persists across sessions
- B) view only exists for this SparkSession
- C) view written to disk
- D) errors

### 57. Given:
```python
df.where("a > 10").where("b < 5")
```
Equivalent to:
- A) `df.where("a > 10 AND b < 5")`
- B) `df.where("a > 10 OR b < 5")`
- C) error
- D) Catalyst optimizes to A

### 58. Reading a Parquet directory with mixed schemas:
- A) merges schemas with `mergeSchema=true`
- B) errors by default
- C) takes the first file's schema
- D) all of the above are valid behaviors depending on config

### 59. `df.printSchema()` triggers:
- A) an action
- B) only metadata read
- C) shuffle
- D) errors on streaming

### 60. The smallest cluster size that can run Spark:
- A) 1 driver + 1 executor (or master + worker)
- B) 3 nodes minimum
- C) 5 nodes minimum
- D) only local mode

---

## Answers

1. **B** — values 10, 12, 14, 16, 18 satisfy v > 8 → 5 rows.
2. **A** — max(20) - min(10) = 10.
3. **B** — only row 2 has non-null b.
4. **D** — `a` is not in scope; need `F.col("a")` or `df.a`.
5. **B** — without bucketing/sorting metadata, Spark re-shuffles for join.
6. **B** — cache freed, DataFrame still works.
7. **A** — partition discovery from path.
8. **A** — broadcasts what you ask for; can OOM driver if too big.
9. **B** — map-side combine.
10. **B** — coalesce reduces from 100 to 50.
11. **A** — repartition shuffles to 100.
12. **A** — write succeeded.
13. **C** — `collect()` not valid on streaming.
14. **B** — inner excludes null keys.
15. **A** — external shuffle service.
16. **D** — projection pruning means only the needed columns read.
17. **A** — extra job to find distinct values.
18. **B** — withColumnRenamed errors if column doesn't exist; check first.
19. **B** — first count caches; second count after unpersist recomputes.
20. **B** — Directed Acyclic Graph.
21. **B** — wide transformations.
22. **B** — partition-replace.
23. **A** — one task per core.
24. **A** — amortize init cost per partition.
25. **B** — non-deterministic without sort.
26. **A** — skewed partition replicated.
27. **A** — unbounded state.
28. **C** — both forms work.
29. **A** — uses default shuffle partitions.
30. **B** — 1:1 partition mapping.
31. **A** — works fine.
32. **C** — both equivalent.
33. **B** — bad, drives OOM.
34. **C** — 10 rows from range(10).
35. **B** — coalesce(int) only.
36. **B** — partition pruning.
37. **B** — plan grows; better to combine into one select.
38. **A** — eqNullSafe matches null==null, not null OR equal.
39. **B** — approximate fraction.
40. **A** — seed parameter.
41. **A** — 2 preceding + current row = 3-row window.
42. **A** — unique elements.
43. **D** — size of null = null.
44. **B** — header NOT written by default; need `option("header", "true")`.
45. **A** — global window = OOM trap.
46. **D** — AQE coalesces small post-shuffle partitions.
47. **D** — both — and config controls whether it errors.
48. **D** — both shuffle and broadcast patterns are joins.
49. **A** — 1 partition → 1 file.
50. **A** — single-threaded writer.
51. **B** — single task is the bottleneck.
52. **B** — parsed → analyzed → optimized → physical.
53. **A** — Snappy is default and you're being explicit.
54. **A** — default MEMORY_AND_DISK = cache().
55. **A** — spark.udf.register.
56. **B** — session-local.
57. **A** — chained filters are AND-ed.
58. **D** — config-dependent.
59. **B** — schema is metadata.
60. **A** — driver + 1 executor (local mode also valid).
