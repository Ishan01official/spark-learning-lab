# Mock exam 1

60 questions. ~120 minutes. Answers at the end.

---

### 1. What is the default value of `spark.sql.shuffle.partitions`?
- A) 100
- B) 200
- C) 400
- D) equal to # cores

### 2. Which transformation is wide (causes a shuffle)?
- A) `filter`
- B) `select`
- C) `groupBy().count()`
- D) `withColumn`

### 3. `df.coalesce(1)` will:
- A) shuffle to 1 partition
- B) combine partitions to 1 without shuffle
- C) only work if df has 1 partition already
- D) error

### 4. The default join type in `df.join(other, "id")` is:
- A) left outer
- B) right outer
- C) inner
- D) full outer

### 5. Which is NOT an action?
- A) `count()`
- B) `collect()`
- C) `filter()`
- D) `show()`

### 6. `df.cache()` is:
- A) eager — caches immediately
- B) lazy — caches on first action
- C) only works on RDDs
- D) deprecated

### 7. Which output mode requires a watermark for streaming aggregations?
- A) append
- B) update
- C) complete
- D) none of them

### 8. `F.lit(1)` produces:
- A) a column with literal 1
- B) a row of literal 1
- C) an integer 1
- D) an error — `lit` requires a string

### 9. After `df.dropDuplicates(["id"])`, which row is kept when duplicates exist?
- A) the first
- B) the last
- C) random / undefined
- D) the one with smallest values

### 10. Which read option triggers two passes over the file?
- A) `header=True`
- B) `inferSchema=True`
- C) `mode="FAILFAST"`
- D) `multiLine=True`

### 11. Given:
```python
df = spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "v"])
df.na.drop().count()
```
Result?
- A) 0
- B) 1
- C) 2
- D) 3

### 12. `df.repartition(10, "country")`:
- A) reduces to 10 partitions, no shuffle
- B) shuffles to 10 partitions, partitioned by country
- C) creates 10 files on disk
- D) creates 10 country directories

### 13. Which DataFrame is broadcast in:
```python
small = spark.read.parquet("a")
big = spark.read.parquet("b")
big.join(F.broadcast(small), "key")
```
- A) `small`
- B) `big`
- C) both
- D) neither

### 14. `df.persist(StorageLevel.MEMORY_ONLY)` vs `df.cache()` are:
- A) different — `cache()` is MEMORY_AND_DISK
- B) different — `cache()` is MEMORY_ONLY
- C) identical
- D) `cache()` only works on RDDs

### 15. SQL:
```sql
SELECT country, SUM(amount) FROM orders GROUP BY country HAVING SUM(amount) > 100
```
What does HAVING do that WHERE can't?
- A) filter on aggregated values
- B) filter NULLs
- C) sort
- D) join

### 16. `F.col("a") + F.col("b")` if `a=1`, `b=null`:
- A) 1
- B) null
- C) 0
- D) error

### 17. To convert string `"2024-09-15"` to date:
- A) `F.to_date("col", "yyyy-MM-dd")`
- B) `F.cast("date")`
- C) both
- D) `F.parse_date`

### 18. Which is a narrow transformation?
- A) `groupBy`
- B) `repartition`
- C) `filter`
- D) `distinct`

### 19. The Spark UI tab showing cached DataFrames is:
- A) Jobs
- B) Stages
- C) Storage
- D) Executors

### 20. `df.write.partitionBy("year").parquet(path)`:
- A) shuffles by year then writes
- B) writes one directory per distinct year value
- C) writes one file per year
- D) errors if year not in df.columns

### 21. Given:
```python
w = Window.partitionBy("user").orderBy("ts")
df.withColumn("sum", F.sum("amt").over(w))
```
This computes:
- A) total per user
- B) running sum per user, ordered by ts
- C) running sum across all users
- D) per-row sum

### 22. `df.select("a", "b").show()` will:
- A) print all rows
- B) print first 20 rows by default
- C) print first 10 rows
- D) return a DataFrame, not print

### 23. To make a Python function usable in a DataFrame:
- A) `F.udf(my_func, ReturnType())`
- B) `@F.udf(ReturnType())`
- C) `spark.udf.register("name", my_func, ReturnType())`
- D) all of the above

### 24. Which produces the SAME result as `df.distinct()`?
- A) `df.dropDuplicates()`
- B) `df.groupBy(*df.columns).count().drop("count")`
- C) both A and B
- D) neither

### 25. Reading Parquet with `df = spark.read.parquet("path")`, the schema is:
- A) inferred from the first file
- B) read from Parquet's embedded metadata
- C) requires explicit `.schema()` call
- D) defaulted to StringType for all columns

### 26. `df.explain()` shows:
- A) physical plan only
- B) logical and physical plans
- C) only the parsed plan
- D) only when there's an error

### 27. AQE (Adaptive Query Execution) can:
- A) coalesce small shuffle partitions
- B) switch a sort-merge join to broadcast
- C) handle skewed joins
- D) all of the above

### 28. `spark.sql.autoBroadcastJoinThreshold` default:
- A) 1 MB
- B) 10 MB
- C) 100 MB
- D) disabled by default

### 29. Which file format does NOT support predicate pushdown?
- A) Parquet
- B) ORC
- C) JSON
- D) Delta

### 30. To read JSON where each record spans multiple lines:
- A) default works
- B) `.option("multiLine", "true")`
- C) `.option("wholeFile", "true")`
- D) impossible

### 31. In Structured Streaming, the role of a checkpoint is:
- A) speed up writes
- B) store offsets + state for restart
- C) cache the result table
- D) optional, only for debugging

### 32. Stream-stream join requires:
- A) one watermark
- B) two watermarks (one per side)
- C) two watermarks AND a time predicate
- D) no special config

### 33. Given:
```python
df.groupBy("country").agg(F.collect_list("city").alias("cities"))
```
Type of `cities` column:
- A) string (comma-separated)
- B) array<string>
- C) struct
- D) map

### 34. `F.explode("array_col")`:
- A) one row per array element
- B) one row per array
- C) returns array length
- D) errors on null arrays

### 35. `df.coalesce(1).write.parquet(path)`:
- A) writes 1 file
- B) writes 1 file per executor
- C) writes 1 file per partition before coalesce
- D) errors if df has more than 1 partition

### 36. RDDs vs DataFrames — main difference:
- A) DataFrames have a schema
- B) RDDs are deprecated
- C) RDDs run faster
- D) DataFrames don't support transformations

### 37. To register a DataFrame as a SQL temp view:
- A) `df.createTempView("name")`
- B) `df.createOrReplaceTempView("name")`
- C) `spark.catalog.createTempView("name", df)`
- D) A and B both

### 38. `F.regexp_extract("col", r"(\d+)", 1)`:
- A) extracts first digit group
- B) extracts all digits
- C) replaces digits with 1
- D) returns count of digit groups

### 39. Default trigger for `writeStream` is:
- A) every 1 second
- B) every 5 seconds
- C) fire next batch as soon as previous done
- D) one batch then stop

### 40. `df.write.mode("ignore").parquet(path)`:
- A) overwrites
- B) appends
- C) does nothing if path exists
- D) errors if path exists

### 41. `Window.unboundedPreceding`:
- A) start of partition
- B) row 0
- C) -infinity
- D) errors

### 42. Setting a config at runtime:
- A) `spark.conf.set("...", "...")` for SQL configs
- B) `spark.conf.set("...", "...")` for executor configs too
- C) only via `spark-submit`
- D) cannot be done

### 43. `df.count()` after `df.cache()` and then again:
- A) second call uses cache, faster
- B) both calls re-compute
- C) both calls use cache
- D) first call computes + caches, second uses cache

### 44. To check if a column is in df:
- A) `"col" in df.columns`
- B) `df.hasColumn("col")`
- C) `df.contains("col")`
- D) `df.col_exists("col")`

### 45. `F.when(...).otherwise(...)`:
- A) only for groupBy
- B) creates a CASE WHEN expression
- C) deprecated; use ternary
- D) requires SQL string

### 46. The driver memory is set with:
- A) `spark.driver.memory`
- B) `--driver-memory` in spark-submit
- C) both
- D) cannot be configured

### 47. A DataFrame's plan is recomputed on each action UNLESS:
- A) it's cached
- B) it's a SQL query
- C) it's < 1 GB
- D) it's read from Parquet

### 48. `df.toPandas()`:
- A) converts the whole DF to local pandas
- B) only sample 1000 rows
- C) only first 20 rows
- D) errors for DataFrames > 1 GB

### 49. `pyspark.sql.functions` is conventionally imported as:
- A) `import pyspark.sql.functions as f`
- B) `from pyspark.sql import functions as F`
- C) `from pyspark.sql.functions import *`
- D) all are common; B is most common

### 50. `df.sortWithinPartitions("col")`:
- A) global sort
- B) sort each partition independently, no shuffle
- C) errors
- D) sorts by column then partition

### 51. To enable AQE explicitly:
- A) `spark.conf.set("spark.sql.adaptive.enabled", "true")`
- B) enabled by default since 3.2
- C) both A and B work
- D) not possible

### 52. After a join, two columns named "id" exist. To select them:
- A) `df.select("id")` — uses the first one
- B) `df.select(a.id, b.id)` — qualify with aliases
- C) `df.select("a.id", "b.id")` — works if aliased
- D) B and C

### 53. `F.broadcast(small_df)`:
- A) forces broadcast of small_df
- B) only a hint, may be ignored
- C) both — it's a strong hint
- D) errors if small_df is too big

### 54. Streaming `dropDuplicates(["id"])` without watermark:
- A) state grows unboundedly
- B) works fine
- C) errors immediately
- D) only first 1000 ids tracked

### 55. The pattern `select(*[F.col(c) for c in df.columns])`:
- A) identical to `df`
- B) selects all columns explicitly
- C) errors
- D) renames all columns

### 56. `df.write.format("delta")` requires:
- A) `delta-spark` package installed
- B) Spark 3.0+
- C) appropriate Spark configs
- D) all of the above

### 57. The Catalyst optimizer rules include:
- A) predicate pushdown
- B) column pruning
- C) constant folding
- D) all of the above

### 58. RDD `mapPartitions(f)` differs from `map(f)`:
- A) `mapPartitions` operates on an iterator per partition
- B) `map` is wide
- C) `mapPartitions` is deprecated
- D) no difference

### 59. `F.lit(None)`:
- A) literal null
- B) errors
- C) returns 0
- D) returns "None"

### 60. The Spark history server shows:
- A) currently running jobs
- B) completed jobs from event logs
- C) cluster topology only
- D) deprecated

---

## Answers

1. **B** — 200.
2. **C** — groupBy causes a shuffle.
3. **B** — coalesce avoids shuffle.
4. **C** — inner.
5. **C** — filter is a transformation.
6. **B** — lazy.
7. **A** — append needs a watermark for streaming aggregations.
8. **A** — column with literal.
9. **C** — undefined which row survives.
10. **B** — inferSchema reads twice.
11. **C** — 2 (drops the null row).
12. **B** — shuffle + partition by column.
13. **A** — small is broadcast.
14. **A** — `cache()` = MEMORY_AND_DISK.
15. **A** — HAVING filters on agg results.
16. **B** — null propagates.
17. **C** — both work.
18. **C** — filter is narrow.
19. **C** — Storage tab.
20. **B** — directory per partition value.
21. **B** — running sum (default frame with orderBy).
22. **B** — 20 default.
23. **D** — all three forms work.
24. **C** — both produce same.
25. **B** — Parquet has embedded schema.
26. **A** — physical plan by default.
27. **D** — all three are AQE features.
28. **B** — 10 MB.
29. **C** — JSON has no row groups / stats.
30. **B** — multiLine option.
31. **B** — offsets + state for restart.
32. **C** — two watermarks + time predicate.
33. **B** — array<string>.
34. **A** — one row per element.
35. **A** — 1 partition → 1 file.
36. **A** — DataFrames have a schema.
37. **D** — A and B both, B is safer.
38. **A** — extracts first group of digits.
39. **C** — no trigger = run continuously.
40. **C** — ignore mode skips if exists.
41. **A** — start of partition for window.
42. **A** — SQL configs can be set at runtime, executor configs cannot.
43. **D** — first computes + caches, then uses.
44. **A** — Pythonic.
45. **B** — CASE WHEN.
46. **C** — both.
47. **A** — cache memoizes.
48. **A** — full conversion.
49. **D** — B is most common.
50. **B** — per-partition sort.
51. **C** — works either way; default on since 3.2.
52. **D** — qualify with aliases.
53. **C** — strong hint.
54. **A** — state grows.
55. **B** — explicit select-all.
56. **D** — all needed.
57. **D** — all are Catalyst rules.
58. **A** — iterator per partition.
59. **A** — literal null.
60. **B** — completed jobs from event logs.
