# 02 — PySpark cheatsheet

A condensed reference of every API you're likely to be tested on. Bookmark this for the day before the exam.

## SparkSession

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("my-app")
    .config("spark.sql.shuffle.partitions", 200)
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext           # for RDD operations (rare on the exam)
```

## Reading

```python
# CSV
df = spark.read.csv(path, header=True, inferSchema=True)
df = spark.read.option("header", True).csv(path)

# JSON
df = spark.read.json(path)
df = spark.read.json(path, multiLine=True)

# Parquet (preferred)
df = spark.read.parquet(path)

# Delta
df = spark.read.format("delta").load(path)

# With explicit schema (skips inferSchema scan)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])
df = spark.read.schema(schema).csv(path)

# Reading multiple files / dirs
df = spark.read.parquet("path1", "path2")
```

## Writing

```python
df.write.mode("overwrite").parquet(path)
df.write.mode("append").saveAsTable("db.table")
df.write.partitionBy("year", "month").parquet(path)
df.write.format("delta").mode("overwrite").save(path)
```

Modes: `append`, `overwrite`, `ignore`, `error` (default).

## Select / project

```python
df.select("col1", "col2")
df.select(F.col("a"), F.col("b").alias("b_renamed"))
df.select("*", F.lit(1).alias("one"))
df.selectExpr("a as a2", "b + 1 as b1")
```

## withColumn / withColumnRenamed / drop

```python
df.withColumn("new_col", F.col("a") + F.col("b"))
df.withColumn("a", F.col("a") * 2)       # replaces a
df.withColumnRenamed("old", "new")
df.drop("col1", "col2")
```

`withColumn` is fine for 1-2 columns; chaining many is slow — use `select` instead.

## Filter / where (equivalent)

```python
df.filter(F.col("age") > 30)
df.filter("age > 30")            # SQL string
df.where("age > 30 AND name IS NOT NULL")
df.filter(F.col("status").isin("NEW", "PAID"))
df.filter(F.col("email").rlike(r"@example\.com$"))
df.filter(F.col("name").isNull())
df.filter(F.col("name").isNotNull())
```

## Aggregations

```python
from pyspark.sql import functions as F

df.groupBy("country").count()
df.groupBy("country").agg(
    F.sum("amount").alias("total"),
    F.avg("amount").alias("mean"),
    F.count("*").alias("n"),
    F.countDistinct("user_id").alias("unique_users"),
    F.min("date").alias("first"),
    F.max("date").alias("last"),
    F.collect_list("sku").alias("skus"),
    F.collect_set("sku").alias("unique_skus"),
)
```

`count("*")` vs `count("col")`: `count("*")` counts rows; `count("col")` counts non-null values.

## Joins

```python
a.join(b, "user_id")                          # inner, single column
a.join(b, ["user_id", "tenant"])              # inner, multiple
a.join(b, a.id == b.user_id)                  # inner, expression
a.join(b, a.id == b.user_id, "left")          # left outer
a.join(b, a.id == b.user_id, "right")         # right outer
a.join(b, a.id == b.user_id, "outer")         # full outer
a.join(b, a.id == b.user_id, "left_semi")     # rows from a that match b
a.join(b, a.id == b.user_id, "left_anti")     # rows from a with no match
a.join(F.broadcast(b), "user_id")             # force broadcast
a.crossJoin(b)                                # cartesian
```

Default join type is `inner`. Watch for column duplicates after joins — `drop` or use the `using` (single string) form to dedupe.

## Window functions

```python
from pyspark.sql.window import Window

w = Window.partitionBy("user_id").orderBy(F.desc("event_time"))

df.withColumn("rn", F.row_number().over(w))
df.withColumn("rank", F.rank().over(w))
df.withColumn("dense_rank", F.dense_rank().over(w))
df.withColumn("lag", F.lag("amount").over(w))
df.withColumn("lead", F.lead("amount").over(w))
df.withColumn("running_total", F.sum("amount").over(w))
df.withColumn("avg_3", F.avg("amount").over(w.rowsBetween(-2, 0)))   # 3-row sliding
```

Frame specifications:
- `rowsBetween(start, end)` — row-count window (e.g., last 3 rows).
- `rangeBetween(start, end)` — value-based window (e.g., last 7 days of timestamp).
- `Window.unboundedPreceding`, `Window.currentRow`, `Window.unboundedFollowing`.

## Sorting

```python
df.orderBy("col")                  # ascending (default)
df.orderBy(F.col("col").desc())
df.orderBy("col1", F.col("col2").desc())
df.sort("col")                     # alias for orderBy
df.sortWithinPartitions("col")     # cheaper — no shuffle, sort per partition
```

## Repartition / coalesce

```python
df.repartition(200)                # shuffle to 200 partitions
df.repartition("country")          # shuffle and partition by column
df.repartition(50, "country")
df.coalesce(10)                    # combine to 10, NO shuffle (can't increase)
```

`coalesce` is cheap but can leave skewed partitions. `repartition` is balanced but expensive.

## Caching

```python
df.cache()                                  # alias for persist(MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.unpersist()
```

Lazy: cache happens on the first action after `cache()`.

## Date / time

```python
F.current_date()
F.current_timestamp()
F.date_add("date_col", 7)
F.date_sub("date_col", 7)
F.datediff("end", "start")
F.year("date_col")
F.month("date_col")
F.dayofweek("date_col")
F.date_format("date_col", "yyyy-MM-dd")
F.to_date("string_col", "yyyy-MM-dd")
F.to_timestamp("string_col")
F.unix_timestamp("ts_col")
F.from_unixtime("epoch_seconds")
F.window("event_time", "5 minutes")        # streaming/agg windows
```

## String

```python
F.concat("a", "b")
F.concat_ws("-", "a", "b", "c")
F.lower("col")
F.upper("col")
F.trim("col")
F.length("col")
F.substring("col", 1, 3)
F.split("col", ",")                        # returns array
F.regexp_extract("col", r"(\d+)", 1)
F.regexp_replace("col", r"\s+", "_")
F.translate("col", "abc", "xyz")
```

## Null handling

```python
df.na.drop()                                # drop rows with any null
df.na.drop(subset=["a", "b"])
df.na.drop(how="all")                       # drop rows where ALL cols are null
df.na.fill(0)                               # fill all numeric nulls with 0
df.na.fill({"a": 0, "b": "unknown"})
df.na.replace([1, 2], [10, 20], "col")

F.coalesce(F.col("a"), F.col("b"), F.lit(0))
F.isnan("col")
F.isnull("col")
```

## When / case

```python
F.when(F.col("age") < 18, "minor") \
 .when(F.col("age") < 65, "adult") \
 .otherwise("senior")
```

## Arrays / structs

```python
F.array("a", "b")
F.array_contains("arr_col", "x")
F.explode("arr_col")                # one row per element
F.posexplode("arr_col")             # adds index
F.size("arr_col")

F.struct("a", "b").alias("nested")
df.select("nested.a", "nested.b")
```

## UDFs

```python
from pyspark.sql.types import StringType

@F.udf(StringType())
def upper(s):
    return s.upper() if s else None

df.withColumn("upper_name", upper("name"))
```

Prefer built-in functions; UDFs are slow (Python interop). For UDFs, prefer pandas UDFs:

```python
@F.pandas_udf(StringType())
def upper_p(s: pd.Series) -> pd.Series:
    return s.str.upper()
```

## Actions

```python
df.show(20, truncate=False)
df.head(5)
df.first()
df.take(10)                         # list of Rows
df.collect()                        # list of all Rows — careful, drives OOM
df.count()
df.toPandas()                       # local pandas DataFrame
df.write.parquet(...)               # also an action

df.foreach(f)                       # apply f per row
df.foreachPartition(f)              # apply f per partition
```

## Spark SQL

```python
df.createOrReplaceTempView("orders")
result = spark.sql("SELECT country, SUM(amount) FROM orders GROUP BY country")

spark.catalog.listTables()
spark.catalog.listColumns("orders")
spark.catalog.dropTempView("orders")
```

## Schemas

```python
df.printSchema()
df.schema
df.columns
df.dtypes

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, BooleanType,
    DateType, TimestampType, ArrayType, MapType, StructType
)
```

## Plans / debugging

```python
df.explain()                        # short plan
df.explain(True)                    # all 4 stages: parsed, analyzed, optimized, physical
df.explain("formatted")             # most readable
df.explain("cost")                  # with cost estimates (AQE)
```

## Configs you should know

| Config | Default | Purpose |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | # partitions after shuffle |
| `spark.default.parallelism` | total cores | # partitions for RDDs without shuffle |
| `spark.sql.adaptive.enabled` | true (3.2+) | enable AQE |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | AQE coalesce small shuffle parts |
| `spark.sql.adaptive.skewJoin.enabled` | true | AQE skew join handling |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | broadcast cutoff |
| `spark.sql.files.maxPartitionBytes` | 128MB | max file size per read partition |

## RDDs (minor on the exam)

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd.map(lambda x: x * 2).collect()
rdd.filter(lambda x: x > 2).count()
rdd.reduce(lambda a, b: a + b)
rdd.flatMap(lambda x: range(x)).collect()
df = rdd.toDF()
```

You can convert: `df.rdd`, `rdd.toDF()`.

## Streaming (minor — see module 05)

```python
stream = spark.readStream.format("rate").option("rowsPerSecond", 100).load()
q = stream.writeStream.format("console").outputMode("append").start()
q.awaitTermination()
```
