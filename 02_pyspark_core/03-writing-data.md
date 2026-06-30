# 03 — Writing data: modes, partitioning, bucketing

## Why this matters

How you *write* data dictates how fast every *read* against it will be — possibly for years. A bad partition layout written once is paid for on every query forever.

## The unified writer API

```python
(df.write
   .format("parquet")             # csv / json / parquet / orc / jdbc / avro / delta
   .mode("overwrite")             # append | overwrite | errorifexists | ignore
   .option("compression", "snappy")
   .partitionBy("country", "year")
   .save("s3://bucket/orders/"))
```

## Save modes — what each does

| Mode | Behavior if path exists | Use when |
| --- | --- | --- |
| `append` | adds new files to existing dir | streaming-ish inserts, hourly partitions |
| `overwrite` | wipes path, writes fresh | full refreshes, dev/test |
| `errorifexists` (default) | throws `AnalysisException` | production safety net |
| `ignore` | silently does nothing | idempotent jobs, "create if absent" |

**Gotcha:** `overwrite` wipes the *entire path*, even partitions you didn't intend to touch. To only replace partitions matching the new data, use **dynamic partition overwrite**:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
(df.write.mode("overwrite").partitionBy("date").save("s3://bucket/orders/"))
# Only partitions present in df get replaced; older partitions stay.
```

Delta Lake handles this natively (see module 04).

## Partitioning (physical layout)

`partitionBy("country", "year")` lays files out as:
```
s3://bucket/orders/country=US/year=2024/part-00000.parquet
s3://bucket/orders/country=US/year=2025/part-00000.parquet
s3://bucket/orders/country=IN/year=2024/part-00000.parquet
```
A reader that filters `WHERE country='US' AND year=2024` reads exactly *one* directory.

### How to pick partition columns

Rules of thumb, in order of importance:

1. **Low to medium cardinality.** `country` (~200 values) ✅. `user_id` (millions) ❌ — one tiny file per user kills the filesystem.
2. **Frequently filtered on.** Every read filter should ideally touch a partition column.
3. **Each partition should hold ≥ 1 GB** of data when fully loaded. Smaller and you hit the *small files problem*: thousands of <100 MB files mean thousands of tasks doing thousandths of work each.
4. **Time partitions** (`year`, `month`, `date`) are almost always a good idea for event data.

### Sizing files inside each partition

```python
(df.repartition(8, "country", "year")          # control task count BEFORE write
   .write.partitionBy("country", "year")
   .parquet("s3://bucket/orders/"))
```
Without the repartition, each upstream task writes one file per partition value it sees → file explosion. The repartition forces 8 files per directory.

Target file size: **128 MB – 1 GB** per Parquet file. Smaller wastes metadata; larger hurts parallelism.

## Bucketing (logical layout, Hive-style)

```python
(df.write
   .bucketBy(50, "customer_id")               # 50 buckets, hash on customer_id
   .sortBy("customer_id")
   .saveAsTable("warehouse.orders_bucketed"))
```
Bucketing pre-shuffles data into N files by hash of a key. Two tables bucketed on the same key with the same N can be joined *without a shuffle*. The cost is one-time at write; the win is on every subsequent join.

**Limits:** requires Hive metastore (`saveAsTable`). Most teams have replaced this with Delta + Z-order or partition-aligned joins instead.

## Format-by-format write notes

### Parquet
```python
df.write.parquet("path/")                                     # default snappy
df.write.option("compression", "zstd").parquet("path/")       # smaller, slightly slower
df.write.option("parquet.block.size", 134217728).parquet("path/")  # 128 MB row groups
```

### CSV
```python
(df.write
   .option("header", "true")
   .option("compression", "gzip")
   .csv("path/"))
```
**Beware**: gzip CSVs are not splittable. Future readers will be slow. Prefer `compression=none` or `bzip2` if CSV is mandatory.

### JDBC
```python
(df.write
   .format("jdbc")
   .option("url", "jdbc:postgresql://...")
   .option("dbtable", "orders_stg")
   .option("batchsize", 10000)               # rows per insert
   .option("isolationLevel", "NONE")         # speeds up bulk; check your DB!
   .mode("append")
   .save())
```
Writes are parallel by partition count. Too many partitions → connection storm on the DB. Coalesce to a few before writing.

## `saveAsTable` vs `save`

- `save(path)` writes files only. Spark forgets about them when the session ends.
- `saveAsTable(name)` writes files **and** registers the table in the metastore so `spark.table("name")` and `spark.sql("SELECT ... FROM name")` work in any future session.

Production lake: use a metastore (Hive, Glue, Unity Catalog) and `saveAsTable`.

## Industry use cases

| Pattern | Layout |
| --- | --- |
| Daily orders ETL, queries always filter by date | `partitionBy("date")`, files 256 MB–1 GB each |
| Multi-tenant SaaS analytics | `partitionBy("tenant_id", "date")` — tenant first so single-tenant queries scan minimal data |
| User-event clickstream, queried by event_name + date | `partitionBy("date", "event_name")` |
| Customer-360 wide table, joined by customer_id | `bucketBy(200, "customer_id")` *or* Delta Z-order on customer_id |

## Scale notes

| Scenario | Outcome |
| --- | --- |
| Write 100 GB with no partitioning | one big dataset, fast write, slow filtered reads (full scan) |
| Write 100 GB partitioned by `date` (90 days) | 90 dirs × ~1 GB each → fast date-filtered reads |
| Write 100 GB partitioned by `user_id` (1M users) | **disaster**: 1M tiny files, listing takes longer than the read |
| Write 1 TB with 200 shuffle partitions but no repartition before write | 200 files per partition dir → potentially too many small files |
| Write 10 TB without dynamic overwrite | `overwrite` wipes 10 TB to write 100 GB of new data |

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| Job succeeds, table appears empty | wrote to wrong path, or `mode=ignore` and path existed | check audit logs, set `errorifexists` |
| Hours-long writes for small data | thousands of tiny output files | `df.repartition(N)` before write |
| `FileAlreadyExistsException` mid-write | retry hit committed file | enable `spark.sql.sources.commitProtocolClass` or use Delta |
| Wrong rows appear in old partitions | `overwrite` without dynamic mode | set `partitionOverwriteMode=dynamic` |
| Reads from JDBC dest fail with deadlocks | too many parallel writers | `df.coalesce(8)` before write |

## References

- 📚 [LS Ch.5 §"Writing DataFrames to External Sources" / Ch.4 §"Saving as a Table"]
- 📚 [HPS Ch.3 §"DataFrame Write API" / Ch.5 §"Partitioning"]
- 📚 [DAS Ch.4 §"Output Formats"]
- 📺 ["Spark file output committers / S3" — see Steve Loughran talks](https://www.youtube.com/results?search_query=spark+s3+committer+steve+loughran)
- 📺 ["Choosing partition columns in Spark"](https://www.youtube.com/results?search_query=spark+partitioning+strategy)
