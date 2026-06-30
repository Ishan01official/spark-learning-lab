# 06 — Built-in functions tour

## Why this matters

Every built-in function in `pyspark.sql.functions` runs in the JVM in native code. Every UDF you write runs in a Python worker process with serialization overhead. **Always prefer built-ins.** The list is large; you only need to know the ~50 that come up daily.

Convention: `from pyspark.sql import functions as F`.

## String functions

```python
F.lower("name"), F.upper("name")
F.length("name")
F.trim("name"), F.ltrim("name"), F.rtrim("name")
F.lpad("code", 5, "0"), F.rpad("code", 5, " ")
F.substring("name", 1, 3)                   # 1-indexed in Spark SQL!
F.substring_index("path", "/", -1)          # filename from a path
F.concat("first", F.lit(" "), "last")
F.concat_ws(", ", "city", "state", "zip")   # null-safe joiner
F.split("csv_field", ",")                   # returns Array<String>
F.regexp_replace("phone", "[^0-9]", "")
F.regexp_extract("ua", r"Chrome/(\d+)", 1)
F.translate("col", "abc", "ABC")            # char-by-char replace
F.initcap("name")                           # "john smith" → "John Smith"
F.format_string("%s-%d", "name", "id")
F.repeat("x", 5)                            # "xxxxx"
F.reverse("name")
```

## Date and timestamp functions

```python
F.current_date(), F.current_timestamp()
F.to_date("ts_str", "yyyy-MM-dd")
F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss")
F.date_format("ts", "yyyy-MM")              # → string "2024-07"
F.year("ts"), F.month("ts"), F.dayofmonth("ts")
F.dayofweek("ts")                           # 1 = Sunday
F.hour("ts"), F.minute("ts"), F.second("ts")
F.weekofyear("ts")
F.date_add("ts", 7), F.date_sub("ts", 7)
F.add_months("ts", 1)
F.months_between("end_ts", "start_ts")
F.datediff("end_ts", "start_ts")            # in days
F.last_day("ts")                            # last day of that month
F.trunc("ts", "month")                      # truncate to month
F.date_trunc("hour", "ts")                  # truncate timestamp
F.unix_timestamp("ts")                      # epoch seconds
F.from_unixtime(F.col("epoch"))
F.window("event_ts", "5 minutes")           # tumbling window struct (for streaming)
```

## Numeric / math

```python
F.abs("x"), F.sqrt("x"), F.pow("x", 2)
F.exp("x"), F.log("x"), F.log10("x")
F.round("x", 2), F.ceil("x"), F.floor("x")
F.greatest("a","b","c"), F.least("a","b","c")
F.rand(seed=42), F.randn(seed=42)
```

## Null handling

```python
F.coalesce("a", "b", F.lit(0))              # first non-null
F.nvl("a", F.lit(0))                        # null-safe (Spark 3.5+)
F.when(F.col("x").isNull(), F.lit("N/A")).otherwise(F.col("x"))
F.col("x").isNull(), F.col("x").isNotNull()
```

Aggregations ignore nulls by default — but `count("*")` counts nulls while `count("col")` does not.

## Collection / array functions

```python
F.array("a", "b", "c")                      # build array column
F.array_contains("tags", "premium")
F.size("tags")                              # array length
F.array_distinct("tags")
F.array_intersect("tags1", "tags2")
F.array_union("tags1", "tags2")
F.array_except("tags1", "tags2")
F.sort_array("tags", asc=True)
F.explode("tags")                           # array → rows
F.posexplode("tags")                        # adds position index
F.explode_outer("tags")                     # keeps row even when array is null/empty
F.flatten("arr_of_arr")
F.element_at("tags", 1)                     # 1-indexed; -1 = last
F.slice("tags", 1, 3)
F.aggregate("nums", F.lit(0), lambda acc, x: acc + x)   # fold over an array
F.transform("nums", lambda x: x * 2)        # map over an array
F.filter("nums", lambda x: x > 0)
F.exists("nums", lambda x: x < 0)
```

## JSON

```python
F.get_json_object("json_str", "$.user.id")
F.json_tuple("json_str", "user.id", "user.name")
F.from_json("json_str", schema)             # parse string → struct
F.to_json("struct_col")                     # struct → string
F.schema_of_json('{"a":1,"b":"x"}')         # infer JSON schema
```

## Aggregate functions (used inside `.agg(...)`)

```python
F.count("*"), F.count("col"), F.countDistinct("col1", "col2")
F.sum("col"), F.avg("col"), F.min("col"), F.max("col")
F.stddev("col"), F.variance("col")
F.approx_count_distinct("col", rsd=0.05)    # HyperLogLog; 1000× faster on large data
F.first("col", ignorenulls=True)
F.last("col")
F.collect_list("col"), F.collect_set("col")
F.percentile_approx("col", 0.5)             # median
F.percentile_approx("col", [0.5, 0.9, 0.99])
F.skewness("col"), F.kurtosis("col")
```

## Window-only functions (require an `OVER` window)

```python
F.row_number()
F.rank()
F.dense_rank()
F.lag("col", 1), F.lead("col", 1)
F.cume_dist()
F.percent_rank()
F.ntile(4)
```
(See `09-window-functions.md` for usage.)

## Hashing and IDs

```python
F.md5("col"), F.sha2("col", 256)
F.crc32("col")
F.hash("col1", "col2")                      # Murmur3, deterministic across runs
F.xxhash64("col1", "col2")
F.monotonically_increasing_id()             # 64-bit unique; NOT sequential
F.uuid()
```

`monotonically_increasing_id` is unique within a single Spark job but **not contiguous** — values jump. Don't use it as a surrogate key unless you understand that.

## Conditional logic

```python
F.when(cond, val).when(cond, val).otherwise(val)
F.coalesce(a, b, c)
F.nullif(a, b)                              # null if equal, else a
F.iff(cond, a, b)                           # Spark 3.5+
F.greatest(a,b), F.least(a,b)
```

## Casting

```python
F.col("x").cast("integer")
F.col("x").cast("decimal(18,2)")
F.col("x").cast(IntegerType())
```

## Industry use cases

| Pattern | Functions |
| --- | --- |
| Parse and bucket timestamps | `to_timestamp`, `date_trunc`, `window` |
| Clean user input | `trim`, `lower`, `regexp_replace`, `coalesce` |
| Extract from JSON event payloads | `get_json_object`, `from_json` |
| Pseudonymize PII | `sha2("email", 256)` |
| Approximate cardinality dashboards | `approx_count_distinct` |
| Flatten nested API responses | `explode`, `posexplode_outer` |
| Compute percentiles for SLAs | `percentile_approx` |
| Tag rows with a deterministic hash for A/B | `xxhash64` modulo N |

## Scale notes

| Function | Notes |
| --- | --- |
| `countDistinct` | exact, requires shuffle, expensive on high cardinality |
| `approx_count_distinct` | sub-second on billions of rows, ~2% error |
| `collect_list` / `collect_set` | unbounded per group — OOM risk for skewed keys |
| UDF wrapping `re.match` | 10–50× slower than `regexp_extract` |
| `explode` on huge arrays | row count can explode by 10–1000× — plan downstream |

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| `AnalysisException: missing argument` | function expects Column, got string in wrong position | wrap arg in `F.col(...)` or `F.lit(...)` |
| All values become null after `to_timestamp` | format string doesn't match | check ISO vs locale formats; try without format arg |
| `OutOfMemoryError` on `collect_list` | one key has millions of values | switch to top-N pattern or `approx_count_distinct` |
| `regexp_extract` returns empty strings | regex didn't match — Spark returns "" not null | wrap in `nullif(..., "")` |

## References

- 📚 [LS Ch.6 §"Spark SQL Built-in Functions" appendix]
- 📚 [HPS Ch.3 §"Working with Columns"]
- 📚 [DAS — usage threaded throughout chapters]
- 📺 [Official function reference site — `spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- 📺 ["PySpark built-in functions tour"](https://www.youtube.com/results?search_query=pyspark+built+in+functions+tour)
