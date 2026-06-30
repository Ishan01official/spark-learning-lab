# 04 — Schemas and data types

## Why this matters

A schema is a contract. Explicit schemas (a) skip the cost of inference, (b) give you correct types instead of "everything is a string", (c) fail fast when upstream data drifts, and (d) document intent in code.

## The type system

PySpark's types live in `pyspark.sql.types`.

| Spark type | Python equivalent | Storage | Notes |
| --- | --- | --- | --- |
| `BooleanType` | `bool` | 1 byte | |
| `ByteType` | `int` | 1 byte | -128..127 |
| `ShortType` | `int` | 2 bytes | -32k..32k |
| `IntegerType` | `int` | 4 bytes | most common int |
| `LongType` | `int` | 8 bytes | epoch ms, ids |
| `FloatType` | `float` | 4 bytes | rarely needed |
| `DoubleType` | `float` | 8 bytes | default for floating point |
| `DecimalType(p,s)` | `decimal.Decimal` | varies | money — use this, not double |
| `StringType` | `str` | varies | |
| `BinaryType` | `bytes` | varies | |
| `DateType` | `datetime.date` | 4 bytes | |
| `TimestampType` | `datetime.datetime` | 8 bytes | timezone-aware (session TZ) |
| `TimestampNTZType` | `datetime.datetime` | 8 bytes | NO timezone (Spark 3.4+) |
| `ArrayType(elem)` | `list` | varies | nested |
| `MapType(k,v)` | `dict` | varies | nested |
| `StructType([StructField...])` | `Row` | varies | nested |

## Building schemas — two styles

### Programmatic
```python
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, TimestampType, DecimalType,
    ArrayType,
)

orders_schema = StructType([
    StructField("order_id",     LongType(),       nullable=False),
    StructField("customer_id",  LongType(),       nullable=False),
    StructField("country",      StringType(),     nullable=False),
    StructField("amount",       DecimalType(18, 2), nullable=False),
    StructField("created_at",   TimestampType(),  nullable=False),
    StructField("tags",         ArrayType(StringType()), nullable=True),
])
```

### DDL-string (shorter, common in Databricks)
```python
orders_schema = (
    "order_id LONG NOT NULL, customer_id LONG NOT NULL, country STRING, "
    "amount DECIMAL(18,2), created_at TIMESTAMP, tags ARRAY<STRING>"
)
df = spark.read.schema(orders_schema).parquet("s3://bucket/orders/")
```
Identical result, less ceremony.

## Why `DecimalType` for money

```python
>>> 0.1 + 0.2
0.30000000000000004
```
`DoubleType` cannot represent `0.1` exactly. Multiply, sum across millions of rows, and your totals drift by cents to dollars. `DecimalType(18, 2)` uses base-10 arithmetic and stays exact. Use it for any monetary or accounting column.

## Nullability — what `nullable=False` actually does

It's a **hint**, not a constraint. Spark trusts your declaration: it can choose plans assuming the column has no nulls. If reality disagrees, you get wrong results, not an error. Set `nullable=False` only when you have validated upstream that nulls are impossible.

## Nested types — working with them

```python
from pyspark.sql import functions as F

# An order with nested address and items
schema = """
order_id LONG,
customer STRUCT<id: LONG, name: STRING, address: STRUCT<city: STRING, zip: STRING>>,
items   ARRAY<STRUCT<sku: STRING, qty: INT, price: DECIMAL(18,2)>>
"""
df = spark.read.schema(schema).json("orders.json")

# Reach into struct fields with dot notation
df.select("customer.name", "customer.address.city").show()

# Explode array-of-struct into one row per item
df_items = df.select(
    "order_id",
    F.explode("items").alias("item")
).select("order_id", "item.sku", "item.qty", "item.price")
```

`explode` is a *wide-ish* op: it doesn't shuffle but it multiplies row counts. A 10M-row orders set with avg 5 items each becomes 50M rows — keep that in mind downstream.

## Inferring a schema once, then reusing it

For exploratory work it's fine to let Spark infer. Capture the result and freeze it:

```python
inferred = spark.read.option("inferSchema", "true").csv("sample.csv").schema
print(inferred.json())      # paste this into your code as the canonical schema
```

## Casting

```python
df = df.withColumn("amount", F.col("amount").cast("decimal(18,2)"))
df = df.withColumn("created_at", F.to_timestamp("created_str", "yyyy-MM-dd HH:mm:ss"))
```
Cast fails *silently to null* by default in PERMISSIVE mode. To fail loud, run in `ANSI` mode:
```python
spark.conf.set("spark.sql.ansi.enabled", "true")
# Now bad casts throw, division-by-zero throws, etc. Recommended for production.
```

## Industry use cases

| Pattern | Schema choice |
| --- | --- |
| Money / accounting | `DecimalType(18, 2)` or `DecimalType(38, 8)` for crypto |
| Event ingest, schema may evolve | Define struct explicitly; new fields default to null on old data |
| API ingest with optional fields | All optional fields `nullable=True`, validated downstream |
| Time-series IoT | `TimestampType` in UTC, partition by `date` derived from timestamp |
| Geographic data | `ArrayType(DoubleType())` for `[lon, lat]` or use ST_* with Sedona |

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| Numbers come back as `StringType` | inferred but column has one bad row | explicit schema |
| Sum of millions of doubles drifts | float math | `DecimalType` |
| `NULL` in NOT NULL column | upstream broke contract | run a validation count, fail the job |
| `_corrupt_record` filled with rows | schema doesn't match actual data | `mode=DROPMALFORMED` is *not* the fix — find and fix upstream |
| Timezone shift in timestamps | session tz differs from data tz | set `spark.sql.session.timeZone=UTC` everywhere |

## References

- 📚 [LS Ch.4 §"Defining a Schema"]
- 📚 [HPS Ch.3 §"Schema Inference and Specification"]
- 📚 [DAS Ch.3 §"Schemas and Types"]
- 📺 ["PySpark schema explained" — common talk on YouTube](https://www.youtube.com/results?search_query=pyspark+schema+explained)
