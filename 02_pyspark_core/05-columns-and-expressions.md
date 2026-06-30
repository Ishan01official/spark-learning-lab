# 05 — Columns and expressions

## Why this matters

Spark transforms DataFrames by composing column expressions. The same result can be expressed five different ways with very different readability and (occasionally) performance.

## What is a `Column`?

A `Column` is a *symbolic expression*, not data. It refers to "the value of column X for each row." Nothing is executed until an action runs.

```python
from pyspark.sql import functions as F
F.col("amount")                          # type: Column
F.col("amount") * 1.05                   # type: Column (multiplication is an expression)
F.col("amount") > 100                    # type: Column (boolean expression)
```

## Five ways to reference a column

```python
df.amount                # attribute access
df["amount"]             # bracket access
F.col("amount")          # explicit, works everywhere (recommended)
F.column("amount")       # alias for col
"amount"                 # bare string, works in some methods (.select, .groupBy)
```

Prefer `F.col(...)`. It's unambiguous and works in every method including the ones where a string isn't allowed (e.g. `withColumn`).

## Building expressions

```python
F.col("amount") * 1.18                                # arithmetic
F.col("amount").between(10, 100)                      # range
F.col("name").startswith("A")                         # string
F.col("status").isin("OPEN", "PARTIAL")               # membership
F.col("notes").isNull()                               # null check
~F.col("notes").isNull()                              # negation
(F.col("a") > 0) & (F.col("b") < 5)                   # AND — use & not 'and'
(F.col("a") > 0) | (F.col("b") < 5)                   # OR  — use | not 'or'
```

**Critical:** use `&`, `|`, `~` for logical ops on Columns. Python's `and`, `or`, `not` won't work — they evaluate truthiness of the Python object, not the row.

## `select` vs `withColumn` vs `selectExpr`

```python
df.select("order_id", F.col("amount") * 1.18)         # builds a new DataFrame; full control over output columns

df.withColumn("amount_with_tax", F.col("amount") * 1.18)  # keep all columns + add/replace one

df.selectExpr("order_id", "amount * 1.18 AS amount_with_tax")  # SQL string expressions
```

| Method | Use when |
| --- | --- |
| `select` | building a new shape — drop and rename columns |
| `withColumn` | adding or replacing a single column, keeping the rest |
| `selectExpr` | quick SQL one-liners, especially when copy-pasting from SQL |

**Performance note:** chaining 30 `withColumn` calls is fine — Catalyst collapses them into one projection. But it can confuse stack traces; for big projections, prefer one `select(*cols)`.

## `when` / `otherwise` (the Spark `CASE WHEN`)

```python
df.withColumn(
    "tier",
    F.when(F.col("amount") >= 1000, "GOLD")
     .when(F.col("amount") >= 100,  "SILVER")
     .otherwise("BRONZE")
)
```
Reads top-down. First match wins. `otherwise` defaults to null if omitted.

## `expr` and `F.expr` — SQL strings as columns

```python
df.withColumn("net", F.expr("amount * (1 - discount_pct / 100)"))
df.filter(F.expr("status IN ('OPEN','PENDING') AND amount > 100"))
```
Useful when an expression reads more naturally in SQL than in Python operator chains. Same cost.

## Aliasing

```python
df.select(F.col("amount").alias("price"),
          (F.col("amount") * F.col("qty")).alias("line_total"))
```
Without alias, computed columns get auto-generated names like `(amount * qty)` — ugly and breaks downstream code that names columns.

## `withColumnRenamed` and bulk renaming

```python
df.withColumnRenamed("amt", "amount")                 # one column

# Bulk rename — programmatic
rename = {"amt": "amount", "cust": "customer_id"}
for old, new in rename.items():
    df = df.withColumnRenamed(old, new)

# Or with select for one pass
df = df.select(*[F.col(c).alias(rename.get(c, c)) for c in df.columns])
```

## Dropping columns

```python
df.drop("debug_col", "scratch")              # multiple cols, no error if absent
```

## Inspecting columns

```python
df.columns                # ['order_id', 'amount', ...]
df.dtypes                 # [('order_id','bigint'), ('amount','decimal(18,2)'), ...]
df.printSchema()          # tree view
df.schema                 # StructType object
df.schema["amount"]       # StructField object
df.schema["amount"].dataType
```

## Common patterns

### Coalesce nulls
```python
df.withColumn("country", F.coalesce(F.col("country"), F.lit("UNKNOWN")))
```

### Conditional assignment using a lookup
```python
mapping = F.create_map(
    F.lit("US"), F.lit("United States"),
    F.lit("IN"), F.lit("India"),
)
df.withColumn("country_full", mapping[F.col("country")])
```

### Boolean to int
```python
df.withColumn("is_big", (F.col("amount") > 100).cast("int"))
```

### Building a struct
```python
df.withColumn("address", F.struct(F.col("street"), F.col("city"), F.col("zip")))
```

### Exploding maps and arrays
```python
df.select("id", F.explode("items").alias("item"))           # array → rows
df.select("id", F.explode("attrs").alias("k", "v"))         # map → rows
```

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| `TypeError: Column is not iterable` | used Python `and`/`or` on columns | use `&`, `|` |
| `AnalysisException: cannot resolve 'x'` | typo in column name; case mismatch | check `df.columns`; Spark is case-sensitive by default for column names from sources |
| Computed column name `(amount * 1.18)` | forgot `.alias(...)` | alias it |
| `withColumn` produces wrong result after chain | replaced a column you then referenced by its old meaning | use a new name; or rewrite as one `select` |
| Silent nulls in arithmetic | mixed types or null inputs | inspect with `df.filter(F.col("x").isNull()).count()` |

## References

- 📚 [LS Ch.3 §"Columns and Expressions" / Ch.4 §"DataFrame API Operations"]
- 📚 [HPS Ch.3 §"DataFrame API"]
- 📚 [DAS Ch.3 §"Transformations"]
- 📺 ["PySpark withColumn vs select"](https://www.youtube.com/results?search_query=pyspark+withcolumn+vs+select)
