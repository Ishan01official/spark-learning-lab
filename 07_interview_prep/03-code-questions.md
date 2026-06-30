# 03 — Common code-question patterns

The exam returns to the same shapes of question repeatedly. Recognizing the pattern lets you answer in 30 seconds instead of 2 minutes.

## Pattern 1: "What does this code return?"

You're given 5-10 lines of PySpark; pick the output among 4 choices.

### Example

```python
df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c"), (1, "d")], ["id", "v"])
df.groupBy("id").count().orderBy("id").show()
```

Choices:
- A) `(1,1), (2,1), (3,1)`
- B) `(1,2), (2,1), (3,1)`
- C) `(1,1), (1,1), (2,1), (3,1)`
- D) error

Answer: **B**. `groupBy("id").count()` returns one row per id with the count.

### Walk-through approach

1. **What's the input shape?** `(int, string)` × 4 rows.
2. **What does the operation do?** Group by id → 3 groups → counts.
3. **Sort matters?** Yes, `orderBy("id")` ascending.

## Pattern 2: "Which method is correct?"

Same operation, several method choices. Pick the one with the right syntax.

### Example

> You want to drop rows where any of columns `a`, `b`, `c` is null. Which is correct?

- A) `df.dropna(subset=["a", "b", "c"])`
- B) `df.na.drop(subset=["a", "b", "c"])`
- C) `df.filter("a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL")`
- D) all of the above

Answer: **D**. All three are equivalent.

These are knowledge questions — memorize that `dropna` / `na.drop` are aliases. The exam loves these.

## Pattern 3: "Which line causes the error?"

Given a snippet that fails, pick the offending line.

### Example

```python
1: df = spark.read.csv("data.csv", header=True)
2: df = df.withColumn("amount", F.col("amount").cast("decimal"))
3: result = df.groupBy("country").agg(F.sum("amount"))
4: result.write.parquet("output/", mode="overwrite")
```

Which fails?
- A) line 1
- B) line 2 (no precision/scale on decimal)
- C) line 3
- D) line 4

Answer: **B**. `cast("decimal")` actually works — but it defaults to `decimal(10, 0)`. Subtle. Some versions error. The "right" answer is usually the one that's plausibly broken.

Read these very carefully. The exam tests subtle errors:
- Wrong argument order in `withColumn` (column name, then column expression).
- Missing parentheses in `F.col`.
- Quoting issues in SQL strings.
- `groupBy` then `select` of non-aggregated columns.

## Pattern 4: "Which is more efficient?"

Two snippets do the same thing, differently. Pick the faster one.

### Example

```python
# A:
df.filter("country = 'US'").groupBy("city").count()

# B:
df.groupBy("city").count().filter("country = 'US'")
```

Which is faster?
- A) A
- B) B
- C) same
- D) depends

Answer: **A**, but Catalyst would push the filter down for B anyway. **C** ("same") is the safer pick if Catalyst is mentioned in the prompt.

Watch for:
- Filter pushdown: Catalyst does it; usually doesn't matter where you write it.
- Predicate pushdown to source: only for Parquet/ORC, depends on file format.
- Projection pruning: `select` early helps if you're reading lots of columns.
- Broadcast join: huge advantage if one side fits.

## Pattern 5: "Which transformation is narrow / wide?"

Identify whether an operation causes a shuffle.

| Narrow (no shuffle) | Wide (shuffle) |
|---|---|
| `filter` | `groupBy().agg` |
| `select` | `join` (unless broadcast) |
| `withColumn` (no agg) | `distinct` |
| `union` | `dropDuplicates` |
| `map` (RDD) | `repartition` |
| `flatMap` | `sortBy` / `orderBy` |
| `sample` | `aggregateByKey` |
| `coalesce` to fewer | (window: depends; partitionBy makes it wide) |

Trick examples:
- `coalesce` is narrow if reducing partitions.
- `repartition` is always wide.
- A `groupBy` followed by an action requires a wide stage.

## Pattern 6: "How many stages will this job have?"

Count the wide transformations. Each one creates a new stage boundary.

### Example

```python
df = spark.read.parquet("a/")
df2 = df.filter("c > 0")              # narrow
df3 = df2.join(other, "id")            # WIDE — stage boundary
df4 = df3.groupBy("k").count()         # WIDE — stage boundary
df4.write.parquet("out/")              # action
```

Stages: 2 wide ops → 3 stages (one for each "phase" of computation).

The DAG visualization in the Spark UI shows this clearly.

## Pattern 7: "What does explain() show?"

You're shown an `explain` output and asked to identify something — broadcast vs sort-merge join, AQE coalescence, filter pushdown.

Key things to recognize in plans:
- **`BroadcastHashJoin`** → broadcast join.
- **`SortMergeJoin`** → standard shuffle join.
- **`ShuffleHashJoin`** → rare, only when one side small enough + config tweak.
- **`AdaptiveSparkPlan`** → AQE active.
- **`AQEShuffleRead`** → AQE rewrote partition count.
- **`*Project`** → projection pruning happened.
- **`PushedFilters`** → predicate pushdown to source.
- **`coalesce`** vs **`Exchange hashpartitioning`** — coalesce is narrow, Exchange is shuffle.

## Pattern 8: "What's wrong with this UDF?"

UDFs come up a lot. Common gotchas:

```python
@F.udf
def double_it(x):
    return x * 2

df.withColumn("d", double_it("amount"))
```

Bug: no return type → defaults to StringType, will produce string "amount_value_amount_value" via Python `*` on a string. Fix: `@F.udf(IntegerType())`.

Other common UDF bugs:
- Missing `F.udf(...)` decorator entirely → "object is not callable".
- Calling the UDF with the column name as string vs `F.col(...)` (both work, actually).
- UDF returning a Python type Spark can't serialize.
- UDF that references a NoneType (no `None` check).

## Pattern 9: Window function questions

Watch for:
- Missing `orderBy` on a Window when using `row_number`/`rank` (returns indeterminate).
- Frame-based vs row-based windows: `rowsBetween` vs `rangeBetween`.
- Default frame for `sum().over(window_with_orderBy)` is `unboundedPreceding TO currentRow` — running total!

### Example

```python
w = Window.partitionBy("user_id").orderBy("event_time")
df.withColumn("running_total", F.sum("amount").over(w))
```

This is a running total, not a per-window sum. To get per-window sum (whole partition), use a window WITHOUT orderBy:

```python
w = Window.partitionBy("user_id")
df.withColumn("user_total", F.sum("amount").over(w))
```

## Pattern 10: Reading CSV / schema inference

```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

Common gotchas:
- Without `header=True`, you get `_c0, _c1, ...` column names.
- Without `inferSchema=True`, every column is StringType.
- `inferSchema=True` is two passes — slow on big files. Better: provide explicit schema.

## Practice approach

For each pattern above:
1. Cover the answer.
2. Walk through your reasoning out loud (or in your head).
3. Confirm with the answer.
4. If wrong, identify which step in your reasoning slipped.

Speed comes from pattern recognition, not from working everything out from scratch.
