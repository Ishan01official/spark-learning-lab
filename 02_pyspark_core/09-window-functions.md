# 09 — Window functions

## Why this matters

Window functions are the cleanest way to express "for each row, compute something using nearby rows" — ranking, running totals, lag/lead, top-N-per-group. Without them, you'd write self-joins, which are slow, awkward, and error-prone.

## The mental model

Every window function answers:
1. **Partition by** — which rows are in my "neighborhood"? (group of rows)
2. **Order by** — in what order are they?
3. **Frame** — which rows in the neighborhood do I look at? (rows N before to M after, or unbounded)

```python
from pyspark.sql import Window

w = (Window
       .partitionBy("country")          # neighborhoods
       .orderBy(F.col("amount").desc()) # order
       .rowsBetween(Window.unboundedPreceding, Window.currentRow))  # frame
```

## Three categories of window functions

### 1. Ranking — `row_number`, `rank`, `dense_rank`, `ntile`

```python
w = Window.partitionBy("country").orderBy(F.col("amount").desc())

df.withColumn("rn", F.row_number().over(w))     # 1, 2, 3, 4, ...  (unique)
df.withColumn("rk", F.rank().over(w))           # 1, 2, 2, 4, ...  (ties skip)
df.withColumn("dr", F.dense_rank().over(w))     # 1, 2, 2, 3, ...  (ties no skip)
df.withColumn("nt", F.ntile(4).over(w))         # quartile bucket 1..4
```

**Top-N per group** — the most common use case:
```python
top3_per_country = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") <= 3)
      .drop("rn")
)
```

### 2. Lag / lead — relative row access

```python
w = Window.partitionBy("user_id").orderBy("event_ts")

df.withColumn("prev_event_ts", F.lag("event_ts", 1).over(w))
df.withColumn("next_event_ts", F.lead("event_ts", 1).over(w))
df.withColumn("seconds_since_prev",
              F.unix_timestamp("event_ts") - F.unix_timestamp(F.lag("event_ts", 1).over(w)))
```

Industry pattern: **sessionization**. Split a user's events into sessions where a gap > 30 minutes starts a new session.
```python
gap = F.unix_timestamp("event_ts") - F.unix_timestamp(F.lag("event_ts").over(w))
is_new_session = F.when((gap.isNull()) | (gap > 30 * 60), 1).otherwise(0)

sessionized = (df
    .withColumn("is_new_session", is_new_session)
    .withColumn("session_id", F.sum("is_new_session").over(
        Window.partitionBy("user_id").orderBy("event_ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ))
)
```

### 3. Aggregate over a window — running totals, moving averages

```python
# Running total per customer
w_running = (Window
    .partitionBy("customer_id")
    .orderBy("order_ts")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df.withColumn("running_revenue", F.sum("amount").over(w_running))

# 7-day moving average (rows-based)
w_ma = (Window
    .partitionBy("customer_id")
    .orderBy("order_ts")
    .rowsBetween(-6, 0))                # 7 rows: 6 before + current

df.withColumn("ma7_amount", F.avg("amount").over(w_ma))

# 7-day moving avg by actual time, not row count (range-based)
days = lambda i: i * 86400              # seconds in a day
w_time = (Window
    .partitionBy("customer_id")
    .orderBy(F.col("order_ts").cast("long"))   # range requires numeric/timestamp
    .rangeBetween(-days(6), 0))

df.withColumn("ma7_days", F.avg("amount").over(w_time))
```

## Frame syntax — `rowsBetween` vs `rangeBetween`

| | `rowsBetween` | `rangeBetween` |
| --- | --- | --- |
| What it counts | physical rows | the *value* of the order column |
| Example | last 7 rows | last 7 days |
| Use when | row count matters | time matters (gaps, irregular intervals) |

Constants:
- `Window.unboundedPreceding` — start of partition
- `Window.unboundedFollowing` — end of partition
- `Window.currentRow` — this row
- Integer N — N rows or N units before/after

Default frame depends on whether `orderBy` is set:
- With order: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (running-total semantics).
- Without order: whole partition.

**Always specify the frame explicitly.** Defaults bite.

## Industry use cases

| Pattern | Window |
| --- | --- |
| Top product per category | `partitionBy(category) orderBy(sales desc)` + `row_number=1` |
| Customer's previous order date | `partitionBy(customer_id) orderBy(order_ts)` + `lag` |
| Running balance per account | `partitionBy(account_id) orderBy(ts) rowsBetween(unbounded, current) sum` |
| 7-day moving average for forecasting | `partitionBy(sku) orderBy(date) rangeBetween(-7 days, 0)` |
| Sessionization | `lag` + cumulative `sum` of session-start flag |
| Funnel step durations | `lag` over event sequence per user |
| First/last touch attribution | `first_value` / `last_value` with `ignoreNulls` |

## Performance characteristics

A window function with `partitionBy` *requires a shuffle* (Spark must co-locate rows of the same partition). With `orderBy`, it also sorts. So a single window function = roughly one shuffle + one sort, similar cost to a sort-merge join.

Reusing the *same window spec* across multiple columns is free — Catalyst computes the shuffled/sorted partition once:
```python
w = Window.partitionBy("country").orderBy(F.col("amount").desc())
df.select(
    "*",
    F.row_number().over(w).alias("rn"),
    F.rank().over(w).alias("rk"),
    F.sum("amount").over(w.rowsBetween(Window.unboundedPreceding, 0)).alias("running"),
)
```

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| Wrong running totals | wrong frame (default range vs rows) | specify `rowsBetween` explicitly |
| `WindowSpec: No partition defined for window operation` (warning) | window with `orderBy` but no `partitionBy` | OK if intentional (whole-DF window); else add `partitionBy` |
| OOM on a window | one partition is huge (skewed window key) | salt or aggregate first |
| Window without orderBy gives random rank | rank is meaningless without order | always order |
| `lag` returns null in the middle of partition | the prior row was null | use `lag(col, 1, default)` |

## A common confusion: window vs `groupBy`

| | `groupBy` | window |
| --- | --- | --- |
| Output rows | one per group | one per input row |
| Use to... | summarize | enrich rows with group context |
| Cost | one shuffle | one shuffle + sort (if ordered) |

## References

- 📚 [LS Ch.7 §"Window Functions"]
- 📚 [HPS Ch.5 §"Window Functions"]
- 📚 [DAS Ch.4 (multiple examples)]
- 📺 ["Spark window functions explained"](https://www.youtube.com/results?search_query=pyspark+window+functions+explained)
- 📺 [Mosaic/Databricks sessionization tutorial](https://www.youtube.com/results?search_query=spark+sessionization+window+functions)
