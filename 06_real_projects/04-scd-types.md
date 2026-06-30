# 04 — SCD types — when to use which

## Why this matters

A "slowly changing dimension" is any dim that mutates: customer addresses, product prices, employee titles. Six well-defined "SCD types" describe how to handle the change. Picking the wrong one is one of the most common warehouse-modeling mistakes.

## The six types

| Type | What it does | Use when |
|---|---|---|
| Type 0 | Never updates; first value is the only value | "Originally assigned" attributes |
| Type 1 | Overwrite the existing row | History doesn't matter |
| Type 2 | Insert a new row, mark old as expired | You need full history (most common) |
| Type 3 | Add a "previous_X" column | You only ever need one prior value |
| Type 4 | Splits into current + history tables | Type 2 but with hot-path separation |
| Type 6 | Combines 1, 2, 3 | "Type 2 but tell me what the current value is" |

In practice you'll use Type 1 and Type 2 ~95% of the time. Type 6 is a thoughtful refinement of Type 2.

[*Kimball Data Warehouse Toolkit* Ch.5]

## Type 1 — overwrite

```python
target.alias("t").merge(updates.alias("s"), "t.id = s.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

Simple. Wipes prior values. Use when the dim attribute is a current state with no historical meaning — e.g. "this customer's preferred email", where "what was their email last year" is irrelevant.

## Type 2 — insert new + close old

The classic. Adds three columns: `valid_from`, `valid_to`, `is_current`.

```
customer_id | name  | country | valid_from | valid_to   | is_current
1           | alice | US      | 2024-01-01 | 2024-06-01 | false
1           | alice | UK      | 2024-06-01 | NULL       | true
```

Every change adds a row. Queries:

- "Who's our current alice?" → `WHERE customer_id = 1 AND is_current = true`
- "What was alice on 2024-03-15?" → `WHERE customer_id = 1 AND valid_from <= '2024-03-15' AND (valid_to IS NULL OR valid_to > '2024-03-15')`

### Implementing Type 2 with Delta MERGE

The trick: a single MERGE that both closes the old row and inserts the new. Pre-stage two records per change:

```python
# For each change:
#   close_row: merge_key = customer_id (matches existing current row -> close it)
#   new_row:   merge_key = NULL         (no match -> insert)

changes_only = updates.join(
    target.toDF().alias("t").where("is_current = true"),
    on=[F.col("s.customer_id") == F.col("t.customer_id")],
    how="left"
).where("t.customer_id IS NULL OR s.country != t.country OR s.name != t.name")

close_rows = changes_only \
    .where("t.customer_id IS NOT NULL") \
    .withColumn("merge_key", F.col("s.customer_id"))

new_rows = changes_only \
    .withColumn("merge_key", F.lit(None).cast("long"))

staged = close_rows.unionByName(new_rows)

target.alias("t").merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true") \
    .whenMatchedUpdate(set={
        "valid_to":   "s.event_time",
        "is_current": "false"
    }) \
    .whenNotMatchedInsert(values={
        "customer_id": "s.customer_id",
        "name":        "s.name",
        "country":     "s.country",
        "valid_from":  "s.event_time",
        "valid_to":    "null",
        "is_current":  "true"
    }) \
    .execute()
```

A complete working version is in `examples/04_scd2_merge.py` in module 04.

### Type 2 with hash-detection

If schema changes are coming and you don't want to maintain a column-list comparison, hash the attributes:

```python
target_with_hash = target.toDF().withColumn(
    "_hash",
    F.sha2(F.concat_ws("|",
                       F.col("country"),
                       F.col("name"),
                       F.col("email")), 256)
)
# Change = hash differs from current target hash
```

Robust to attribute additions. But fragile if you ever change the hash spec.

### Common Type 2 mistakes

- Forgetting `is_current` — possible to derive from `valid_to IS NULL` but adding it speeds queries.
- Using `valid_to = '9999-12-31'` instead of NULL — preference; NULL is clearer for "open-ended".
- Including non-historic attrs in change detection (e.g. `last_login_at`) — you'll get a new row every day. Detect changes only on the *Type 2 attributes*.
- Not handling deletes — needs a separate "is_deleted" or "closed" status.

## Type 3 — previous-value column

```
customer_id | name  | current_country | previous_country | changed_at
1           | alice | UK              | US               | 2024-06-01
```

Compact, but only one history step. Useful for "show me what changed in the last refresh" without committing to full Type 2.

## Type 4 — current + history table split

```
dim_customers              (small, hot)
dim_customers_history      (large, cold)
```

Same content as Type 2 but split. Use when:
- The "current" lookup is so frequent it must be fast.
- The historical table is large enough that joining it in slows queries.

Type 4 = Type 2 with manual physical partitioning. With Delta + partitioning by `is_current`, you get the same effect at no design cost.

## Type 6 — combined

Type 2 row history + a Type 1 "current" column denormalized into every row.

```
customer_id | name  | country | current_country | valid_from | valid_to   | is_current
1           | alice | US      | UK              | 2024-01-01 | 2024-06-01 | false
1           | alice | UK      | UK              | 2024-06-01 | NULL       | true
```

Now you can answer:
- "What was alice's country on date X" (Type 2: country column).
- "What is alice's country now, regardless of when this row was valid?" (Type 1: current_country column).

The current_country column is updated on every history row when alice's current state changes. Useful for joins where you don't want to filter on `is_current` first.

## Choosing

```mermaid
flowchart TD
    A[Does anyone care about past values?] -->|No| T1[Type 1: overwrite]
    A -->|Yes| B{Need full history<br/>or just one prior?}
    B -->|Just one prior| T3[Type 3]
    B -->|Full history| C{Need to join often<br/>on "current" state?}
    C -->|No| T2[Type 2]
    C -->|Yes, for joins to current| T6[Type 6]
```

## Performance notes

Type 2 tables grow ~ N rows per entity. Most queries filter on `is_current = true` — partition or Z-ORDER by it to make those queries fast. Point-in-time queries on `valid_from`/`valid_to` benefit from Z-ORDER on `customer_id` and a partition on `is_current`.

For SCD2 of fact-like data (e.g. order status history), the row count can balloon. Consider Type 4 (split current + history) or just keeping a flat event log.

## References

- *The Data Warehouse Toolkit* by Kimball & Ross — Ch.5 on SCD types (the canonical text)
- *Delta Lake: The Definitive Guide* — Ch.8 example "SCD2 with MERGE"
- 📺 [SCD Type 2 in Delta Lake — Databricks](https://www.youtube.com/results?search_query=scd+type+2+delta+lake+databricks)
- [DAS Ch.9 §"SCD Implementation"]
