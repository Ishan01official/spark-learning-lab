# 07 — Aggregations

## Why this matters

Aggregations are the workhorses of analytics: every dashboard, report, and feature engineering pipeline ends in a `groupBy().agg()`. They are also the most common cause of shuffles, OOMs, and skew problems.

## The three flavors

```python
# 1. Global aggregation — one row, no key
df.agg(F.sum("amount").alias("total"), F.count("*").alias("rows")).show()

# 2. Grouped aggregation — one row per group
(df.groupBy("country")
   .agg(F.sum("amount").alias("total"),
        F.countDistinct("customer_id").alias("buyers"))).show()

# 3. Rollup / cube / grouping sets — multi-level subtotals (see below)
```

## `groupBy` + `agg` — line by line

```python
result = (
    df                                              # base DataFrame
      .groupBy("country", "year")                   # composite key: (country, year)
      .agg(                                         # one or more aggregate expressions
          F.count("*").alias("n_orders"),           # rows in this group
          F.sum("amount").alias("revenue"),
          F.avg("amount").alias("aov"),             # average order value
          F.countDistinct("customer_id").alias("uniq_buyers"),
          F.approx_count_distinct("customer_id", rsd=0.02).alias("uniq_buyers_approx"),
          F.expr("percentile_approx(amount, 0.5)").alias("median_amount"),
      )
)
```

What Spark does under the hood:
1. **Partial aggregation** in each task on the read side (no shuffle yet).
2. **Shuffle** partial results, hash-partitioned by `(country, year)`.
3. **Final aggregation** in each shuffle partition merges partials.

This two-phase aggregation is why `sum`, `count`, `avg` (etc.) scale so well: the data moving across the network is per-group partials, not raw rows.

## Aggregations that *don't* parallelize cleanly

| Function | Why it's expensive |
| --- | --- |
| `countDistinct` | Spark must dedupe across all partials → effectively a second shuffle |
| `collect_list` / `collect_set` | per-group payload is unbounded — one fat group can OOM the executor |
| `percentile` (exact) | requires sorting all values per group |
| UDAF (custom) | usually loses partial-aggregation optimizations |

Replacements:
- `countDistinct` → `approx_count_distinct(col, rsd=0.05)` — HyperLogLog, ~5% error, orders of magnitude cheaper.
- `percentile` → `percentile_approx(col, p)` — t-digest based.
- `collect_list` over wide groups → keep only top N rows per group using a window + filter.

## Multi-level subtotals: `rollup`, `cube`, `grouping_sets`

```python
# ROLLUP: country, (country, year), grand total
df.rollup("country", "year").agg(F.sum("amount").alias("revenue")).show()

# CUBE: all combinations — (), (country), (year), (country, year)
df.cube("country", "year").agg(F.sum("amount").alias("revenue")).show()

# GROUPING SETS via SQL — exact levels you want
spark.sql("""
SELECT country, year, SUM(amount) AS revenue
FROM orders
GROUP BY GROUPING SETS ((country), (year), (country, year), ())
""").show()
```
`grouping_id()` tells you which level a row belongs to:
```python
df.cube("country", "year").agg(F.sum("amount"), F.grouping_id()).show()
```

## `pivot` — long to wide

```python
(df.groupBy("country")
   .pivot("year", [2023, 2024, 2025])     # explicit list = faster, deterministic columns
   .agg(F.sum("amount"))).show()
```
Without the explicit list, Spark runs an extra job just to discover distinct pivot keys.

## `agg` inside a window — running totals etc.

See `09-window-functions.md`. The key difference: `groupBy().agg()` *collapses* rows into one per group; window functions *keep* every row and add a column.

## Industry use cases

| Use case | Pattern |
| --- | --- |
| Daily KPI dashboard | `groupBy("date").agg(...)` written to a small summary table |
| User cohort metrics | `groupBy("signup_month").agg(F.countDistinct("user_id"))` |
| Top-K queries (top product per country) | window + `row_number()` filter (see note 09) |
| Funnel analysis | groupBy `step` + `count`, then SQL window to compute drop-off |
| Real-time dashboards on huge cardinality | `approx_count_distinct` materialized hourly |

## Scale notes

| Setup | Outcome |
| --- | --- |
| 1 TB data, 10k groups, simple sum | one shuffle, ~minutes on a small cluster |
| 1 TB data, 1B groups (e.g. by `user_id`) | shuffle write huge; consider `bucketBy` upstream or pre-aggregate |
| 1 TB data, 1 dominant key (`country='US'` is 80%) | **skew**: that one task gets 800 GB. Use AQE skew join handling or salting. |
| `collect_set` over 100M-row group | OOM. Cap with `slice(collect_list, 1, 1000)` or use top-N window pattern. |

## Failure modes and debugging

| Symptom | Cause | Fix |
| --- | --- | --- |
| One task runs 100× longer than others | skewed key | enable AQE skew, or salt the key |
| `OutOfMemoryError` during `collect_set` | unbounded group payload | switch to approx or top-N |
| Wrong totals when joining aggregated DF back | duplicates in original before groupBy | dedupe first; check with `count() vs countDistinct(key)` |
| `approx_count_distinct` gives 0 | column was null for all rows in group | `coalesce` to a placeholder or check upstream |
| `pivot` produces 5000 columns | pivot key cardinality way too high | pre-filter to top N keys |

### Detecting skew in the UI

In the Spark UI → Stages → click a stage → "Summary Metrics":
- If **Max** task duration ≫ **Median**, you have skew.
- If **Max** shuffle write ≫ **Median**, the skew is on input.
- If **Max** GC time is high, executor memory pressure (often from `collect_*`).

## References

- 📚 [LS Ch.4 §"Aggregations" / Ch.7 §"Optimizing and Tuning Spark Applications"]
- 📚 [HPS Ch.5 §"Aggregations" / Ch.6 §"Working with Key/Value Data"]
- 📚 [DAS Ch.2 §"Top-N" / Ch.3 §"Aggregations"]
- 📺 ["Apache Spark — skew handling" — Sim Simeonov, Daniel Tomes talks](https://www.youtube.com/results?search_query=apache+spark+skew+handling)
- 📺 ["approx_count_distinct internals"](https://www.youtube.com/results?search_query=apache+spark+hyperloglog+approx+count+distinct)
