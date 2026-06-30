# 03 — Reading query plans

## Why this matters

Performance work in Spark = reading plans. If you can't read what Catalyst produced, you can't tell whether your `filter` got pushed down, whether your join is broadcast or sort-merge, or whether a shuffle is sneaking into your pipeline. This note is the cheat sheet you'll come back to.

## The four modes of `explain()`

```python
df.explain()                # physical plan (default)
df.explain(True)            # all four: parsed, analyzed, optimized, physical
df.explain("formatted")     # vertical, indented, operator IDs — best for big plans
df.explain("cost")          # plan + statistics, requires CBO
```

Use `"formatted"` for anything non-trivial — the default flat format becomes unreadable past ~10 operators.

## Anatomy of a physical plan

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- SortMergeJoin [user_id#0L], [user_id#10L], Inner
   :- Sort [user_id#0L ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(user_id#0L, 200), ENSURE_REQUIREMENTS
   :     +- *(1) Filter (isnotnull(user_id#0L) AND (amount#1L > 100))
   :        +- *(1) ColumnarToRow
   :           +- FileScan parquet [user_id#0L,amount#1L] PushedFilters: [...]
   +- Sort [user_id#10L ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(user_id#10L, 200), ENSURE_REQUIREMENTS
         +- *(2) Filter isnotnull(user_id#10L)
            +- *(2) ColumnarToRow
               +- FileScan parquet [user_id#10L,name#11]
```

Read it **bottom-up** like a SQL execution tree:
1. Two `FileScan parquet` leaves — Parquet readers.
2. `ColumnarToRow` — vectorized → row format.
3. `Filter` — predicate evaluation.
4. `Exchange hashpartitioning(...)` — **shuffle** by `user_id`, 200 partitions.
5. `Sort` — local sort within each partition.
6. `SortMergeJoin` — merges the two sorted streams.
7. `AdaptiveSparkPlan` — the whole thing is AQE-wrapped.

## The signals you're looking for

| In the plan | What it tells you |
|---|---|
| `Exchange ...` | A shuffle. Each one is expensive. Count them. |
| `BroadcastExchange` | Small side will be broadcast — cheap join. |
| `BroadcastHashJoin` | Hash join with broadcast (no shuffle on probe side). |
| `SortMergeJoin` | Both sides shuffled and sorted. The default for large joins. |
| `ShuffledHashJoin` | Rare; both shuffled but no sort. AQE may insert this. |
| `BroadcastNestedLoopJoin` | Cartesian-ish. Bad unless tiny. |
| `PushedFilters: [...]` | Predicate pushdown succeeded. Empty = scanning everything. |
| `ReadSchema: struct<...>` | Projection pruning result. Should match what you `select`'d. |
| `*(N)` prefix | Whole-stage codegen stage N. Missing = something disabled codegen. |
| `Coalesce N` | Reduces partition count, no shuffle. |
| `Repartition N` | Reshuffles into N partitions. |
| `HashAggregate` then `Exchange` then `HashAggregate` | Two-phase aggregation: partial → shuffle → final. |
| `AdaptiveSparkPlan isFinalPlan=false` | AQE is on; plan will mutate at runtime. After execution, run `df.explain()` again to see the *final* plan. |

## Reading "formatted" mode

```
== Physical Plan ==
* HashAggregate (5)
+- Exchange (4)
   +- HashAggregate (3)
      +- Filter (2)
         +- Scan parquet (1)

(1) Scan parquet
Output [3]: [user_id#0L, country#1, amount#2L]
PushedFilters: [IsNotNull(amount), GreaterThan(amount,100)]
ReadSchema: struct<user_id:bigint,country:string,amount:bigint>

(2) Filter
Input [3]: [user_id#0L, country#1, amount#2L]
Condition: ((isnotnull(amount#2L) AND (amount#2L > 100)) AND isnotnull(country#1))
...
```
Each numbered operator gets its own block with input/output schema and details. Way easier to scan than the inline format.

## A 30-second triage checklist

Open the plan and ask:
1. **How many `Exchange` nodes?** Each is a shuffle. Most queries should have 0–2.
2. **Are filters pushed down?** Check `PushedFilters` on the scan — if your `where` clause isn't there, something blocked it (UDF, type mismatch).
3. **Is projection pruned?** `ReadSchema` should be only the columns you need.
4. **Join strategy correct?** Broadcast for small dim tables, SMJ for large-large.
5. **Any operator without `*(N)`?** Codegen broke — usually a Python UDF.
6. **AQE final plan?** Run `.collect()` first, then `df.explain()` — different plan number means AQE rewrote it.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Plan looks fine but query is slow | Reading `explain()` not the *executed* plan | Run the query, then `explain()`, then check Spark UI SQL tab |
| `PushedFilters: []` with an obvious filter | UDF / non-pushable expression after the scan | Move the filter earlier, replace UDF |
| Three `Exchange` nodes for a single join | Aggregation key ≠ join key ≠ output ordering | Restructure so keys line up, or accept the shuffles |
| AQE plan says `isFinalPlan=false` and you're stuck | You called `explain()` before execution | `.count()` or `.collect()`, then `explain()` again |

## References

- 📺 [How to Read Spark SQL Query Plans — Databricks](https://www.youtube.com/results?search_query=spark+sql+query+plans+databricks)
- [LS Ch.7 §"Spark UI"] — pairs the plan with the UI
- [HPS Ch.5 §"Reading Spark Plans"]
- [Cert Guide] — explain() output reading is a heavily tested topic
