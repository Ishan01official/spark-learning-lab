# Module 03 — Optimization exercises

Each exercise is meant to be done with the Spark UI open. The point is to learn to *read* what's happening, not to memorize APIs.

---

## 1. Identify the broadcast cutoff

In a fresh Spark session, join a 5M-row DataFrame against a parameterized N-row DataFrame. Sweep N through `[100, 10_000, 1_000_000, 10_000_000]`. For each N:

- Print `df.explain()` and identify whether the plan is `BroadcastHashJoin` or `SortMergeJoin`.
- Note the boundary where Spark switched from BHJ → SMJ.
- Force the other strategy with the `broadcast()` hint or `autoBroadcastJoinThreshold=-1` and time both.

**Expected**: a table of N → strategy chosen → wall-clock time. Discuss when forcing the broadcast at large N hurts.

---

## 2. Predict the number of shuffles

For each of the following queries, predict the number of `Exchange` operators **before** running `.explain()`. Then check.

```python
# (a) groupBy + agg
df.groupBy("user_id").sum("amount")

# (b) two consecutive groupBys on different keys
df.groupBy("user_id").sum("amount").groupBy("country").avg("sum(amount)")

# (c) groupBy then join on the group key
df.groupBy("user_id").sum("amount").join(users, "user_id")

# (d) repartition then groupBy on the same key
df.repartition("user_id").groupBy("user_id").sum("amount")
```

For (d), explain why Spark can eliminate one of the shuffles even though you wrote two distinct operations.

---

## 3. Fix the skewed job

Generate 10M rows where `user_id` is one of `[1, 2, 3, 4, 5]` but 90% of rows have `user_id=1`. Join against a `users` DataFrame.

1. Run naively. Record the Max/Median task duration ratio.
2. Enable AQE skew handling. Re-run. Did the ratio improve? By how much?
3. Disable AQE, salt the hot key (N=20), re-run. Compare against (2).

**Expected**: three sets of metrics + a one-paragraph "when would I salt manually instead of relying on AQE" answer.

---

## 4. UI post-mortem

Pick any slow stage from any job you've run. Take screenshots of:

- The Stages tab task summary (showing Min/Median/Max).
- The SQL tab DAG (the operator-with-times view).
- The Executors tab.

Annotate them. What's the bottleneck — CPU, memory, network, skew, planning? What change would you try first?

---

## 5. Make a stage do *fewer* shuffles

You're given a pipeline:

```python
result = (orders
    .join(users, "user_id")
    .filter(F.col("country") == "US")
    .groupBy("user_id", "product_id")
    .agg(F.sum("amount").alias("total"))
    .join(products, "product_id")
    .orderBy(F.desc("total")))
```

Rewrite this pipeline to minimize shuffles and read volume:

- Push the `country == "US"` filter earlier.
- Project only the columns you need from each table.
- Broadcast small dims.
- Question whether the final `orderBy` is necessary (vs `limit`).

Count `Exchange` nodes in both plans. Aim for a 2× reduction.

---

## 6. Cache or not?

You have:

```python
features = expensive_transform(raw)  # ~30 s of work
predictions = features.filter(F.col("score") > 0.5)
predictions.write.parquet("preds/")
metrics = features.groupBy("label").agg(F.count("*"), F.avg("score"))
metrics.show()
```

- Time it as-is, then time it with `features.cache()` + `features.count()` after the transform. Why does adding `count()` matter?
- Run with `MEMORY_AND_DISK_SER` instead of `MEMORY_AND_DISK`. Any difference for a DataFrame?
- Now imagine `features` is 200 GB and your cluster is 100 GB of executor memory. What's the right answer here — cache, partial cache, checkpoint, write+read? Justify.

---

## 7. Debugging a real failure

You run a job and see:

```
org.apache.spark.shuffle.FetchFailedException:
Failed to connect to executor-7.cluster.local:43210
```

Followed by:

```
ExecutorLostFailure (executor 7 exited).
Reason: Container killed by YARN for exceeding memory limits.
22.0 GB of 20 GB physical memory used.
```

Walk through your debugging:
1. What's the root cause vs the symptom?
2. Which Spark configs do you change, and what to?
3. What do you investigate first to confirm the fix worked?

Write up your answer as a runbook entry.
