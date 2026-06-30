# Slow Job Or OOM

## Symptoms

- Job runtime increased.
- Executor OOM.
- Driver OOM.
- High spill.
- One stage dominates runtime.
- Tasks have a long tail.

## Root Causes

- Skewed join or aggregation.
- Too few partitions.
- Too many tiny files.
- Large broadcast.
- `collect()` or `toPandas()` on large data.
- Caching wide data.
- Python UDF overhead.

## What To Check First

1. Spark UI stage duration.
2. Task duration distribution.
3. Shuffle read/write.
4. Spill memory/disk.
5. Input size.
6. Physical plan from `explain()`.

## Fix Options

- Broadcast the correct small table.
- Repartition by the right key.
- Salt hot keys.
- Select fewer columns before shuffle.
- Filter earlier.
- Avoid Python UDFs.
- Unpersist unused cached DataFrames.
- Tune executor sizing after code/data issues are understood.

## Prevention

- Add input row count and key distribution checks.
- Track runtime per stage.
- Keep a performance baseline for important jobs.

## Interview Explanation

"I do not start by adding memory. I first identify whether the bottleneck is scan, shuffle, skew, spill, or sink. Then I fix the specific cause and compare before/after metrics."
