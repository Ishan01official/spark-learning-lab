# Case Study - Skewed Join

## Symptoms

- Job runs fast for most tasks.
- One or a few tasks run much longer.
- High shuffle read in the slow stage.
- Executors may spill or fail with OOM.

## Root Cause

One key owns a large percentage of the rows. Spark sends rows with the same key to the same partition for the join, so one task gets much more work than the others.

## What To Check First

1. Spark UI Stages tab: task duration distribution.
2. Shuffle read size per task.
3. Top key counts in the join column.
4. Physical plan join type.

## Fix Options

- Broadcast the smaller side if it is safely small.
- Enable AQE skew join handling.
- Salt the hot key.
- Pre-aggregate before join.
- Fix upstream bad default keys when possible.

## Interview Explanation

"I would prove skew by comparing task durations and shuffle sizes in Spark UI. If a few tasks dominate, I would inspect key distribution. Then I would choose broadcast, AQE skew handling, salting, or upstream cleanup depending on table size and correctness constraints."
