# Shuffle, Skew, And Spill

## Symptoms

- High shuffle read/write.
- Disk spill.
- Few slow tasks.
- Executor lost during join or aggregation.

## Root Causes

- Wide transformation.
- Hot key.
- Poor partition count.
- Large rows or nested payloads.
- Joining without filtering or pruning first.

## Spark UI Areas

- Stages tab: shuffle read/write columns.
- Tasks table: sort by duration and shuffle size.
- Executors tab: spill and failed tasks.
- SQL tab: join and exchange nodes.

## Fix Options

- Broadcast small side.
- Enable or tune AQE.
- Salt skewed keys.
- Pre-aggregate.
- Filter and select before join.
- Repartition intentionally.

## Prevention

- Profile key distribution.
- Add tests for hot default keys.
- Monitor shuffle metrics for critical jobs.

## Interview Explanation

"Shuffle is the cost of moving data between executors. Skew means that movement is uneven. Spill means memory was insufficient for part of the operation. The fix depends on whether the root cause is data distribution, query shape, or cluster sizing."
