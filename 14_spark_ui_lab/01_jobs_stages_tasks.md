# Jobs, Stages, And Tasks In Spark UI

## Simple Explanation

- A job usually starts when an action runs.
- A stage is a group of tasks separated by shuffle boundaries.
- A task is work on one partition.

## Production Explanation

If a job is slow, do not inspect all code at once. Find the slow stage. Then inspect tasks in that stage. If task times are uneven, suspect skew. If all tasks are slow, suspect scan size, expensive operation, or cluster sizing.

## What To Inspect

- Job duration.
- Stage duration.
- Number of tasks.
- Shuffle read/write.
- Spill.
- Failed tasks.

## Common Mistakes

- Looking only at total job time.
- Ignoring task distribution.
- Changing cluster size before understanding the slow stage.

## Exercise

Run:

```bash
python 01_fundamentals/examples/04_narrow_wide_demo.py
```

Open Spark UI and write:

1. Which action triggered the job?
2. Which stage performed shuffle?
3. How many tasks ran?
4. What would change if partitions increased?
