# 13_debugging_playbook - Production Spark Debugging

This is the field guide for failed and slow Spark jobs.

## How To Debug

1. Capture the symptom.
2. Find the failed or slow stage.
3. Compare task metrics.
4. Read the physical plan.
5. Inspect driver and executor logs.
6. Make one targeted change.
7. Prove the fix with metrics.

## Playbooks

1. [`01_failure_triage.md`](./01_failure_triage.md)
2. [`02_job_slow_or_oom.md`](./02_job_slow_or_oom.md)
3. [`03_shuffle_skew_and_spill.md`](./03_shuffle_skew_and_spill.md)
4. [`04_streaming_checkpoint_issues.md`](./04_streaming_checkpoint_issues.md)
5. [`05_databricks_failures.md`](./05_databricks_failures.md)

## Common Issues Covered

- Job failed.
- Job slow.
- OOM.
- Driver lost.
- Executor lost.
- Shuffle spill.
- Skewed join.
- Small files.
- Too many tasks.
- Too few tasks.
- Slow writes.
- Slow reads.
- Streaming checkpoint issue.
- Schema mismatch.
- Delta concurrent write issue.
- Databricks cluster startup issue.
- Library dependency issue.
- Permission issue.
- Unity Catalog access issue.
