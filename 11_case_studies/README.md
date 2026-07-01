# 11_case_studies - Performance And Incident Case Studies

This module teaches how to reason from symptoms to root cause.

## Case Studies

1. [`01_skewed_join_case_study.md`](./01_skewed_join_case_study.md)
2. [`02_small_files_case_study.md`](./02_small_files_case_study.md)
3. [`03_production_incident_rca.md`](./03_production_incident_rca.md)

## Runnable Labs

- [`labs/01_skewed_join_lab.py`](./labs/01_skewed_join_lab.py) - generate a hot-key join, inspect Spark UI, then compare with salting.
- [`labs/02_small_files_lab.py`](./labs/02_small_files_lab.py) - write many small files, then compare with a more intentional write layout.

## How To Use

Read the problem first. Before reading the fix, write down:

- What you would check in Spark UI.
- What logs you would inspect.
- What code change you suspect.
- What metric would prove the fix worked.

## Production Relevance

Most production Spark issues are not solved by "add more cluster." They are solved by understanding data shape, query plan, files, shuffle, and state.
