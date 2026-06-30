# 14_spark_ui_lab - Learn Spark UI By Reading Real Symptoms

Spark UI is the fastest way to understand what Spark actually did.

## Why This Matters

You cannot tune Spark from code alone. The UI shows jobs, stages, tasks, SQL plans, storage, environment, executors, shuffle, spill, skew, and failed tasks.

## Learning Objectives

- Navigate every major Spark UI tab.
- Connect code to jobs, stages, and tasks.
- Detect skew from task metrics.
- Detect spill and memory pressure.
- Compare fast and slow runs.
- Explain UI findings in interviews.

## Spark UI Tabs

| Tab | What It Tells You | Questions To Ask |
| --- | --- | --- |
| Jobs | Actions and job status | Which action triggered work? |
| Stages | Shuffle boundaries and task groups | Which stage is slow? |
| Tasks | Per-partition work | Are tasks balanced? |
| SQL | Physical plans and query metrics | What join strategy was used? |
| Storage | Cached DataFrames/RDDs | Is cache helping or wasting memory? |
| Environment | Spark configs and runtime | Are configs what I expected? |
| Executors | Memory, cores, failures, spill | Are executors healthy? |

## Labs

1. Run `01_fundamentals/examples/03_lazy_eval_demo.py` and identify the action.
2. Run `01_fundamentals/examples/04_narrow_wide_demo.py` and find the shuffle stage.
3. Run `03_optimization/examples/06_explain_plans.py` and compare the plan with the SQL tab.
4. Run a cache example and inspect the Storage tab.

## Screenshot Placeholder Policy

Screenshots are useful but environment-specific. Add screenshots under `14_spark_ui_lab/screenshots/` only if they are small, readable, and tied to a lab.

## Interview Answer Pattern

"I would open Spark UI, find the slow stage, compare task durations and shuffle size, inspect spill and executor health, then map the finding back to the physical plan."
