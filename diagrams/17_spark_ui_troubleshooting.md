# 17 — Spark UI Troubleshooting Map

## Why this matters

The Spark UI is the only place Spark tells you the truth. Logs say *that* something failed; the UI
shows *why* — which stage, which task, how much shuffle, how much spill, which join strategy. This
page is a symptom-first map: start from what you observe, follow the tree to the tab and metric
that confirms it.

## The idea in plain English

Almost every "Spark problem" is one of five roots: **skew**, **spill/memory**, **too much
shuffle**, **too many small files/tasks**, or a **slow/blocked driver**. Each leaves a distinct
fingerprint in a specific UI tab. Diagnose by fingerprint, don't guess by feel.

## Diagram — symptom to root cause

```mermaid
flowchart TB
    START["Job slow or failing"] --> Q1{"Does one stage<br/>dominate the runtime?"}

    Q1 -->|"no — gaps between jobs"| DRV["Driver-side time:<br/>planning, listing files, collect,<br/>Python loops between jobs"]
    Q1 -->|"yes"| Q2{"In that stage: compare<br/>task duration median vs max"}

    Q2 -->|"max >> median"| SKEW["SKEW —<br/>one task gets most data<br/>see page 09"]
    Q2 -->|"all tasks slow"| Q3{"Spill (memory/disk)<br/>column non-zero?"}

    Q3 -->|"yes"| SPILL["Partitions too big for task memory:<br/>more partitions, more memory per task,<br/>or reduce data before the shuffle"]
    Q3 -->|"no"| Q4{"Shuffle read/write huge?"}

    Q4 -->|"yes"| SHUF["Too much data shuffled:<br/>filter/project earlier, broadcast<br/>the small join side"]
    Q4 -->|"no"| Q5{"Thousands of tiny tasks<br/>or thousands of input files?"}

    Q5 -->|"yes"| FILES["Small-files problem:<br/>compact input, coalesce output,<br/>Delta OPTIMIZE"]
    Q5 -->|"no"| CODE["Compute-bound:<br/>look for UDFs blocking codegen,<br/>regex/json parsing per row —<br/>check the SQL tab plan"]
```

## Which tab answers which question

| Question | Tab | Look at |
| --- | --- | --- |
| Which job/stage is slow? | Jobs → Stages | duration, task count per stage |
| Is it skew? | Stages → a stage → Summary metrics | min / median / max task duration and shuffle read |
| Is it spilling? | Stages → task table | "Spill (Memory)" / "Spill (Disk)" columns |
| What join strategy ran? Did AQE act? | SQL / DataFrame | physical plan: `BroadcastHashJoin` vs `SortMergeJoin`, `AQEShuffleRead` |
| Is cache working? | Storage | fraction cached, memory vs disk |
| Are executors dying / GC-bound? | Executors | dead executors, GC time vs task time, per-executor shuffle |
| What config actually applied? | Environment | `spark.sql.*` effective values |

## Diagram — failed job triage

```mermaid
flowchart TB
    FAIL["Job failed"] --> WHERE{"Where did the<br/>exception happen?"}
    WHERE -->|"before any job started<br/>(AnalysisException etc.)"| PLAN["Driver / planning problem:<br/>wrong column, path, schema —<br/>fix the query, no UI needed"]
    WHERE -->|"during a stage"| TASK["Open the failed stage →<br/>read the FIRST task error,<br/>not the last (cascades lie)"]
    TASK --> OOM{"ExecutorLostFailure / OOM?"}
    OOM -->|"yes"| MEM["Check spill + skew first —<br/>bigger executors is the fix of last resort"]
    OOM -->|"no"| ERR["Actual exception: bad records,<br/>nulls, UDF bug — reproduce on a sample"]
```

## Key takeaways

- **Always median vs max**, never averages — skew hides in averages.
- Read failures from the **first** error in the first failed task; later errors are usually
  cascade noise.
- The SQL tab is the most underused tab: it shows the real (post-AQE) plan with per-operator rows
  and timing — the fastest way to see a wrong join strategy or a missing filter pushdown.
- Local reflex to build: run an example from this repo, open <http://localhost:4040>, and find the
  stage boundary you predicted from the code. That habit is the whole
  [`14_spark_ui_lab/`](../14_spark_ui_lab/).

## Common mistakes

- Jumping straight to "increase executor memory" — it hides spill for a while and doubles cost;
  the root cause (skew, partition sizing, unfiltered shuffle) remains.
- Debugging from logs alone when the UI would show the answer in one screenshot.
- Forgetting the UI dies with the application — use the history server (or Databricks' persisted
  Spark UI) for post-mortems.
- Tuning configs found on Stack Overflow without confirming the fingerprint in the UI first.

## Interview angle

Troubleshooting scenarios are the core of senior interviews, and they all reduce to this page:
*"A job that took 20 minutes now takes 3 hours — walk me through your process."* Narrate the
decision tree: isolate the stage → task distribution → spill → shuffle volume → plan in the SQL
tab. Naming the exact tabs and metrics is what makes the answer credible.

## Related in this repo

- [`14_spark_ui_lab/01_jobs_stages_tasks.md`](../14_spark_ui_lab/01_jobs_stages_tasks.md)
- [`03_optimization/11-spark-ui-tour.md`](../03_optimization/11-spark-ui-tour.md) and
  [`12-debugging-failures.md`](../03_optimization/12-debugging-failures.md)
- [`13_debugging_playbook/`](../13_debugging_playbook/) — full playbooks per failure class
- [`07_interview_prep/04-troubleshooting-scenarios.md`](../07_interview_prep/04-troubleshooting-scenarios.md)
