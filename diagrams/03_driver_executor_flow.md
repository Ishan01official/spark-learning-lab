# 03 — Driver–Executor Flow

## Why this matters

Knowing *what* the driver and executors are is static knowledge. Knowing *when* each one acts —
from `spark-submit` to the final result — is what lets you answer "where did my job spend its
time?" and "which process actually threw that error?".

## The idea in plain English

The driver never processes data; it plans and coordinates. An application starts, resources get
allocated, and then a loop repeats for every action in your code: build a job, split it into
stages and tasks, send tasks to executors, collect results. Executors heartbeat back the whole
time so the driver knows who is alive.

## Diagram — application lifecycle

```mermaid
sequenceDiagram
    participant U as You (spark-submit / notebook)
    participant D as Driver
    participant CM as Cluster manager
    participant E as Executors

    U->>D: start application (SparkSession)
    D->>CM: request executor containers
    CM->>E: launch executor JVMs
    E->>D: register (cores, memory)

    Note over D: your code runs, builds plans lazily

    U->>D: action called, e.g. df.write / count()
    D->>D: plan job, split into stages and tasks
    D->>E: send tasks (one per partition)
    E->>E: run tasks, shuffle between executors
    E->>D: task results and metrics
    D->>U: result / files written

    loop every few seconds
        E->>D: heartbeat
    end

    U->>D: spark.stop()
    D->>CM: release executors
```

## Diagram — who fails how

```mermaid
flowchart TB
    TF["Task fails"] -->|"retried on another executor<br/>up to 4 times"| OK1["Job usually survives"]
    EF["Executor dies"] -->|"tasks rescheduled,<br/>cached and shuffle data on it is lost"| OK2["Job survives, slower<br/>(recompute / refetch)"]
    DF["Driver dies"] --> DEAD["Application dies.<br/>No recovery within the app."]
```

## Key takeaways

- Nothing runs on executors until an **action** fires. Ten transformations = zero cluster work.
- One task per partition per stage. 200 partitions → 200 tasks → parallelism capped by total
  executor cores.
- Task failures are cheap (retried), executor failures are survivable (lineage recompute),
  driver failure is fatal — so protect the driver: no huge `collect()`, no giant broadcast
  variables, sensible `spark.driver.memory`.
- Heartbeats explain timeout-style errors: a GC-frozen executor misses heartbeats and gets marked
  dead even though it never "crashed".

## Common mistakes

- Reading executor logs for a planning error (e.g. `AnalysisException`) that happened on the
  driver — or driver logs for a task OOM that happened on an executor.
- Assuming a "lost executor" message means a bug. On spot/preemptible nodes it's routine; the
  question is how expensive the recomputation was.
- Calling `collect()` "to check the data" on a wide table. Use `show()`, `limit()`, or write a
  sample instead.

## Interview angle

Classic sequence question: *"What happens, step by step, when you run a Spark job?"* Answer in
lifecycle order: session → resource allocation → lazy plan → action → job → stages → tasks →
shuffle → result. Then the follow-up everyone gets: *"What happens if an executor is lost?"* —
tasks rescheduled, shuffle/cache data recomputed from lineage, job continues.

## Related in this repo

- [`01_fundamentals/02-job-stage-task.md`](../01_fundamentals/02-job-stage-task.md)
- [`01_fundamentals/07-actions-vs-transformations.md`](../01_fundamentals/07-actions-vs-transformations.md)
- [`01_fundamentals/diagrams/driver-executor.mmd`](../01_fundamentals/diagrams/driver-executor.mmd)
- [`13_debugging_playbook/01_failure_triage.md`](../13_debugging_playbook/01_failure_triage.md)
