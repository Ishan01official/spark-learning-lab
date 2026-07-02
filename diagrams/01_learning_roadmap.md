# 01 - Learning Roadmap

## Why it matters

Spark has too many topics to learn randomly. A roadmap prevents two common failures: memorizing API
syntax before understanding execution, and jumping into production tools before you can debug a
single slow job.

## Plain English explanation

Learn Spark in layers. First make the tools run. Then learn how Spark thinks: driver, executors,
partitions, jobs, stages, and shuffles. After that, build daily PySpark skill, then performance,
Delta Lake, streaming, production workflows, architecture, and interview practice.

## Mermaid diagram

```mermaid
flowchart TB
    L0["Level 0<br/>Setup, Python, SQL, Java, Git"] --> L1["Level 1<br/>Spark fundamentals"]
    L1 --> L2["Level 2<br/>Daily PySpark"]
    L2 --> L3["Level 3<br/>Production data engineering"]
    L3 --> L4["Level 4<br/>Optimization"]
    L4 --> L5["Level 5<br/>Delta Lake"]
    L5 --> L6["Level 6<br/>Structured Streaming"]
    L6 --> L7["Level 7<br/>Databricks production"]
    L7 --> L8["Level 8<br/>Cloud lakehouse architecture"]
    L8 --> L9["Level 9<br/>Case studies and debugging"]
    L9 --> L10["Level 10<br/>Certification and interviews"]

    L1 -.-> UI["Open Spark UI<br/>from the beginning"]
    L4 -.-> UI
    L9 -.-> UI
```

## Key takeaways

- Execution model comes before API memorization.
- Every level has a matching folder, runnable code, and failure mode.
- The Spark UI is not an advanced topic. Start using it in fundamentals.
- Interview prep is the last layer, but every page includes an interview angle.

## Common mistakes

- Starting with performance tuning before understanding shuffles.
- Treating Delta Lake and streaming as isolated tools instead of extensions of Spark execution.
- Reading notes without running examples.
- Skipping troubleshooting until a real incident.

## Interview angle

When asked how you learned Spark, describe a progression: architecture first, PySpark fluency
second, then optimization, Delta, streaming, and production debugging. This sounds more credible
than a list of disconnected libraries.

## Related repo folders/files

- [`ROADMAP.md`](../ROADMAP.md)
- [`LEARNING_STRATEGY.md`](../LEARNING_STRATEGY.md)
- [`PROJECT_INDEX.md`](../PROJECT_INDEX.md)
- [`00_setup/`](../00_setup/)
- [`20_learning_strategy/`](../20_learning_strategy/)
