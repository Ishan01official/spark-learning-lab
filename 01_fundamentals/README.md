# 01_fundamentals — How Spark actually works

## Why this matters

You can write working PySpark code by copying examples. You cannot debug, optimize, or interview without understanding what Spark is *doing under the hood*. This module is the mental model that everything else in the curriculum builds on.

After this module you can answer, without looking anything up:

- Why does Spark have a *driver* and *executors*, and what does each do?
- What's the difference between a **job**, a **stage**, and a **task**? How do they map to your code?
- Why does `df.filter(...)` not seem to do anything, but `df.show()` runs for 10 seconds?
- Why is `groupBy` slow but `select` is fast?
- When should you ever use an RDD instead of a DataFrame?

This is also the heaviest-weighted area on the **Databricks Associate Developer** exam (Domain 1).

## Notes (in order)

1. [`01-cluster-architecture-deep-dive.md`](./01-cluster-architecture-deep-dive.md) — Driver, executor, cluster manager. The whole picture.
2. [`02-job-stage-task.md`](./02-job-stage-task.md) — How your code becomes parallel work.
3. [`03-rdds-explained.md`](./03-rdds-explained.md) — The original abstraction. Why it still matters.
4. [`04-dataframes-vs-rdds.md`](./04-dataframes-vs-rdds.md) — Why DataFrames won, and when RDDs still pay off.
5. [`05-lazy-evaluation-and-dag.md`](./05-lazy-evaluation-and-dag.md) — The most counter-intuitive Spark feature, made obvious.
6. [`06-narrow-vs-wide-transformations.md`](./06-narrow-vs-wide-transformations.md) — The single most important performance distinction.
7. [`07-actions-vs-transformations.md`](./07-actions-vs-transformations.md) — What actually triggers computation.

## Architecture notebook

Open [`notebooks/01_spark_architecture_lab.ipynb`](./notebooks/01_spark_architecture_lab.ipynb)
for a 45–60 minute guided lab: predict → run → inspect Spark UI → explain aloud.
It includes an embedded architecture diagram, synthetic orders, input partitions,
narrow transformations, shuffle, task concurrency, caching, bounded collection,
and a revision quiz. No external data or credentials are needed.

Import the notebook into Databricks or open it in local Jupyter using the repository
environment. Classic compute exposes the fullest Spark UI and optional RDD process
probe; serverless/Connect can restrict introspection, caching, and settings. Local
mode illustrates parallel tasks on one machine, not a multi-machine deployment.

## Examples

- [`examples/01_rdd_basics.py`](./examples/01_rdd_basics.py)
- [`examples/02_dataframe_basics.py`](./examples/02_dataframe_basics.py)
- [`examples/03_lazy_eval_demo.py`](./examples/03_lazy_eval_demo.py)
- [`examples/04_narrow_wide_demo.py`](./examples/04_narrow_wide_demo.py)

## Diagrams

- [`diagrams/driver-executor.mmd`](./diagrams/driver-executor.mmd)
- [`diagrams/job-stage-task.mmd`](./diagrams/job-stage-task.mmd)
- [`diagrams/narrow-vs-wide.mmd`](./diagrams/narrow-vs-wide.mmd)
- [`diagrams/dag-lifecycle.mmd`](./diagrams/dag-lifecycle.mmd)

## References

- [LS Ch.2, Ch.3]
- [HPS Ch.2, Ch.3]
- [DAS Ch.1, Ch.2]
- [Cert Domain 1 — Apache Spark Architecture & Components]
- 📺 [Anatomy of a Spark Job — Conor Murphy](https://www.youtube.com/watch?v=rNpzrkB5KQQ)
- 📺 [Deep Dive into Spark's Catalyst Optimizer — Yin Huai](https://www.youtube.com/watch?v=GDeePbbCz2g)
