# Visual Diagram System

This folder is the visual spine of the repo. Every important Spark concept has one focused page:
a short explanation, a GitHub-rendered Mermaid diagram, key takeaways, common mistakes, and an
interview angle — in the same order you learn the topics in [`ROADMAP.md`](../ROADMAP.md).

Hand-drawn illustration prompts (for sketch-style images you can generate and embed) live in
[`docs/visuals/`](../docs/visuals/README.md).

## How to use this folder

1. Read the diagram page **before** the module notes. It gives you the mental model in 2 minutes.
2. Read the module notes and run the examples.
3. Come back to the diagram and check: can you redraw it from memory? If not, you skipped a step.

All diagrams render directly on GitHub — no tooling required. To preview locally, use the
[Mermaid Live Editor](https://mermaid.live) or any IDE Mermaid plugin.

## Index

| # | Page | Concept | Module |
| --- | --- | --- | --- |
| 01 | [Learning roadmap](./01_learning_roadmap.md) | The Level 0 → 10 journey through this repo | [`ROADMAP.md`](../ROADMAP.md) |
| 02 | [Spark architecture](./02_spark_architecture.md) | Cluster anatomy and the Spark component stack | [`01_fundamentals/`](../01_fundamentals/) |
| 03 | [Driver–executor flow](./03_driver_executor_flow.md) | Application lifecycle from `spark-submit` to results | [`01_fundamentals/`](../01_fundamentals/) |
| 04 | [Lazy evaluation & DAG](./04_lazy_evaluation_and_dag.md) | Transformations, actions, jobs, stages, tasks | [`01_fundamentals/`](../01_fundamentals/) |
| 05 | [Narrow vs wide transformations](./05_narrow_vs_wide_transformations.md) | Which operations shuffle and why it matters | [`01_fundamentals/`](../01_fundamentals/) |
| 06 | [Partitioning & shuffle](./06_partitioning_and_shuffle.md) | Shuffle mechanics, repartition vs coalesce | [`03_optimization/`](../03_optimization/) |
| 07 | [Catalyst, Tungsten, AQE](./07_catalyst_tungsten_aqe.md) | How Spark optimizes your query at plan time and runtime | [`03_optimization/`](../03_optimization/) |
| 08 | [Join strategies](./08_join_strategies.md) | How Spark picks a join and how to influence it | [`02_pyspark_core/`](../02_pyspark_core/) |
| 09 | [Skew & broadcast join](./09_skew_and_broadcast_join.md) | Diagnosing and fixing skewed joins | [`03_optimization/`](../03_optimization/) |
| 10 | [Delta Lake architecture](./10_delta_lake_architecture.md) | Parquet files + transaction log = ACID tables | [`04_delta_lake/`](../04_delta_lake/) |
| 11 | [MERGE, OPTIMIZE, Z-ORDER](./11_merge_optimize_zorder.md) | Delta write patterns and table maintenance | [`04_delta_lake/`](../04_delta_lake/) |
| 12 | [Streaming pipeline](./12_streaming_pipeline.md) | Structured Streaming from source to sink | [`05_streaming/`](../05_streaming/) |
| 13 | [Watermark & checkpoint flow](./13_watermark_checkpoint_flow.md) | Event time, late data, and recovery | [`05_streaming/`](../05_streaming/) |
| 14 | [Batch vs streaming](./14_batch_vs_streaming.md) | Choosing a processing mode | [`05_streaming/`](../05_streaming/) |
| 15 | [Medallion architecture](./15_medallion_architecture.md) | Bronze / Silver / Gold layer responsibilities | [`06_real_projects/`](../06_real_projects/) |
| 16 | [End-to-end ETL project](./16_end_to_end_etl_project.md) | The `orders-etl` project as a full system | [`06_real_projects/orders-etl/`](../06_real_projects/orders-etl/) |
| 17 | [Spark UI troubleshooting](./17_spark_ui_troubleshooting.md) | Symptom → UI tab → root cause | [`14_spark_ui_lab/`](../14_spark_ui_lab/) |
| 18 | [Databricks production workflow](./18_databricks_production_workflow.md) | From notebook to scheduled, governed jobs | [`15_databricks_production/`](../15_databricks_production/) |
| 19 | [Cloud lakehouse architecture](./19_cloud_lakehouse_architecture.md) | The full platform view across cloud services | [`16_cloud_lakehouse/`](../16_cloud_lakehouse/) |
| 20 | [Certification & interview map](./20_certification_and_interview_map.md) | Exam domains and interview levels mapped to repo folders | [`07_interview_prep/`](../07_interview_prep/) |

There is also one **everything-connected** master diagram:
[`end-to-end-spark-data-flow.mmd`](./end-to-end-spark-data-flow.mmd), explained line by line in
[`ARCHITECTURE.md`](../ARCHITECTURE.md). Read it *after* pages 02–06, not before — it makes far
more sense once you know the pieces.

## Diagram conventions

- **One concept per diagram.** Several small diagrams beat one giant one.
- **Flow direction is meaningful:** `LR` for data flow, `TB` for hierarchy and decisions.
- **Consistent vocabulary:** driver, executor, task, stage, job, partition, shuffle — always the
  official Spark terms, matching the module notes and the Spark UI.
- Per-module `.mmd` files (e.g. [`01_fundamentals/diagrams/`](../01_fundamentals/diagrams/)) stay
  close to the code; this folder holds the cross-cutting concept pages.

## Page template

Every numbered page follows the same structure, so you always know where to look:

1. **Why this matters** — the real-world problem this concept solves
2. **The idea in plain English** — no jargon explanation
3. **Diagram(s)** — Mermaid, renders on GitHub
4. **Key takeaways** — what to remember
5. **Common mistakes** — what beginners get wrong
6. **Interview angle** — how this is asked and how to answer
7. **Related in this repo** — module notes, examples, and exercises
