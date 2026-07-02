# Visual Diagram System

This folder is the Mermaid-first visual spine of `spark-learning-lab`. Each page explains one core
Spark idea in simple English, then gives you a GitHub-rendered diagram you can redraw from memory.

Use these pages before the matching module notes. They are intentionally focused: one concept, one
mental model, one interview angle.

## How to use this folder

1. Read the page before the module.
2. Run the related examples.
3. Open the Spark UI when a job runs.
4. Redraw the diagram from memory.
5. Answer the interview angle out loud.

## Index

| # | Page | Concept | Primary repo area |
| --- | --- | --- | --- |
| 01 | [Learning roadmap](./01_learning_roadmap.md) | Beginner to architect learning path | [`ROADMAP.md`](../ROADMAP.md) |
| 02 | [Spark architecture](./02_spark_architecture.md) | Driver, executors, cluster manager, storage | [`01_fundamentals/`](../01_fundamentals/) |
| 03 | [Driver executor flow](./03_driver_executor_flow.md) | From application start to completed tasks | [`01_fundamentals/`](../01_fundamentals/) |
| 04 | [Lazy evaluation and DAG](./04_lazy_evaluation_and_dag.md) | Transformations, actions, jobs, stages, tasks | [`01_fundamentals/`](../01_fundamentals/) |
| 05 | [Narrow vs wide transformations](./05_narrow_vs_wide_transformations.md) | Pipelined work vs shuffle boundaries | [`01_fundamentals/`](../01_fundamentals/) |
| 06 | [Partitioning and shuffle](./06_partitioning_and_shuffle.md) | Partitions, shuffle files, repartitioning | [`03_optimization/`](../03_optimization/) |
| 07 | [Join strategies](./07_join_strategies.md) | Broadcast, sort-merge, shuffle hash, nested loop | [`02_pyspark_core/`](../02_pyspark_core/) |
| 08 | [Skew and broadcast join](./08_skew_and_broadcast_join.md) | Hot keys, stragglers, salting, broadcast | [`03_optimization/`](../03_optimization/) |
| 09 | [Catalyst, Tungsten, AQE](./09_catalyst_tungsten_aqe.md) | Planning, codegen, runtime re-optimization | [`03_optimization/`](../03_optimization/) |
| 10 | [Delta Lake architecture](./10_delta_lake_architecture.md) | Parquet data files plus transaction log | [`04_delta_lake/`](../04_delta_lake/) |
| 11 | [MERGE, OPTIMIZE, Z-ORDER](./11_merge_optimize_zorder.md) | Upserts and table maintenance | [`04_delta_lake/`](../04_delta_lake/) |
| 12 | [Structured Streaming](./12_structured_streaming.md) | Source, micro-batch engine, state, sink | [`05_streaming/`](../05_streaming/) |
| 13 | [Watermark and checkpointing](./13_watermark_and_checkpointing.md) | Late data, state cleanup, recovery | [`05_streaming/`](../05_streaming/) |
| 14 | [Medallion architecture](./14_medallion_architecture.md) | Bronze, Silver, Gold lakehouse layers | [`06_real_projects/`](../06_real_projects/) |
| 15 | [End-to-end ETL](./15_end_to_end_etl.md) | A complete batch pipeline shape | [`06_real_projects/orders-etl/`](../06_real_projects/orders-etl/) |
| 16 | [Spark UI debugging](./16_spark_ui_debugging.md) | Symptom to UI tab to fix | [`14_spark_ui_lab/`](../14_spark_ui_lab/) |
| 17 | [Databricks production workflow](./17_databricks_production_workflow.md) | Notebook to governed scheduled job | [`15_databricks_production/`](../15_databricks_production/) |
| 18 | [Cloud lakehouse architecture](./18_cloud_lakehouse_architecture.md) | Cloud storage, compute, governance, serving | [`16_cloud_lakehouse/`](../16_cloud_lakehouse/) |
| 19 | [Certification roadmap](./19_certification_roadmap.md) | Exam prep plus interview prep | [`07_interview_prep/`](../07_interview_prep/) |

## Companion visual assets

- Hand-drawn prompt files: [`docs/visuals/hand_drawn_prompts/`](../docs/visuals/hand_drawn_prompts/)
- Static SVG explainers: [`assets/svg/`](../assets/svg/)
- Animated SVG/HTML explainers: [`assets/animations/`](../assets/animations/)

## Page template

Each numbered page contains:

1. Title
2. Why it matters
3. Plain English explanation
4. Mermaid diagram
5. Key takeaways
6. Common mistakes
7. Interview angle
8. Related repo folders/files

The master system diagram in [`end-to-end-spark-data-flow.mmd`](./end-to-end-spark-data-flow.mmd)
is explained in [`ARCHITECTURE.md`](../ARCHITECTURE.md). Read it after pages 02-06.
