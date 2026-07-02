# Static SVG Explainers

These files are lightweight, dependency-free SVG diagrams for Spark concepts. They are designed to
embed directly in Markdown or open in a browser.

## Assets

| File | Concept | Companion page |
| --- | --- | --- |
| [`spark_architecture.svg`](./spark_architecture.svg) | Driver, executors, storage, shuffle | [`diagrams/02`](../../diagrams/02_spark_architecture.md) |
| [`lazy_evaluation.svg`](./lazy_evaluation.svg) | Transformations wait for an action | [`diagrams/04`](../../diagrams/04_lazy_evaluation_and_dag.md) |
| [`dag_flow.svg`](./dag_flow.svg) | Action to job, stages, tasks | [`diagrams/04`](../../diagrams/04_lazy_evaluation_and_dag.md) |
| [`shuffle_flow.svg`](./shuffle_flow.svg) | Repartitioning by key | [`diagrams/06`](../../diagrams/06_partitioning_and_shuffle.md) |
| [`join_strategy.svg`](./join_strategy.svg) | Broadcast vs sort-merge choice | [`diagrams/07`](../../diagrams/07_join_strategies.md) |
| [`delta_lake_flow.svg`](./delta_lake_flow.svg) | Data files plus transaction log | [`diagrams/10`](../../diagrams/10_delta_lake_architecture.md) |
| [`streaming_pipeline.svg`](./streaming_pipeline.svg) | Source, micro-batch, state, sink | [`diagrams/12`](../../diagrams/12_structured_streaming.md) |
| [`medallion_architecture.svg`](./medallion_architecture.svg) | Bronze, Silver, Gold | [`diagrams/14`](../../diagrams/14_medallion_architecture.md) |
| [`spark_ui_debugging.svg`](./spark_ui_debugging.svg) | Symptom to UI tab to fix | [`diagrams/16`](../../diagrams/16_spark_ui_debugging.md) |
| [`databricks_production.svg`](./databricks_production.svg) | Notebook to production workflow | [`diagrams/17`](../../diagrams/17_databricks_production_workflow.md) |

## Style

- Pure inline SVG.
- No external fonts, scripts, images, or CSS files.
- Consistent colors: black text, blue normal flow, orange compute/data movement, red warnings.
- Labels are short enough to remain readable on GitHub.
