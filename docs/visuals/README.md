# Visual Prompt Library

This folder contains prompt files for hand-drawn Spark learning illustrations. The Mermaid pages in
[`diagrams/`](../../diagrams/README.md) are precise. These prompts are for memorable notebook-style
visuals that can be generated, redrawn, or used as teaching references.

## Style DNA

Use the same visual language across every generated image:

- white or very light paper background
- rough black pen lines with slight wobble
- simple boxes, arrows, clouds, clusters, tables, and small characters
- sparse red and blue handwritten annotations
- playful but educational
- clean negative space
- one core concept per image
- no realistic UI screenshots
- no corporate infographic look

## Prompt index

| Prompt | Concept | Companion Mermaid page |
| --- | --- | --- |
| [spark_overview.md](./hand_drawn_prompts/spark_overview.md) | Driver, executors, storage, shuffle | [02](../../diagrams/02_spark_architecture.md) |
| [lazy_evaluation.md](./hand_drawn_prompts/lazy_evaluation.md) | Transformations wait until an action | [04](../../diagrams/04_lazy_evaluation_and_dag.md) |
| [dag_and_stages.md](./hand_drawn_prompts/dag_and_stages.md) | Job, stages, tasks, partitions | [04](../../diagrams/04_lazy_evaluation_and_dag.md) |
| [shuffle_and_partitioning.md](./hand_drawn_prompts/shuffle_and_partitioning.md) | Shuffle as repartitioning by key | [06](../../diagrams/06_partitioning_and_shuffle.md) |
| [joins_and_skew.md](./hand_drawn_prompts/joins_and_skew.md) | Join strategy plus hot-key skew | [07](../../diagrams/07_join_strategies.md) and [08](../../diagrams/08_skew_and_broadcast_join.md) |
| [delta_lake.md](./hand_drawn_prompts/delta_lake.md) | Delta log as table ledger | [10](../../diagrams/10_delta_lake_architecture.md) |
| [streaming.md](./hand_drawn_prompts/streaming.md) | Micro-batch streaming loop | [12](../../diagrams/12_structured_streaming.md) |
| [medallion_architecture.md](./hand_drawn_prompts/medallion_architecture.md) | Bronze to Silver to Gold | [14](../../diagrams/14_medallion_architecture.md) |
| [spark_ui_debugging.md](./hand_drawn_prompts/spark_ui_debugging.md) | Spark UI as debugging detective board | [16](../../diagrams/16_spark_ui_debugging.md) |
| [databricks_production.md](./hand_drawn_prompts/databricks_production.md) | Notebook to production job workflow | [17](../../diagrams/17_databricks_production_workflow.md) |

## How to use

1. Open a prompt file.
2. Copy the fenced prompt block into an image model.
3. Check the output against the labels, style, and avoid lists.
4. Regenerate if it becomes too dense, too corporate, too colorful, or too text-heavy.
5. Save approved images near the lesson that uses them.

The older [`prompts/`](./prompts/) folder is kept for compatibility with earlier drafts. New work
should use [`hand_drawn_prompts/`](./hand_drawn_prompts/).
