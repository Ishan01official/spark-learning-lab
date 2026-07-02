# Animated SVG Explainers

These files are self-contained HTML pages with inline SVG, CSS animations, and small
Play/Pause/Restart controls. They work by opening the file directly in a browser.

## Required animation set

| File | Concept | What it demonstrates |
| --- | --- | --- |
| [`lazy_evaluation_dag.html`](./lazy_evaluation_dag.html) | Lazy evaluation and DAG | Transformations appear, action triggers execution, DAG becomes job -> stages -> tasks |
| [`jobs_stages_tasks.html`](./jobs_stages_tasks.html) | Jobs, stages, tasks | One action creates a job; shuffle splits stages; partitions become tasks |
| [`shuffle_flow.html`](./shuffle_flow.html) | Shuffle | Records move across a costly network/disk boundary into new partitions |
| [`broadcast_join.html`](./broadcast_join.html) | Broadcast join | Small table is copied to executors while the large table stays partitioned |
| [`delta_lake_transaction_log.html`](./delta_lake_transaction_log.html) | Delta Lake transaction log | Commit JSON, active files, snapshot, ACID/time travel idea |
| [`streaming_micro_batches.html`](./streaming_micro_batches.html) | Structured Streaming | Source events form micro-batches, processing runs, checkpoint updates, sink receives output |
| [`medallion_flow.html`](./medallion_flow.html) | Medallion architecture | Raw data moves through Bronze, Silver, Gold with checks |
| [`spark_ui_debugging_flow.html`](./spark_ui_debugging_flow.html) | Spark UI debugging | Failed job -> stages tab -> skew -> shuffle metrics -> fix |

The earlier `*_animation.html` files are kept for backward compatibility. New docs should link the
required filenames above.
