# Hand-Drawn Visuals

The Mermaid pages in [`diagrams/`](../../diagrams/README.md) are precise. These are **memorable** —
notebook-sketch illustrations of the eight concepts that benefit most from a single friendly
image: the ones you want to recall in an interview, put in a talk, or share with a post.

This folder contains ready-to-use **image-generation prompts** (in [`prompts/`](./prompts/)), not
images. Paste a prompt into any capable image model (GPT-image, Midjourney, Gemini, etc. — or draw
it yourself on a tablet), review the result against the prompt's checklist, and save the final
image here.

## Style DNA (shared by every prompt)

Every prompt embeds the same visual language, so the set looks like one notebook:

- **Paper:** clean white or very light paper background, generous empty space (at least a third
  of the canvas stays blank)
- **Line:** rough black pen strokes, slightly wobbly, hand-drawn — not vector-perfect
- **Accents:** red for warnings/problems/hot spots, blue for annotations/flow — used sparingly,
  everything else stays black
- **Text:** 5–8 short handwritten labels maximum; the drawing explains, the labels only name
- **Tone:** friendly educational sketch, one core concept per image, landscape 16:9
- **Never:** corporate infographic style, gradients, 3D, screenshots, dense text blocks,
  clip-art icons, cute mascot posters

## Workflow

1. Open a prompt file and copy the fenced prompt block verbatim into your image model.
2. Compare the output against the file's **Labels to include** and **What to avoid** lists.
3. Regenerate or edit until it passes; typical fixes: "reduce the text", "more empty space",
   "make the lines rougher".
4. Save the approved image as `images/<same-name-as-prompt>.png` and embed it where the caption
   suggests (module README, main README, social post).

## Prompt index

| Prompt | Concept | Companion Mermaid page |
| --- | --- | --- |
| [spark-overview-handdrawn.md](./prompts/spark-overview-handdrawn.md) | Driver, executors, cluster in one scene | [02](../../diagrams/02_spark_architecture.md) |
| [dag-explainer-handdrawn.md](./prompts/dag-explainer-handdrawn.md) | Lazy evaluation and the DAG as a recipe | [04](../../diagrams/04_lazy_evaluation_and_dag.md) |
| [shuffle-explainer-handdrawn.md](./prompts/shuffle-explainer-handdrawn.md) | The shuffle as a mail-sorting room | [06](../../diagrams/06_partitioning_and_shuffle.md) |
| [delta-lake-handdrawn.md](./prompts/delta-lake-handdrawn.md) | Transaction log as the table's ledger | [10](../../diagrams/10_delta_lake_architecture.md) |
| [streaming-handdrawn.md](./prompts/streaming-handdrawn.md) | Micro-batches on a conveyor belt | [12](../../diagrams/12_streaming_pipeline.md) |
| [medallion-architecture-handdrawn.md](./prompts/medallion-architecture-handdrawn.md) | Bronze → silver → gold refinery | [15](../../diagrams/15_medallion_architecture.md) |
| [spark-ui-debugging-handdrawn.md](./prompts/spark-ui-debugging-handdrawn.md) | The straggler task / skew detective | [17](../../diagrams/17_spark_ui_troubleshooting.md) |
| [databricks-production-handdrawn.md](./prompts/databricks-production-handdrawn.md) | Notebook to production assembly line | [18](../../diagrams/18_databricks_production_workflow.md) |

## Prompt file template

Each prompt file has the same eight sections: Title, Purpose, Final image-generation prompt,
Layout description, Labels to include, Style rules, What to avoid, Caption. The fenced prompt is
self-contained — you never need to add context before pasting it.
