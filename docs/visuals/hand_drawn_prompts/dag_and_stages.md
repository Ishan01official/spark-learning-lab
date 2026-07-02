# DAG and Stages

## Purpose

Explain how one action becomes a job, stages, and many tasks.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal hand-drawn technical explainer on white paper. Use rough
black pen lines, simple boxes, arrows, and sparse red/blue handwritten annotations. Keep the image
light and educational.

Concept: Spark DAG, job, stages, and tasks. Draw one big action lever on the left. The lever opens
a folded map labeled DAG. The map splits into two stage islands separated by a red river labeled
shuffle boundary. On each stage island, draw several tiny task cards sitting on partition tiles.
A small serious black helper character points at the red river with a warning flag.

The core idea: one action creates a job; shuffles cut the DAG into stages; each stage runs tasks
over partitions.
```

## Layout description

Action lever on left, DAG map in the center, two stage islands separated by a red shuffle boundary,
task cards on each island.

## Labels to include

- `action`
- `DAG`
- `job`
- `stage`
- `tasks`
- `shuffle boundary` in red
- `partitions` in blue

## Visual style

Hand-drawn map metaphor, black line art, red for shuffle boundary, blue for partition notes, simple
shapes, no dense text.

## What to avoid

- Real map geography
- More than two stage islands
- Explaining every Spark scheduler detail
- Perfect vector flowchart style

## Suggested caption

One action unfolds the DAG. Shuffle rivers cut it into stages, and each stage runs task cards over
partition tiles.
