# Lazy Evaluation

## Purpose

Show that transformations build a recipe and an action triggers execution.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal hand-drawn technical explainer on a white paper background.
Use rough black pen lines, simple boxes, a few arrows, and sparse red/blue handwritten notes.
Keep at least one third of the canvas blank.

Concept: Spark lazy evaluation. Draw a kitchen counter with recipe cards labeled read, filter,
select, withColumn, groupBy. A tiny serious black helper character stacks the cards without cooking
anything. On the far right, draw a big hand pressing a red button labeled action. After the button,
draw a simple stove turning on and a small cluster of executor pots starting to cook.

The image should clearly say: transformations only build the plan; action triggers the job.
```

## Layout description

Left-to-right recipe metaphor: transformation cards accumulate on the left, action button in the
middle-right, execution starts on the far right.

## Labels to include

- `transformations`
- `plan only`
- `action`
- `job starts`
- `no work yet` in blue
- `execution` in red

## Visual style

Notebook sketch, black pen, one red trigger button, blue note for lazy plan, simple character,
clean negative space.

## What to avoid

- Long code snippets
- Literal Spark UI screenshot
- Many transformation names beyond five cards
- Overly cute cartoon cooking scene

## Suggested caption

Spark transformations stack up like recipe cards; the cluster starts cooking only when an action
presses the button.
