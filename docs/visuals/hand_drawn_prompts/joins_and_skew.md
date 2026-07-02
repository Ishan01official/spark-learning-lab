# Joins and Skew

## Purpose

Show both the happy path of broadcast joins and the failure pattern of one hot skewed key.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal hand-drawn technical explainer on clean white paper. Use
rough black pen, simple tables, arrows, tiny worker characters, and sparse red/blue annotations.

Concept: Spark joins and skew. Split the image into two loose halves, not rigid panels. On the
left, draw a tiny lookup table being copied as small blue cards to three executors; label this
broadcast join. On the right, draw a large key pile labeled hot key falling into one overloaded
partition while other partitions have small neat piles. A small serious black helper character
holds a red warning sign near the overloaded partition.

The core idea: broadcast avoids big-table shuffle when one side is small; skew happens when one
key creates a straggler task.
```

## Layout description

Left side shows broadcast cards fanning out to executors. Right side shows uneven partition piles
with one large hot-key pile marked red.

## Labels to include

- `broadcast small table`
- `local join`
- `hot key`
- `straggler`
- `skew` in red
- `check Spark UI` in blue

## Visual style

Notebook sketch with black tables and piles, blue broadcast arrows, red skew warning, one small
operator character, strong negative space.

## What to avoid

- Showing every join type
- Crowding with many table columns
- Making the broadcast table look large
- Turning it into a polished slide

## Suggested caption

Fast joins move the small table to the workers. Slow skewed joins dump one hot key onto one unlucky
task.
