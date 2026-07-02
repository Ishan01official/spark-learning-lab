# Shuffle and Partitioning

## Purpose

Make shuffle feel concrete: records are redistributed so matching keys land together.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal hand-drawn technical explainer on clean white paper. Use
rough black pen lines, simple boxes, arrows, and a few red/blue handwritten annotations. Avoid a
formal flowchart; make it look like a notebook sketch.

Concept: Spark partitioning and shuffle. Draw three messy input baskets on the left, each holding
record cards with letters A, B, C, D. In the center, draw a sorting table where a small serious
black helper character sorts cards by key. On the right, draw three neat output baskets: all A
cards together, B/C cards together, D cards together. Draw the center arrows in red/orange to show
network and disk cost.

The core idea: shuffle moves records across partitions by key, which is expensive but necessary
for joins, groupBy, distinct, and orderBy.
```

## Layout description

Input partitions left, sorting/shuffle table center, output partitions right. Red/orange arrows
cross through the center to show costly movement.

## Labels to include

- `input partitions`
- `shuffle`
- `by key`
- `output partitions`
- `network + disk` in red
- `parallel tasks` in blue

## Visual style

White background, rough black pen, simple basket/card metaphor, red/orange for shuffle movement,
blue for task note, clean blank space.

## What to avoid

- Too many keys
- Dense performance text
- Realistic server/network hardware
- Making shuffle look free or invisible

## Suggested caption

Shuffle is Spark's sorting table: records move so matching keys land together, and that movement
costs network and disk.
