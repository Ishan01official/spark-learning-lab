# Spark Overview

## Purpose

Create one memorable opening image that explains Spark as a planner plus many parallel workers.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal hand-drawn technical explainer on clean white paper.
Use rough black pen lines, simple boxes, arrows, tiny worker characters, and generous blank space.
Add only a few small red and blue handwritten annotations. The image should feel like a notebook
sketch by an engineer, not a corporate infographic.

Concept: Apache Spark architecture. On the left, draw a small driver character at a desk holding a
plan. In the middle, draw three executor workbenches, each handling its own slice of data. At the
bottom, draw a simple storage box shared by the executors. Add one red arrow between executors to
show shuffle. Use a tiny solid black helper character with white dot eyes as the serious operator
moving task cards from the driver to executors.

Keep the visual simple. One core concept only: driver plans, executors do parallel work, storage
holds data, shuffle is expensive.
```

## Layout description

Driver on the left, executors across the middle-right, storage along the bottom, one red shuffle
arrow between executors, and blue task arrows from driver to executors.

## Labels to include

- `driver plans`
- `executors work`
- `partitions`
- `storage`
- `tasks`
- `shuffle` in red

## Visual style

White paper, rough black pen, sparse red and blue annotations, no shadows, lots of blank space,
slightly playful operator character, simple arrows and boxes.

## What to avoid

- Server rack realism
- Cloud vendor logos
- Dense architecture diagram
- More than six labels
- Corporate infographic style

## Suggested caption

Spark in one sketch: the driver plans, executors process partitions, and shuffle is the expensive
moment when data has to move.
