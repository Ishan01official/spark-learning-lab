# Spark Overview — Hand-Drawn

## Purpose

The one image that makes Spark architecture click: a small "boss" figure planning work and several
identical "worker" figures each processing their own slice of data. Used at the top of the repo's
visual learning section and as the opening image for any "what is Spark" explanation.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal educational sketch illustration.

Visual DNA: clean white paper background, rough black hand-drawn pen lines with a slightly
wobbly hand-drawn feel, lots of empty white space, a few small handwritten labels, sparse red
and blue accents only. Friendly notebook-sketch style, like a good engineer explaining on paper.
No gradients, no shadows, no 3D, no corporate infographic style, no clip-art icons, no dense text.

Concept: how Apache Spark works — one driver plans, many executors work in parallel.

Composition: on the left, one small stick-figure character at a tiny desk labeled "driver",
holding a to-do list and looking at a plan pinned above the desk. From the desk, three blue
arrows fan out to the right toward three identical stick-figure workers labeled "executors",
each standing at its own workbench sawing through its own slice of one long log. The log slices
are labeled "partitions". Below the workers, a simple open box labeled "storage" that the workers
reach into. Between two workers, a small red double-headed arrow labeled "shuffle" where they pass
pieces to each other.

Handwritten labels, black ink unless noted: "driver (plans)", "executors (do the work)",
"partitions", "storage", and in red: "shuffle". At most one more tiny annotation in blue:
"tasks" on one of the arrows.

Color rules: black for all figures and objects, blue only for the task arrows and one annotation,
red only for the shuffle arrow and its label.

Constraints: one concept only, main scene fills about half the canvas, keep at least one third
of the paper blank, maximum 6 short labels, no title text, no paragraphs, charming but precise.
```

## Layout description

Left third: driver figure + desk + plan. Middle-right: three executor workbenches in a loose
vertical stack, each with its own log slice. Bottom: storage box shared by all workers. Arrows
flow left → right (plans to workers); one red horizontal arrow between workers (shuffle).

## Labels to include

- `driver (plans)` — black
- `executors (do the work)` — black
- `partitions` — black
- `storage` — black
- `tasks` — blue, on an arrow
- `shuffle` — red

## Style rules

White paper, rough black pen, wobbly lines, ≥1/3 blank space, blue = flow, red = the expensive
thing, ≤6 labels, 16:9.

## What to avoid

- Server racks, clouds, network icons, or anything datacenter-corporate
- More than one red element — the shuffle must be the only alarm color
- Text explaining the diagram; the scene itself must carry the meaning
- Making the driver bigger or grander than the executors — it's small, it only plans

## Caption for README/social post

> One plans, many saw. Spark in a single sketch: the driver never touches the log — it writes the
> to-do list. Executors each cut their own slice (partition), and the only expensive moment is
> when they have to pass pieces to each other: the shuffle.
