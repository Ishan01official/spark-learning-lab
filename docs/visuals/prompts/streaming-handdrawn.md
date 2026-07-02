# Structured Streaming — Hand-Drawn

## Purpose

De-mystify streaming: it's not a firehose, it's a conveyor belt delivering small trays of new data
to the same kitchen, with a bookmark saving progress. Pairs with
[diagram 12](../../../diagrams/12_streaming_pipeline.md) and
[diagram 13](../../../diagrams/13_watermark_checkpoint_flow.md).

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal educational sketch illustration.

Visual DNA: clean white paper background, rough black hand-drawn pen lines with a slightly
wobbly hand-drawn feel, lots of empty white space, a few small handwritten labels, sparse red
and blue accents only. Friendly notebook-sketch style. No gradients, no 3D, no corporate
infographic style, no dense text.

Concept: Structured Streaming = the same batch engine fed by a conveyor belt of micro-batches,
with a checkpoint notebook that remembers exactly where it stopped.

Composition: a conveyor belt runs from left to right. On the left end, an endless queue of small
trays arrives, each tray holding a few dots of data; the queue visibly continues off the page
(unbounded). In the middle, one stick-figure cook at a station processes exactly ONE tray at a
time, labeled "micro-batch". Next to the cook, a small pot on a side burner labeled "state"
where the cook keeps a running tally. After the station, finished trays slide into a cupboard
labeled "sink". Hanging from the cook's station, a small notebook on a string labeled
"checkpoint" with a tiny bookmark ribbon; a blue arrow from the notebook back to the belt with
a blue note "resume here after a crash". One tray far behind the others on the belt has a small
red tag "late data".

Handwritten labels: "events keep coming" at the left, "micro-batch" at the station, "state" on
the pot, "sink" on the cupboard, "checkpoint" on the notebook, blue note "resume here after a
crash", red tag "late data".

Color rules: black for everything, blue only for the resume arrow and note, red only for the
late-data tag.

Constraints: one concept only, the belt gives the picture its left-to-right motion, keep at
least one third of the paper blank, maximum 7 short labels, no title text.
```

## Layout description

Single horizontal scene: infinite tray queue (left, running off-page) → one cook + one tray +
state pot (center) → sink cupboard (right). The checkpoint notebook hangs at the center station;
its blue arrow points back at the belt position.

## Labels to include

- `events keep coming` — black
- `micro-batch` — black
- `state` — black, on the pot
- `sink` — black
- `checkpoint` — black, on the notebook
- `resume here after a crash` — blue
- `late data` — red, on one straggler tray

## Style rules

White paper, rough black pen, motion left → right, exactly one red element and one blue element,
≥1/3 blank, 16:9.

## What to avoid

- Lightning bolts, speed lines, or "real-time!!" energy — the point is calm, repeated small work
- Showing many cooks; ONE station processing ONE tray is the model
- Kafka logos or brand imagery
- Any clock faces with specific times (the watermark deserves its own image, don't cram it in)

## Caption for README/social post

> Streaming in Spark isn't drinking from the firehose — it's a conveyor belt of small trays, each
> processed by the same engine that runs your batch jobs. A checkpoint notebook remembers the
> exact tray it stopped at, so a crash is just "resume here". The only drama: the occasional tray
> that arrives late.
