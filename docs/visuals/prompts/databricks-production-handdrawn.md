# Databricks Production Workflow — Hand-Drawn

## Purpose

The journey from "works in my notebook" to "runs every night, governed and monitored" — as a
four-checkpoint path. The image version of
[diagram 18](../../../diagrams/18_databricks_production_workflow.md), and a gentle vaccination
against notebook-cowboy culture.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal educational sketch illustration.

Visual DNA: clean white paper background, rough black hand-drawn pen lines with a slightly
wobbly hand-drawn feel, lots of empty white space, a few small handwritten labels, sparse red
and blue accents only. Friendly notebook-sketch style. No gradients, no 3D, no corporate
infographic style, no dense text.

Concept: the path from a developer's notebook to a governed, scheduled production job.

Composition: a winding path drawn as a single black line from bottom-left to top-right, with
four small checkpoint stations along it. Station 1: a stick figure happily scribbling in a
spiral notebook, label "notebook". Station 2: the figure hands pages to a small booth with a
second figure holding a checklist and a stamp, label "review + tests"; a small red note beside
the booth: "no cowboy pushes". Station 3: the pages, now bound into a tidy packet, ride on a
tiny scheduled cart on rails with a clock face (no specific time), label "job on a schedule";
the cart's engine is small and disposable-looking with a tag "cluster: born per run, dies after".
Station 4: at the top, the packet arrives at a small gate with a guard figure holding a key
ring, label "governed tables", and beyond the gate a tiny happy analyst reads a clean report.
A thin blue arrow follows the whole path direction. One small blue note near station 3:
"alerts if it fails".

Handwritten labels: "notebook", "review + tests", "job on a schedule", "cluster: born per run,
dies after", "governed tables", red note "no cowboy pushes", blue note "alerts if it fails".

Color rules: black for everything, blue for the path-direction arrow and one note, red only for
the "no cowboy pushes" note.

Constraints: one concept only, the path must read clearly bottom-left to top-right, keep at
least one third of the paper blank, maximum 7 short labels, no title text, no vendor logos.
```

## Layout description

One continuous path, four stations at comfortable spacing: notebook → review booth → scheduled
cart on rails → governed gate + happy consumer. Rising diagonal composition suggests promotion
through environments.

## Labels to include

- `notebook` — black
- `review + tests` — black
- `job on a schedule` — black
- `cluster: born per run, dies after` — black, small tag
- `governed tables` — black
- `no cowboy pushes` — red
- `alerts if it fails` — blue

## Style rules

White paper, rough black pen, single continuous path (no branches), one red + one or two blue
elements, ≥1/3 blank, 16:9.

## What to avoid

- Databricks/git/cloud logos — generic booth, cart, and gate carry the meaning
- Drawing the four stations as boxes-and-arrows flowchart; it must stay a landscape path scene
- Corporate pipeline imagery (pipes, gears, robots)
- Text about CI/CD tools; the checklist stamp says it all

## Caption for README/social post

> Code doesn't graduate by being copied into a bigger notebook. It walks the path: notebook →
> review and tests → a scheduled job on a cluster that exists only while it runs → tables behind
> a governed gate. The whole trick of production Databricks is making this path the easy one.
