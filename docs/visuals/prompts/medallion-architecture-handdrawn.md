# Medallion Architecture — Hand-Drawn

## Purpose

Bronze/silver/gold as a three-station refinery: keep everything, clean it once, shape it for the
consumer. The image version of
[diagram 14](../../../diagrams/14_medallion_architecture.md) — ideal for READMEs and talks because
the metaphor survives without any caption.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal educational sketch illustration.

Visual DNA: clean white paper background, rough black hand-drawn pen lines with a slightly
wobbly hand-drawn feel, lots of empty white space, a few small handwritten labels, sparse red
and blue accents only. Friendly notebook-sketch style. No gradients, no 3D, no corporate
infographic style, no dense text.

Concept: the medallion architecture — three stations that make data progressively more
trustworthy: keep everything raw, clean it, shape it for the business.

Composition: three workstations left to right, connected by a blue flow arrow. Station 1
"bronze": a big open crate where a stick-figure worker dumps in everything arriving — bent
pieces, odd shapes, all kept, nothing thrown away; small note "keep it all". Station 2 "silver":
a worker at a cleaning bench with a brush and a simple gauge, polishing pieces into uniform
shapes; one cracked piece is set aside into a small side tray with a red tag "quarantine".
Station 3 "gold": a worker arranging a few polished pieces into a small neat display stand
shaped for a customer; a tiny stick-figure customer on the far right happily receives it.
Above the stations, a thin blue arrow spans the whole flow, and one dashed black arrow curves
backward from silver to bronze with the note "replay anytime".

Handwritten labels: "bronze - keep it all", "silver - clean + one truth", "gold - shaped for
the business", red tag "quarantine", black note on the dashed arrow "replay anytime".

Color rules: black for figures and objects, blue only for the forward flow arrow, red only for
the quarantine tag. Do NOT color the stations bronze, silver, or gold — line art only.

Constraints: one concept only, three stations of equal size, keep at least one third of the
paper blank, maximum 6 short labels, no title text.
```

## Layout description

Three equal stations in a row, forward blue arrow above, dashed replay arrow curving back
underneath from station 2 to station 1. The customer figure closes the story on the right.

## Labels to include

- `bronze - keep it all` — black
- `silver - clean + one truth` — black
- `gold - shaped for the business` — black
- `quarantine` — red, on the side tray
- `replay anytime` — black, on the dashed back-arrow

## Style rules

White paper, rough black pen, equal visual weight per station, exactly one red element, blue only
for the forward flow, ≥1/3 blank, 16:9.

## What to avoid

- Actual metallic colors or medal imagery — the names are labels, not a color scheme
- Throwing anything away at bronze (the crate keeps everything — that's the point)
- Databases/cylinders/cloud icons
- A fourth station or side pipelines; three stations, one story

## Caption for README/social post

> Bronze keeps everything (so you can always replay). Silver cleans it once (so nobody cleans it
> twice). Gold shapes it for the person asking. Three stations, three contracts — and the cracked
> piece goes to quarantine, never silently into the trash.
