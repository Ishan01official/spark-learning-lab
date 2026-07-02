# Spark UI Debugging (Skew) — Hand-Drawn

## Purpose

The most relatable Spark pain, drawn: 199 tasks done, 1 straggler carrying a boulder, everyone
waiting. Teaches skew, stragglers, and "median vs max" in one glance. Pairs with
[diagram 09](../../../diagrams/09_skew_and_broadcast_join.md) and
[diagram 17](../../../diagrams/17_spark_ui_troubleshooting.md).

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal educational sketch illustration.

Visual DNA: clean white paper background, rough black hand-drawn pen lines with a slightly
wobbly hand-drawn feel, lots of empty white space, a few small handwritten labels, sparse red
and blue accents only. Friendly notebook-sketch style. No gradients, no 3D, no corporate
infographic style, no dense text.

Concept: data skew — the whole stage waits for one overloaded task.

Composition: a finish line scene. On the right, a tape finish line with a small crowd of many
tiny identical stick-figure runners already finished, relaxed, some sitting, one checking a
wristwatch, with the note "199 tasks: done in 10s". On the left, still far from the line, ONE
exhausted stick-figure runner dragging an enormous boulder tied by a rope; the boulder is
covered in many tiny repeated marks of the same key: "id=42" written a few times; red label
on the boulder "hot key". Next to the struggling runner, a small blue magnifying glass
annotation pointing at the boulder with blue note "check max vs median". Under the whole scene
a thin ground line only. The finished crowd is small and light; the struggling runner and
boulder are the visual center.

Handwritten labels: "199 tasks: done in 10s" near the crowd, "1 task: still running..." near
the straggler, "hot key" in red on the boulder, "id=42" tiny marks in black on the boulder,
"check max vs median" in blue at the magnifying glass.

Color rules: black everywhere, red only for "hot key", blue only for the magnifying glass note.

Constraints: one concept only, strong size contrast between the tiny finished crowd and the
huge boulder, keep at least one third of the paper blank, maximum 6 short labels, no title text.
```

## Layout description

Right: finish line + relaxed finished crowd (small, light strokes). Left-center: the straggler
and giant boulder (the hero of the image). Blue magnifying-glass note beside the boulder makes
the diagnostic lesson explicit.

## Labels to include

- `199 tasks: done in 10s` — black
- `1 task: still running...` — black
- `hot key` — red, on the boulder
- `id=42` — tiny, repeated 2–3 times on the boulder
- `check max vs median` — blue

## Style rules

White paper, rough black pen, exaggerated boulder-vs-runners scale contrast, one red + one blue
element, ≥1/3 blank, 16:9.

## What to avoid

- Making the finished runners detailed — they're background; the straggler is the story
- Charts, bars, or actual UI screenshots
- Blaming imagery (no angry boss figure) — the tone is sympathy, everyone's been this task
- More than one boulder; skew = ONE overloaded partition

## Caption for README/social post

> Every Spark engineer knows this runner. 199 tasks finish in seconds; one drags the boulder —
> a hot key that hashed all its data into a single partition. Averages hide it, so open the
> Stages tab and compare max vs median. Then: broadcast, let AQE split it, or salt the key.
