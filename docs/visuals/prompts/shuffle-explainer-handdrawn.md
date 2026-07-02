# Shuffle — Hand-Drawn

## Purpose

Show *why* the shuffle is expensive: every worker mails pieces to every other worker, and
everything stops while the mail moves. The mental image behind
[diagram 06](../../../diagrams/06_partitioning_and_shuffle.md) and half of all Spark tuning.

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal educational sketch illustration.

Visual DNA: clean white paper background, rough black hand-drawn pen lines with a slightly
wobbly hand-drawn feel, lots of empty white space, a few small handwritten labels, sparse red
and blue accents only. Friendly notebook-sketch style. No gradients, no 3D, no corporate
infographic style, no dense text.

Concept: the Spark shuffle — workers must re-sort data by key across the whole team, and it is
the expensive step.

Composition: a mail-sorting room. On the left, three stick-figure workers at desks, each with a
messy mixed pile of letters marked with tiny letters "a", "b", "c" (keys mixed on every desk).
In the middle, a web of crossing red arrows: every left desk sends bundles toward every right
bin — visibly tangled, this is the busy expensive zone. On the right, three neat mail bins,
each holding only one kind of letter: bin "a", bin "b", bin "c", with one relaxed worker per
bin ready to process. Under the tangled middle zone, a small red note: "disk + network = $$$".
A small blue annotation above the right side: "same key, same place".

Handwritten labels: "before: keys mixed" on the left, "shuffle" in red over the tangled arrows,
"after: grouped by key" on the right, red note "disk + network = $$$", blue note "same key,
same place".

Color rules: black for figures, desks, letters and bins; red only for the crossing shuffle
arrows and the cost note; blue only for the one annotation.

Constraints: one concept only, the tangled middle must contrast with calm left and right sides,
keep at least one third of the paper blank, maximum 6 short labels, no title text.
```

## Layout description

Three zones left → right: mixed desks (calm), crossing red arrow web (chaotic, the shuffle),
sorted bins (calm). The visual story is calm–chaos–calm; the chaos is the cost.

## Labels to include

- `before: keys mixed` — black
- `shuffle` — red, center
- `after: grouped by key` — black
- `disk + network = $$$` — red, small
- `same key, same place` — blue
- tiny `a b c` key marks on letters/bins — black

## Style rules

White paper, rough black pen, the arrow web is the only dense area on the page, red = the
expensive zone only, ≥1/3 blank, 16:9.

## What to avoid

- A clean symmetric arrow grid — the middle should feel like real tangled effort
- Labeling it "wide transformation" or other jargon; keep the room metaphor pure
- More than 3 keys (a, b, c is enough)
- Any server/network hardware imagery

## Caption for README/social post

> The shuffle, honestly drawn: every desk mails letters to every bin so that the same key ends up
> in the same place. That tangled middle is disk writes + network + disk reads — which is why the
> fastest Spark jobs are the ones that shuffle the least.
