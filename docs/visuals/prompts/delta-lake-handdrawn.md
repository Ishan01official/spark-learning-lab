# Delta Lake — Hand-Drawn

## Purpose

One image for the one idea behind Delta Lake: the table is just files **plus a ledger**, and the
ledger is the truth. Pairs with [diagram 10](../../../diagrams/10_delta_lake_architecture.md);
also works standalone for "how do you get ACID on a data lake?".

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal educational sketch illustration.

Visual DNA: clean white paper background, rough black hand-drawn pen lines with a slightly
wobbly hand-drawn feel, lots of empty white space, a few small handwritten labels, sparse red
and blue accents only. Friendly notebook-sketch style. No gradients, no 3D, no corporate
infographic style, no dense text.

Concept: a Delta Lake table = plain data files + an append-only ledger; readers trust only
the ledger.

Composition: center-left, a loose stack of plain paper file sheets labeled "parquet files";
one sheet lies slightly apart and crossed out faintly. Center-right, a stick-figure librarian
at a tall desk writing in a thick open ledger book labeled "_delta_log". The ledger page shows
three tiny readable lines: "v0: add A", "v1: add B", "v2: remove B, add C". A blue arrow from
a small reader stick-figure at the far right points to the LEDGER (not the files), with a blue
note "readers ask the ledger, not the folder". A small red note next to the crossed-out sheet:
"removed in v2 - invisible to readers". The librarian holds a rubber stamp labeled "commit".

Handwritten labels: "parquet files", "_delta_log", the three tiny version lines, "commit" on
the stamp, blue note "readers ask the ledger, not the folder", red note "removed in v2".

Color rules: black everywhere, blue only for the reader arrow and its note, red only for the
crossed-out file note.

Constraints: one concept only, the ledger book should be the visual hero (slightly larger and
more detailed than everything else), keep at least one third of the paper blank, maximum 7
short labels, no title text.
```

## Layout description

Files stack on the left (ordinary, undramatic), ledger + librarian in the center-right (the
hero), small reader figure far right pointing at the ledger. The crossed-out file floats near the
stack, still physically present but marked dead by the ledger.

## Labels to include

- `parquet files` — black
- `_delta_log` — black, on the ledger
- `v0: add A / v1: add B / v2: remove B, add C` — tiny, on the ledger page
- `commit` — black, on the stamp
- `readers ask the ledger, not the folder` — blue
- `removed in v2` — red, at the crossed-out file

## Style rules

White paper, rough black pen, ledger is the largest/most detailed object, ≥1/3 blank, red once,
blue once, 16:9.

## What to avoid

- Database cylinders, lake imagery, or the Delta triangle logo
- Showing the files being edited — files are never modified, only added/ignored
- More than three version lines in the ledger
- Explaining ACID in text; the stamp + ledger carries it

## Caption for README/social post

> A Delta table is a folder of ordinary Parquet files and one very serious librarian. Every change
> is a new numbered entry in the ledger — never an edit to a file. Readers don't browse the
> folder; they ask the ledger. That single trick is ACID transactions, time travel, and safe
> concurrent writes on plain object storage.
