# Lazy Evaluation & DAG — Hand-Drawn

## Purpose

Make laziness *felt*: transformations are just a recipe being written; nothing cooks until the
action. The strongest metaphor for the most-misunderstood Spark concept — pairs with
[diagram 04](../../../diagrams/04_lazy_evaluation_and_dag.md).

## Final image-generation prompt

```text
Generate one standalone 16:9 horizontal educational sketch illustration.

Visual DNA: clean white paper background, rough black hand-drawn pen lines with a slightly
wobbly hand-drawn feel, lots of empty white space, a few small handwritten labels, sparse red
and blue accents only. Friendly notebook-sketch style. No gradients, no 3D, no corporate
infographic style, no dense text.

Concept: Spark's lazy evaluation — transformations only write the recipe; the action starts
the cooking.

Composition: on the left, a stick-figure chef calmly writing on a long recipe card. The card
shows a short checklist written as tiny scribbles with three readable words: "read", "filter",
"groupBy". Around the chef, cold untouched ingredients sit in bowls — nothing is happening,
the stove behind is clearly OFF with a tiny "zzz". On the right, the same chef now slamming a
big push-button labeled ".count()" — from the button, a blue arrow leads to the stove now ON
with pans fired up in sequence, connected by small blue arrows (a little cooking pipeline of
3 pans). A small red annotation near the left scene: "nothing runs yet!".

Handwritten labels: "transformations = recipe" under the left scene, "action" near the button,
"job runs" near the firing stove, in red: "nothing runs yet!", in blue on the pan chain: "stages".

Color rules: black for figures and objects, blue for the execution arrows and the word "stages",
red only for the "nothing runs yet!" note.

Constraints: one concept only, two scenes left and right of the same character, keep at least
one third of the paper blank, maximum 6 short labels plus the three recipe words, no title text.
```

## Layout description

Split scene, same character twice. Left half: writing the recipe, stove off, ingredients idle.
Right half: the `.count()` button pressed, stove alive, three pans chained by arrows (the stages
of the job). Time flows left → right.

## Labels to include

- `transformations = recipe` — black
- `read / filter / groupBy` — tiny, on the recipe card
- `action` — black, near the button
- `job runs` — black
- `stages` — blue, on the pan chain
- `nothing runs yet!` — red, left scene

## Style rules

White paper, rough black pen, ≥1/3 blank, the OFF/ON stove contrast must be instantly readable,
red used exactly once, 16:9.

## What to avoid

- Drawing an actual graph of nodes — the recipe/cooking metaphor *is* the DAG here
- More than three words on the recipe card
- Making both scenes busy; the left scene must feel calm and inactive
- Code snippets beyond the single ".count()" button label

## Caption for README/social post

> Spark is the chef who won't turn on the stove until you order. `filter`, `join`, `groupBy` —
> all just recipe-writing. Call `.count()` and the whole optimized meal cooks at once. That's lazy
> evaluation, and it's why Spark can optimize your entire pipeline before doing any work.
