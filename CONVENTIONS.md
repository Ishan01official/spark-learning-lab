# CONVENTIONS

## File structure

Every module follows the same shape:

```text
<module>/
├── README.md          # Why this topic matters; what you'll be able to do after
├── 01-*.md            # Numbered notes; ordered by dependency, not alphabetically
├── 02-*.md
├── ...
├── examples/          # Runnable scripts, every one tested locally
│   ├── 01_*.py
│   └── 02_*.py
├── exercises/         # Practice prompts; commit your answer as solution_*.py
└── diagrams/          # Mermaid (.mmd); GitHub renders them inline
```

## Note structure (every `NN-topic.md`)

Each technical note has these sections, in order. Skip a section only if it genuinely doesn't apply:

1. **Why this matters** — the production problem this concept solves.
2. **What it is** — a one-paragraph definition. No jargon without a parenthetical.
3. **How it works** — line-by-line walkthrough of a runnable example. Code first, prose second.
4. **When to use vs not** — best use cases + anti-patterns.
5. **Scale notes** — data sizes, partition counts, shuffle cost, memory cost.
6. **Failure modes** — what breaks, what the Spark UI shows, how to fix it.
7. **References** — book chapter tags + curated video links.

For new senior-level modules, also include these sections when relevant:

1. **Simple explanation** — for a beginner.
2. **Production explanation** — how it behaves at work.
3. **Interview explanation** — how to answer under pressure.
4. **Architecture explanation** — tradeoffs and system boundaries.
5. **Exercise** — something the learner can run, inspect, or write.

## Code style (Python / PySpark)

- Python 3.10+, PySpark 3.5+ (works on Databricks Runtime 14+).
- Every example is runnable as `python <module>/examples/<file>.py` from the repo root with `.venv` activated.
- One `SparkSession` per script, in a `def get_spark() -> SparkSession:` helper or top of file.
- Every example ends with `spark.stop()` so the local JVM exits cleanly.
- Use `pyspark.sql.functions as F` — never `from pyspark.sql.functions import *`. Star imports clash with Python builtins (`sum`, `max`, `min`, `count`, `round`).
- Type hints on every helper function.
- Comments explain *why*, not what.

## Diagram style

- Mermaid (`.mmd`) — renders directly on GitHub.
- Flowcharts use `flowchart LR` (left-to-right) for pipelines, `flowchart TD` (top-down) for hierarchies.
- For Spark execution flows, use the standard shapes:
  - `[Box]` = stage or operator
  - `(Pill)` = input/output
  - `{Diamond}` = decision (e.g. shuffle vs no-shuffle)

## Book references

Inline as bracketed tags:

- `[LS Ch.3 §"Spark's Structured APIs"]` for *Learning Spark 2e*.
- `[HPS Ch.5]` for *High Performance Spark 2e*.
- `[DAS Ch.4]` for *Data Algorithms with Spark*.
- `[Cert Domain 2]` for the Databricks certification exam guide.

Full book metadata in [`BOOK_MAP.md`](./BOOK_MAP.md).

## Video references

- One curated link per concept, max. Prefer official Databricks Academy or Spark Summit talks.
- Format: `Video: Title, Speaker, Org, Year, URL`.
- If the canonical talk is paywalled, link the slides or a free equivalent.

## Commit messages

- `module(00_setup): add hello-spark example`
- `note(01_fundamentals): explain narrow vs wide transformations`
- `fix(02_pyspark_core/examples/04_joins.py): handle empty-side broadcast`

## What I won't do in this repo

- No copy-pasted blog posts.
- No "10 best PySpark tricks" listicle pages.
- No code without a line-by-line walkthrough.
- No diagram for the sake of a diagram — only when it makes the concept faster to grasp.
- No empty placeholder markdown files.
