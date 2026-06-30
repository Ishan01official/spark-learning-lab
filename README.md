# spark-learning-lab

A topic-wise, book-anchored, code-first lab for learning Apache Spark and PySpark from fundamentals to production data engineering.

Same study philosophy as [`python-engineering-systems`](https://github.com/Ishan01official/python-engineering-systems):
**read → explain simply → run the code → break it → fix it → write it up.**

## What this repo is

Not a tutorial dump. A structured curriculum where every concept is paired with:

1. A **why** (industry context, the actual problem it solves).
2. A **how** (line-by-line walkthrough of runnable PySpark code).
3. A **when** (best use cases, anti-patterns, when *not* to use it).
4. A **scale** (data sizes, partition counts, shuffle costs, memory footprint).
5. A **failure mode** (what breaks, what the Spark UI shows, how to fix it).
6. A **diagram** (Mermaid, renders directly on GitHub).
7. **References** (book chapter + curated video).

## Source material

| Tag      | Source                                                                                  | Best for                                                            |
| -------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| **LS**   | *Learning Spark, 2nd Edition* — Damji, Wenig, Das, Lee (O'Reilly)                       | The canonical "start here" book. DataFrames, SQL, Streaming basics. |
| **HPS**  | *High Performance Spark, 2nd Edition* — Karau, Polak, Warren (O'Reilly)                 | Performance, internals, Catalyst, shuffles, skew, memory.           |
| **DAS**  | *Data Algorithms with Spark* — Mahmoud Parsian (O'Reilly)                               | Real algorithm patterns: joins, partitioning, dedup, graph.         |
| **Cert** | [Databricks Certified Associate Developer for Apache Spark](https://www.databricks.com/learn/certification/apache-spark-developer-associate) | Exam topic alignment.                                |

Chapter-by-chapter mapping lives in [`BOOK_MAP.md`](./BOOK_MAP.md).

## Repository layout

```text
spark-learning-lab/
├── 00_setup/                # Get PySpark running locally + on Databricks. First program.
├── 01_fundamentals/         # Cluster, driver, executor, RDD, DataFrame, DAG, lazy eval, narrow vs wide.
├── 02_pyspark_core/         # SparkSession, I/O, schema, transformations, aggregations, joins, windows, SQL, UDFs.
├── 03_optimization/         # Catalyst, Tungsten, AQE, partitioning, caching, shuffle, skew, broadcast, debugging.
├── 04_delta_lake/           # ACID, time travel, MERGE, OPTIMIZE, Z-order, schema evolution.
├── 05_streaming/            # Structured Streaming, watermarks, checkpointing, Kafka.
├── 06_real_projects/        # ETL, SCD2, dedup at scale, data quality framework.
├── 07_interview_prep/       # Databricks cert + scenario questions + troubleshooting.
├── 08_notes_from_books/     # Chapter-by-chapter book summaries.
├── 09_latest_updates/       # Spark 3.5 / 4.0 features, Spark Connect, K8s deployments.
├── data/                    # Tiny sample datasets used by examples.
├── BOOK_MAP.md
├── ROADMAP.md
├── CONVENTIONS.md
└── requirements.txt
```

Every module folder contains:

```text
<module>/
├── README.md          # Why this topic matters before what it is
├── 01-*.md, 02-*.md   # Numbered notes, ordered by dependency
├── examples/          # Runnable PySpark scripts, every one tested locally
├── exercises/         # Practice prompts (you commit your answer as `solution_*.py`)
└── diagrams/          # Mermaid (.mmd) — rendered directly on GitHub
```

## How to use it

1. Open the module's `README.md`. It tells you *why* the topic matters before diving in.
2. Read the numbered notes (`01-*.md`, `02-*.md`, …) in order.
3. Run every `examples/*.py` script. Change a line. Re-run. Read the Spark UI.
4. Do the `exercises/`. Commit your solution next to the prompt.
5. Check off the module in [`ROADMAP.md`](./ROADMAP.md).

To run any example from the repo root:

```bash
python -m venv .venv
source .venv/bin/activate           # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python 01_fundamentals/examples/02_dataframe_basics.py
```

Spark UI is at <http://localhost:4040> while a job is running. **Open it every time.**

## Learning path

### Tier 1 — Foundations (must do)

- **00_setup** — Spark in 90 seconds, local install, Databricks Community Edition, your first job
- **01_fundamentals** — Cluster anatomy, RDD vs DataFrame, lazy evaluation, DAG, narrow vs wide

### Tier 2 — Daily PySpark (90% of real work)

- **02_pyspark_core** — Everything you actually type into a notebook
- **03_optimization** — The difference between "Spark is slow" and "Spark is fast"

### Tier 3 — Production stack

- **04_delta_lake** — The default storage layer at Databricks-shop companies
- **05_streaming** — Real-time pipelines, Kafka, watermarks
- **06_real_projects** — End-to-end patterns: ETL, SCD2, dedup, data quality

### Tier 4 — Sharpening

- **07_interview_prep** — Databricks cert prep, scenario questions
- **08_notes_from_books** — Condensed chapter summaries
- **09_latest_updates** — Spark Connect, 3.5/4.0 changes, K8s

## Study loop (per module)

1. Read the module `README.md` once without coding.
2. Read each numbered note, type the code yourself (don't paste).
3. Run examples. Open the Spark UI. Look at the DAG, the stages, the shuffle size.
4. Break something on purpose (force OOM, force skew). Read the error.
5. Solve the exercises.
6. Write a 5-sentence Feynman summary at the bottom of the module README.
7. Tick the box in `ROADMAP.md`.

## Conventions

- Python 3.10+. PySpark 3.5+. Code targets both local and Databricks Runtime 14+.
- Mermaid for diagrams (`.mmd`) — GitHub renders them inline.
- Every example is runnable as `python <path>/file.py` from the repo root, with `.venv` activated.
- Book references inline as `[LS Ch.3]`, `[HPS Ch.5]`, etc. See [`BOOK_MAP.md`](./BOOK_MAP.md).

See [`CONVENTIONS.md`](./CONVENTIONS.md) for the full set.

## Rule

Don't just collect notes. Every concept produces code, runs locally, and gets opened in the Spark UI. **If you didn't run it and look at the UI, you didn't learn it.**
