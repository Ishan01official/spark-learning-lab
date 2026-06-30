# Contributing

This repo is a learning lab, so contributions should make concepts easier to run, debug, and explain.

## Good Contributions

- Add a runnable PySpark example for a concept that only has notes.
- Add an exercise with a realistic failure mode.
- Improve a diagram that explains execution, shuffle, state, or storage behavior.
- Add a short troubleshooting note based on an actual error.
- Update Spark, Delta, Databricks, or PySpark release notes with a tested example.

## Content Standard

Each new concept should include:

1. Why the concept matters.
2. A minimal runnable example.
3. What to inspect in the Spark UI.
4. Common failure modes.
5. A reference to book, docs, or release notes when applicable.

## Before Committing

Run:

```bash
make validate
```

For code examples, also run the specific script you changed:

```bash
python path/to/example.py
```

If an example needs a real cluster, cloud service, Kafka, or a large dataset, say that clearly in the README near the command.

## File Naming

- Notes: `01-topic-name.md`
- Examples: `01_topic_name.py`
- Exercises: describe the task in `exercises/README.md`
- Diagrams: use Mermaid files with `.mmd`

## Commit Style

Use direct commit messages:

```text
Add streaming watermark exercise
Fix local Spark setup notes
Update Spark 4 migration watchlist
```
