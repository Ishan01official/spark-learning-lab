# 03 — Optimization

How Spark plans your query, how to read those plans, and the levers you have to make queries faster, cheaper, and more reliable.

## What you should be able to do after this

- Read an `explain()` plan top-to-bottom and explain what every node does.
- Predict which optimizations Catalyst will apply (and which it won't).
- Use AQE deliberately, not blindly.
- Choose the right partitioning at read time, shuffle time, and write time.
- Decide when to `cache`, when to `checkpoint`, and when neither.
- Diagnose skew, OOMs, and small-files problems from the Spark UI alone.

## Notes

1. [Catalyst — the query optimizer](01-catalyst.md)
2. [Tungsten — the execution engine](02-tungsten.md)
3. [Reading explain() plans](03-reading-plans.md)
4. [Adaptive Query Execution (AQE)](04-aqe.md)
5. [Partitioning strategies](05-partitioning-strategies.md)
6. [Caching and persistence](06-caching-and-persistence.md)
7. [Shuffle tuning](07-shuffle-tuning.md)
8. [Skew handling](08-skew-handling.md)
9. [Broadcast joins in depth](09-broadcast-joins.md)
10. [Memory tuning and OOM](10-memory-tuning.md)
11. [Spark UI tour](11-spark-ui-tour.md)
12. [Debugging failures — a playbook](12-debugging-failures.md)

## Book references

- **High Performance Spark 2e** — Ch.2–6 are the canonical reference for everything here.
- **Learning Spark 2e** — Ch.7 (Optimizing & Tuning).
- **Data Algorithms with Spark** — appendices on tuning, plus optimization-themed solutions in chapters.
- **Databricks Cert** — many cert questions live in this module.
