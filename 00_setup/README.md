# 00_setup — From zero to a working PySpark on your laptop

## Why this matters

Before you write a single line of business logic, you need an environment where you can run PySpark, see the Spark UI, and trust that what works on your laptop will also run on a real cluster. This module gets you there in under an hour.

By the end you can:

- Explain what Spark *is* and what problem it solves, in plain English.
- Install PySpark locally on macOS / Linux / Windows and run a job.
- Open the Spark UI at `http://localhost:4040` and find the DAG.
- Sign up for Databricks Community Edition (free, forever) and run the same job there.
- Write and run your first PySpark program end-to-end.

## Notes (in order)

1. [`01-what-is-spark.md`](./01-what-is-spark.md) — The 5-minute explanation. Why Spark exists, where it sits, who uses it.
2. [`02-architecture-overview.md`](./02-architecture-overview.md) — Driver, executor, cluster manager. A bird's-eye picture you'll re-use everywhere.
3. [`03-local-setup.md`](./03-local-setup.md) — Install Python, Java, PySpark. Verify with a one-liner.
4. [`04-databricks-community.md`](./04-databricks-community.md) — Free hosted Spark for when you outgrow local.
5. [`05-first-pyspark-program.md`](./05-first-pyspark-program.md) — Word count, line-by-line.

## Examples

- [`examples/01_hello_spark.py`](./examples/01_hello_spark.py) — Smallest possible Spark program. Prints "hello".
- [`examples/02_word_count.py`](./examples/02_word_count.py) — The "Hello World" of distributed computing.

## Diagrams

- [`diagrams/spark-stack.mmd`](./diagrams/spark-stack.mmd) — Where PySpark sits in the Spark stack.

## After this module

If `python examples/02_word_count.py` runs without errors and you opened `http://localhost:4040` and saw a "Jobs" tab — move on to [`01_fundamentals`](../01_fundamentals/).

## References

- [LS Ch.1, Ch.2] — *Learning Spark 2e*, intro and downloading.
- [HPS Ch.1] — *High Performance Spark 2e*, intro.
- 📺 [Introduction to Apache Spark — Databricks Academy](https://www.databricks.com/learn/training/lp/apache-spark-developer-essentials)
- 📺 [Spark in 100 Seconds — Fireship](https://www.youtube.com/watch?v=tDVPcqGpEnM)
