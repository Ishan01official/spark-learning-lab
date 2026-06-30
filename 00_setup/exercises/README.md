# Exercises — 00_setup

Commit your solutions in this folder as `solution_<n>.py` (or `.md` if the answer is prose).

## 1. Smoke test your environment

Run `examples/01_hello_spark.py`. Then in the printed output, find:

- The Spark version.
- The default parallelism number (it equals your CPU core count under `local[*]`).
- The application ID (it'll look like `local-1719...`).

Write a one-line answer for each in `solution_1.md`.

## 2. Read the DAG

Run `examples/02_word_count.py`. While paused at `input(...)`, open <http://localhost:4040>.

- How many **jobs** were created?
- How many **stages** are in the word-count job?
- Find the **Exchange** node in the DAG — what does it represent?
- What does the **Sort** node show for "data size total"?

Answer in `solution_2.md`.

## 3. Make it slower on purpose

Modify the word-count example to force more shuffling: split into more partitions before the group-by.

```python
words = words.repartition(200, "word")  # add this line
```

Re-run. Then in the UI:

- How did the task count change?
- How did the total shuffle write size change?
- Why is this bad for a tiny dataset?

Write your answer in `solution_3.md`.

## 4. Run the same code on Databricks Community Edition

Paste the body of `02_word_count.py` into a Databricks notebook (skip the `SparkSession.builder` part — `spark` already exists; skip the `input()` and `spark.stop()`).

Use `display(counts)` instead of `counts.show()`. Did the result match?

Save a screenshot or short note in `solution_4.md`.
