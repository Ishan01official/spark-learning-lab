# 05 — Your first PySpark program (line by line)

We're going to write the "word count" — distributed computing's hello world. By the end you'll have run a multi-stage Spark job and seen the DAG in the UI.

Full script: [`examples/02_word_count.py`](./examples/02_word_count.py). Below is the line-by-line tour.

## The script

```python
# 1
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 2
spark = (
    SparkSession.builder
    .appName("word_count")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 3
text = [
    "spark is fast",
    "spark is distributed",
    "spark is fun",
    "fast distributed engines are fun",
]
df = spark.createDataFrame([(line,) for line in text], schema=["line"])

# 4
words = df.select(F.explode(F.split("line", r"\s+")).alias("word"))

# 5
counts = (
    words
    .groupBy("word")
    .count()
    .orderBy(F.desc("count"))
)

# 6
counts.show()

# 7
input("Press Enter to stop Spark (open http://localhost:4040 first)...")
spark.stop()
```

## Line-by-line walkthrough

### `# 1` — Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
```

`SparkSession` is *the* entry point in modern Spark. Older code uses `SparkContext` directly — don't. `SparkSession` wraps it and adds DataFrame / SQL / Catalog APIs.

`functions as F` is the universal alias. Every PySpark example you read online uses it. Never `from pyspark.sql.functions import *` — `sum`, `min`, `max`, `count`, `round` would clobber Python builtins.

### `# 2` — Create the SparkSession

```python
spark = (
    SparkSession.builder
    .appName("word_count")
    .master("local[*]")
    .getOrCreate()
)
```

- `appName` — what the Spark UI shows in the page title and what cluster operators see in YARN/K8s dashboards.
- `master("local[*]")` — run locally, using all CPU cores on this machine. `local[4]` would use exactly 4. On a real cluster you wouldn't set this in code; `spark-submit --master yarn` (or Databricks) decides it.
- `getOrCreate()` — return the existing SparkSession if one already exists in this JVM (e.g. in a notebook), otherwise build a new one.

```python
spark.sparkContext.setLogLevel("WARN")
```

Spark's default log level is INFO and it's deafening. WARN is the right default for learning.

### `# 3` — Create a tiny DataFrame

```python
text = [...]
df = spark.createDataFrame([(line,) for line in text], schema=["line"])
```

`createDataFrame` takes a list of Python tuples and a schema (here just a column name). For real data you'd `spark.read.text(...)` — we'll do that in Module 02. The point of inlining text here is that this example runs anywhere with no external file.

`df` now has one column `line` and 4 rows. Nothing has happened on the cluster yet. **Spark is lazy** — building DataFrames is free. Module 01 explains why.

### `# 4` — Tokenize

```python
words = df.select(F.explode(F.split("line", r"\s+")).alias("word"))
```

Read this inside-out:

- `F.split("line", r"\s+")` — split each `line` string on whitespace. Result is one *array of strings* per row.
- `F.explode(...)` — turn each array into multiple rows (one row per element). If a row had `["spark", "is", "fast"]`, after explode it becomes 3 rows.
- `.alias("word")` — name the resulting column `word`.
- `df.select(...)` — emit a new DataFrame with just that column.

Still nothing has executed on the cluster. We've extended the *logical plan* by two operators.

### `# 5` — Group and count

```python
counts = (
    words
    .groupBy("word")
    .count()
    .orderBy(F.desc("count"))
)
```

- `.groupBy("word")` — partition by the `word` column. This **forces a shuffle**: rows for the same word must end up on the same executor to be counted together. The shuffle is the most expensive part of any Spark job.
- `.count()` — for each group, count the rows.
- `.orderBy(F.desc("count"))` — sort by count descending. This is *another* shuffle (a global sort).

Still nothing has run. We've now built up a logical plan with 4 stages worth of work queued.

### `# 6` — Execute

```python
counts.show()
```

`show()` is an **action**. Until you call an action, Spark builds plans but does not execute them. Now Catalyst optimizes the plan, splits it into stages and tasks, and runs them across the executors. You'll see output like:

```text
+----------+-----+
|      word|count|
+----------+-----+
|     spark|    3|
|        is|    3|
|       fun|    2|
|      fast|    2|
|distributed|   2|
|   engines|    1|
|       are|    1|
+----------+-----+
```

### `# 7` — Pause so you can see the UI

```python
input("Press Enter to stop Spark...")
spark.stop()
```

The Spark UI lives at `http://localhost:4040` *only while the SparkSession is alive*. The `input()` keeps the JVM running so you can browse the UI. Hit Enter when you're done.

## What to look at in the UI

Open `http://localhost:4040` while the script is paused at the `input()`:

1. **Jobs tab** → you should see 2 jobs (one for `show()`'s `collect()`-equivalent, one for the schema introspection it does).
2. Click into the main job → **DAG visualization**. You'll see boxes for *Stage 0*, *Stage 1*, *Stage 2*, separated by **Exchange** nodes — those are the shuffles.
3. Each stage shows how many tasks ran and how much data shuffled. For this 4-row input the shuffle is bytes, not gigabytes — but the *shape* of the plan is the same shape you'd see with 4 TB.

## Industry use case

This pattern — `read → tokenize → group → count → sort` — is exactly how you'd build:

- Search-query frequency reports.
- Log analysis (`grep` at scale: parse log lines, count by error code, top errors per day).
- Click stream analytics: explode an event JSON, group by event type, count.

The only thing that changes between this 4-row demo and a 5 TB production job is the input source and the cluster size. The code is the same.

## Failure modes

- **Hangs forever at "Initial job has not accepted any resources"** — another Spark process is holding the local cores. Kill it: `jps` then `kill -9 <pid>` for any leftover `SparkSubmit`.
- **`AnalysisException: cannot resolve 'line'`** — you spelled the column name wrong, or the schema doesn't have it. `df.printSchema()` to check.
- **Empty output** — your regex `\s+` didn't match because the input was already tokenized, or the input was empty.

## References

- [LS Ch.3 §"DataFrame Operations"]
- [DAS Ch.2 §"Word Count"]
- 📺 [PySpark Word Count, Step by Step — Apache Spark Tutorial](https://www.youtube.com/watch?v=cZS5xYYIPzk)
