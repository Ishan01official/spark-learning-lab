# 03 — RDDs explained

## Why this matters

RDD (Resilient Distributed Dataset) is Spark's original abstraction. In daily PySpark you'll rarely write RDD code. But:

- Every DataFrame compiles down to RDD operations internally.
- The Spark UI shows you RDDs in the Storage tab.
- Some advanced operations (`mapPartitions`, custom partitioners) only exist on RDDs.
- Interviewers love asking about it.

If you don't understand RDDs you can use Spark, but you can't debug Spark.

## What an RDD is

> An RDD is an immutable, partitioned, fault-tolerant collection of records that can be processed in parallel.

Unpacking each word:

- **Immutable** — you don't modify an RDD; transformations produce *new* RDDs.
- **Partitioned** — the records are split into chunks (partitions), each stored on one executor.
- **Fault-tolerant** — Spark remembers the *lineage* (the sequence of transformations) and can recompute any lost partition from its parents.
- **Collection of records** — generic Python objects (or Java/Scala objects). No schema, no column types. Just "stuff".
- **Parallel** — each partition is processed by one task on one core.

That last property — opaque generic objects with no schema — is exactly *why* DataFrames replaced RDDs as the everyday API. Catalyst can't optimize what it can't see inside.

## Five things every RDD has

[LS Ch.2 §"RDDs Under the Hood"]:

1. **A list of partitions** — `rdd.getNumPartitions()`.
2. **A function to compute each partition** — the closure you passed to `map`, `filter`, etc.
3. **A list of dependencies on other RDDs** — its lineage.
4. **Optionally, a partitioner** — for key-value RDDs, decides which partition each key goes to.
5. **Optionally, a list of preferred locations** — "this partition is data-local on node X."

That's the whole abstraction.

## Creating an RDD

Three ways:

```python
sc = spark.sparkContext

# 1. From a Python collection
rdd1 = sc.parallelize([1, 2, 3, 4, 5], numSlices=4)

# 2. From a file
rdd2 = sc.textFile("data/logs/*.log")

# 3. From another RDD (transformation)
rdd3 = rdd1.map(lambda x: x * 10)
```

In modern Spark, you usually don't reach for the SparkContext directly — you use the SparkSession. But `spark.sparkContext` is always available.

## Transformations vs actions on RDDs

**Transformations** are lazy. They return a new RDD without doing any work.

```python
even = rdd1.filter(lambda x: x % 2 == 0)   # nothing happens
squared = even.map(lambda x: x * x)        # nothing happens
```

**Actions** trigger computation and return a value (or write to storage).

```python
squared.collect()       # returns [4, 16] — runs the pipeline
squared.count()         # returns 2
squared.saveAsTextFile("out/")
```

Same lazy-evaluation model as DataFrames. Module 05 expands on it.

## Common transformations (you'll see these all over Spark internals)

| Op | Type | Description |
|---|---|---|
| `map(f)` | narrow | apply `f` to each element |
| `filter(f)` | narrow | keep elements where `f(x)` is True |
| `flatMap(f)` | narrow | `map` then flatten (f returns iterable per element) |
| `mapPartitions(f)` | narrow | apply `f` to each partition (an iterator). Lets you amortize setup cost like opening a DB connection per partition rather than per row. |
| `union(rdd2)` | narrow | concatenate. Partitions are appended; no shuffle. |
| `distinct()` | wide | dedupe — requires shuffle |
| `groupByKey()` | wide | bring all values for each key together — expensive, prefer `reduceByKey` |
| `reduceByKey(f)` | wide | merge by key with a function; combines locally before shuffling (cheaper than groupByKey) |
| `join(rdd2)` | wide | hash-join two key-value RDDs |
| `repartition(n)` | wide | full shuffle to n partitions |
| `coalesce(n)` | narrow (if n < current) | reduce partition count without shuffle |

## Lineage and fault tolerance

```python
a = sc.parallelize(range(1_000_000), numSlices=10)
b = a.map(lambda x: x * 2)
c = b.filter(lambda x: x > 100)
c.count()
```

Spark remembers: `c = filter(map(parallelize(...)))`. If one executor dies and partition 7 of `c` is lost, Spark re-runs `parallelize → map → filter` for that one partition. It does **not** re-run the whole job. This is what "resilient" in RDD means.

Lineage works as long as the input source is replayable (a file, a Kafka topic). It doesn't work for randomness — a `map(lambda _: random())` re-execution produces different values. For non-deterministic computations, `cache()` to materialize and break lineage.

## When you *should* use RDDs over DataFrames

DataFrames are the right default. RDDs are still right for:

1. **Custom partitioning that Catalyst can't express.** E.g. a custom `Partitioner` that hashes only on a slice of a key.
2. **`mapPartitions` with expensive setup.** Connecting to a downstream API or DB once per partition rather than once per row, when you can't push it into a UDF.
3. **Unstructured data with no schema.** Raw binary parsing where you need fine-grained control.
4. **Working with the Spark internals.** E.g. operating on a DataFrame's underlying `.rdd`.

For 95% of PySpark work, you use DataFrames and never touch RDDs directly.

## DataFrame → RDD escape hatch

```python
rdd_of_rows = df.rdd                 # each element is a Row object
rdd_of_dicts = df.rdd.map(lambda r: r.asDict())
```

You can always drop to RDD, but going *back* with `rdd.toDF(schema)` loses Catalyst's view of what you did in between. Use as a last resort.

## Failure modes

- **`groupByKey` OOM** — `groupByKey` shuffles every value for every key to one machine before reducing. On a key with 100M values, that's 100M values in one task's memory. Use `reduceByKey` / `aggregateByKey` instead — they reduce locally first.
- **Stragglers from skewed RDD keys** — same root cause; one partition holds far more data than others. Module 03's skew handling applies to RDDs too.
- **No type safety** — you only learn that `x * 2` was applied to a string when the task fails at runtime. DataFrames catch this at plan time.

## References

- [LS Ch.2 §"RDDs Under the Hood"], Ch.3 §"Why Structure?"]
- [HPS Ch.2 §"RDDs"], Ch.6 §"Working with Key/Value Data"]
- [DAS Ch.2 §"Transformations in Action"]
- Original Spark paper — [Zaharia et al., "Resilient Distributed Datasets" (2012)](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
- 📺 [RDD Deep Dive — Reynold Xin](https://www.youtube.com/watch?v=dmL0N3qfSc8)
