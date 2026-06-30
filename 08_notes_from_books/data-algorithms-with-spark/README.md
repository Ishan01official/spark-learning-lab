# Data Algorithms with Spark — chapter notes

Parsian. O'Reilly, 2022. ~400 pages.

A cookbook of algorithmic recipes implemented in PySpark — useful when you have a specific class of problem (top-N, joins, recommendations, graph) and want a tested pattern.

These notes summarize the chapters most relevant to data engineering. Skip the ML-focused chapters unless that's your focus.

---

## Ch.1–3 — Foundations (skim)

Covers Spark basics, RDDs, DataFrames. Mostly redundant if you've read *Learning Spark*. Worth checking the writing style and idioms used in the rest of the book.

---

## Ch.4 — Reductions in Spark

**Key concepts**
- `reduceByKey` vs `groupByKey` vs `aggregateByKey`.
- Map-side combiner: when it kicks in.
- Building custom aggregations with `aggregateByKey`.

**Most useful**:
```python
# Per-key sum with map-side combine
rdd.reduceByKey(lambda a, b: a + b)

# Custom aggregation: zero, seq op, comb op
rdd.aggregateByKey(0, lambda a, b: a + b, lambda a, b: a + b)
```

**Takeaway**: For RDDs, prefer `reduceByKey`. For DataFrames, `groupBy().agg(F.sum(...))` is the equivalent and just as efficient.

---

## Ch.5 — Top-N Algorithms

**Key concepts**
- Top-N per group: `Window.partitionBy(...).orderBy(...)` + `row_number`.
- Approximate top-N: `F.percentile_approx`.
- Reservoir sampling for unbounded streams.
- The `treeAggregate` for hierarchical reductions on very wide aggregations.

**Most useful**:
```python
w = Window.partitionBy("group").orderBy(F.desc("score"))
df.withColumn("rn", F.row_number().over(w)).filter("rn <= 10").drop("rn")
```

**Takeaway**: Window functions are the standard answer to top-N. For very large groups with very large N, consider sampling or `treeReduce`.

**This lab**: module 02 `09-window-fns.md`.

---

## Ch.6 — Markov Chains

(Skim — domain-specific.)

---

## Ch.7 — Recommendation Systems

(Skim unless that's your domain.)

---

## Ch.8 — Join Algorithms

**Key concepts**
- Broadcast vs shuffle join (covered everywhere).
- Sort-merge join internals.
- Map-side joins for very small references.
- Skewed key handling with manual salting + final dedup.
- Range joins (joins with `BETWEEN` predicates) — Spark doesn't optimize these well.

**Most useful**:
```python
# Range join (slow by default — Cartesian + filter)
a.join(b, F.expr("a.start <= b.ts AND b.ts <= a.end"))

# Bucketized range join (faster)
# Pre-bucket the timeline, join on bucket + filter
```

**Takeaway**: Range joins need special handling. Either pre-bucket your data, or accept Cartesian-product pricing.

**This lab**: module 02 `08-joins.md`, module 03 `09-broadcast-joins.md`.

---

## Ch.9 — Order Inversion (SCD-adjacent)

**Key concepts**
- Combining two related streams of records (e.g., transactions and pricing-changes).
- "Order inversion" pattern for tagging records with the latest applicable reference.
- Useful in SCD2 implementations.

**This lab**: module 06 `04-scd-types.md`, project `scd2-customers/`.

---

## Ch.10 — Numeric and Aggregation Algorithms

**Key concepts**
- Moving averages (window functions).
- Exponentially weighted aggregations.
- Stratified sampling.
- Approximate algorithms: HyperLogLog (`approx_count_distinct`), t-digest.

**Most useful**:
```python
F.approx_count_distinct("user_id", rsd=0.01)  # ~1% error, fast
df.stat.approxQuantile("amount", [0.5, 0.95, 0.99], 0.001)
```

**Takeaway**: For "good enough" aggregations at scale, approximate algorithms are usually right answer.

---

## Ch.11 — Streaming Algorithms

**Key concepts**
- Sketches: HyperLogLog, Count-Min Sketch, Bloom Filter.
- Windowed dedup with bloom filters.
- Reservoir sampling at scale.
- Streaming top-K with space-saving algorithm.

**Takeaway**: For unbounded streams, exact algorithms have unbounded state. Approximate algorithms with bounded error are the standard answer.

**This lab**: module 05 `05-stateful-aggregations.md`.

---

## Ch.12 — Graph Algorithms

(Skim — uses GraphX/GraphFrames which are off the main path for most data engineering.)

---

## Ch.13–14 — Combinatorics and ML pre-processing

(Skim unless directly relevant.)

---

## Overall takeaway

DAS is a recipe book. The best way to use it: skim the table of contents now, then pull it off the shelf when you have a "how do I do top-N per group at scale" or "how do I implement a streaming dedup" question. Most chapters have a 10-line code snippet that gets you 80% of the way there.

Most relevant chapters for the projects in module 06:
- Ch.5 (top-N) → orders-etl gold layer.
- Ch.8 (joins) → all projects.
- Ch.9 (order inversion) → scd2-customers.
- Ch.10 (approximate) → dq-framework, gold aggregations.
- Ch.11 (streaming) → clickstream-stream.
