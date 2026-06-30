# High Performance Spark, 2e — chapter notes

Karau, Warren. O'Reilly, 2024. ~400 pages.

The deep-dive on making Spark fast. Less foundational than *Learning Spark*; more practical for production work.

---

## Ch.1 — Introduction to High Performance Spark

**Key concepts**
- Why Spark performance work matters: a 10× speedup is a 10× cost cut.
- The performance hierarchy: code, configs, cluster, data layout.
- Performance vs cost trade-offs.

**Takeaway**: Most performance work is *not* about clever algorithms — it's about understanding the engine and giving it good inputs.

---

## Ch.2 — How Spark Works

**Key concepts**
- Driver / executor model in detail.
- DAG → stages → tasks.
- Shuffle internals: write side, read side, external shuffle service.
- The block manager: storage of cached blocks, shuffle blocks, broadcast variables.
- Serialization (Java, Kryo, off-heap).

**Most useful** — understand:
- What happens between a wide transformation and the next stage start.
- Why shuffle is expensive (disk write + network + disk read).
- The role of the BlockManager.

**Takeaway**: Performance comes from understanding what Spark is doing internally at each transformation.

**This lab**: module 01 `02-job-stage-task.md` covers a subset of this.

---

## Ch.3 — DataFrames, Datasets, and Spark SQL

**Key concepts**
- Catalyst optimizer in depth: parsing, analysis, optimization, physical planning.
- Tungsten execution: code generation, off-heap memory.
- Common Catalyst rules: predicate pushdown, projection pruning, constant folding, join reordering.
- The cost-based optimizer (CBO) vs rule-based.

**Most useful APIs**:
```python
df.explain("formatted")    # most readable plan format
df.explain("cost")         # with CBO cost estimates
spark.sql("ANALYZE TABLE ... COMPUTE STATISTICS").show()
```

**Takeaway**: Catalyst is opinionated and usually right. When it isn't, you usually need to give it better stats or rewrite to avoid the rule it's missing.

**This lab**: module 03 `01-catalyst.md`, `02-tungsten.md`.

---

## Ch.4 — Joins (HPS classic)

**Key concepts**
- Join strategies: BroadcastHash, SortMerge, ShuffleHash, BroadcastNestedLoop, CartesianProduct.
- When each is chosen; how to force.
- Skew handling: salting (manual) and AQE (automatic).
- Bucketed joins — shuffle-free joins on pre-bucketed tables.
- Join hints (`/*+ BROADCAST(...) */`, etc).

**Most useful**:
```python
# Force broadcast
a.join(F.broadcast(b), "k")

# SQL hint syntax
spark.sql("SELECT /*+ BROADCAST(b) */ * FROM a JOIN b ON a.k = b.k")

# Anti-skew salting
salted = df.withColumn("salt", (F.rand() * N).cast("int"))
```

**Takeaway**: Pick broadcast when one side fits. For symmetric large-large joins, accept the shuffle and tune AQE.

**This lab**: module 03 `09-broadcast-joins.md`, `08-skew-handling.md`.

---

## Ch.5 — Effective Transformations

**Key concepts**
- Narrow vs wide and why it matters.
- Repartition vs coalesce — when each.
- `mapPartitions` for amortizing per-row costs.
- Hash partitioning, range partitioning, custom partitioners.
- Avoiding the "many small partitions" anti-pattern.

**Most useful**:
```python
# Per-partition init
def fn(iter):
    conn = open_connection()
    for row in iter:
        yield process(row, conn)
    conn.close()
df.rdd.mapPartitions(fn)

# Repartition by column for downstream joins
df.repartition(N, F.col("key"))
```

**Takeaway**: The cost of operations is dominated by shuffles. Knowing which operations cause shuffles, and how to skip them, is most of the game.

**This lab**: module 01 `06-narrow-vs-wide.md`, module 03 `05-partitioning.md`.

---

## Ch.6 — Effective Aggregations and Group Operations

**Key concepts**
- Two-phase aggregation: map-side combine, then reduce-side aggregate.
- When Spark can do map-side combine (commutative + associative).
- Window function performance.
- Skewed aggregations and the salting trick.

**Takeaway**: Spark almost always does map-side combine — but UDF-based aggregations defeat it. Use built-ins.

---

## Ch.7 — Working with Goldilocks

**Key concepts** — the famous chapter on data skew.
- The "Goldilocks" rank-based use case as a motivating example.
- Sample-then-merge approaches.
- Sort-with-sample to bound state.
- AQE skew handling.

**Takeaway**: For skewed work, two-pass algorithms are often the answer. Use a cheap pre-pass to find skew, then handle it explicitly in pass two.

---

## Ch.8 — Going Beyond Scala (UDFs, UDAFs, ML)

**Key concepts**
- Pandas UDFs — vectorized, fast.
- The performance gap between UDF and built-in (~10×).
- When UDFs are unavoidable.

**Most useful**:
```python
@F.pandas_udf("double")
def my_udf(s: pd.Series) -> pd.Series:
    return s.apply(some_fn)
```

**Takeaway**: Pandas UDFs close most of the gap to built-ins. Plain Python UDFs are a last resort.

**This lab**: module 02 `11-UDFs.md`.

---

## Ch.9 — Spark SQL and DataFrames in Production

**Key concepts**
- Schema evolution strategies for production DataFrames.
- Type widening / narrowing pitfalls.
- Backward compatibility for downstream consumers.

**Takeaway**: Production schemas are contracts. Evolve them deliberately, not opportunistically.

---

## Ch.10 — Streaming with Spark

**Key concepts**
- The Structured Streaming model — same as LS Ch.8 but with performance angle.
- State store internals: in-memory map, RocksDB.
- Tuning state store size, watermark, checkpoint frequency.
- Trade-offs of foreachBatch vs native sinks.

**Takeaway**: State store sizing is the most common streaming perf issue. Bound state via watermarks and consider RocksDB for > 1 GB state per partition.

**This lab**: module 05 entirely.

---

## Ch.11 — Productionizing Spark

**Key concepts**
- Deployment patterns: standalone, YARN, Kubernetes, Databricks, EMR.
- CI/CD for Spark jobs.
- Monitoring: metrics, logs, alerts.
- Disaster recovery, retry, idempotency.

**Takeaway**: A Spark job in production is mostly *not* Spark. It's testing, deployment, monitoring, alerting, runbooks.

**This lab**: module 06 `05-deployment-patterns.md`.

---

## Ch.12 — Testing and Debugging

**Key concepts**
- Unit testing PySpark with a local SparkSession.
- Schema and content assertions.
- Property-based testing.
- Debugging strategies for stuck or slow jobs.

**Takeaway**: Test the business logic, not Spark. Keep transformations pure (DataFrame → DataFrame functions); test those.

---

## Overall takeaway

If *Learning Spark* teaches you the API, *High Performance Spark* teaches you the engine. Read it once for foundation, then keep it on the shelf — re-read the relevant chapter when you hit a performance problem in that area.
