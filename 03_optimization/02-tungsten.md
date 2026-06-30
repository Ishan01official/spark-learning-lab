# 02 — Tungsten: the execution engine

## Why this matters

Catalyst decides *what* to do. Tungsten decides *how* to execute it efficiently on hardware. It's the reason a DataFrame `groupBy` is 5–10× faster than the equivalent RDD `reduceByKey` even though they compute the same answer.

## What Tungsten is

Three big pieces:

1. **Off-heap binary memory layout** — rows live as packed bytes in `UnsafeRow`, not Java objects. No GC pressure, no boxing, cache-friendly.
2. **Whole-stage code generation (codegen)** — at runtime, Spark generates a single Java method that fuses many operators (e.g. `Filter → Project → Aggregate`) into one tight loop instead of calling iterator hooks for each row.
3. **Cache-aware algorithms** — sort, hash, and aggregate implementations that fit working sets into L1/L2 cache.

[LS Ch.3 §"Project Tungsten"], [HPS Ch.2 §"Tungsten"]

## How codegen actually shows up

```python
df = spark.range(1_000_000)
df.filter("id > 100").select((F.col("id") * 2).alias("x")).explain()
```

Look for `*(1)` in the plan — the asterisk and number mean **whole-stage codegen stage 1**. Operators inside the same `*(N)` block were fused into one generated function.

```
== Physical Plan ==
*(1) Project [(id#0L * 2) AS x#3L]
+- *(1) Filter (id#0L > 100)
   +- *(1) Range (0, 1000000, step=1, splits=8)
```

All three nodes share `*(1)` → one generated method, one loop, no per-row virtual calls.

## When codegen breaks

| Cause | What you see | Fix |
|---|---|---|
| Python UDF in the middle of the plan | `BatchEvalPython` node (no `*`) | Use built-in functions or Pandas UDF |
| Very wide plans (~hundreds of expressions) | Codegen falls back to interpreted mode (`spark.sql.codegen.fallback=true` triggers) | Split the query, materialize intermediate results |
| Data source that doesn't support codegen | Operator without `*` | Convert to a supported format earlier in the pipeline |

## UnsafeRow in one paragraph

A regular JVM `Row` of `(Long, String, Int)` allocates an object header, three boxed values, two pointers, and a string with its own header — easily 80+ bytes for what is logically 16 bytes of data. `UnsafeRow` lays the values out as a fixed-width null-bitmap + fixed values + a variable-width region — typically 30–50% smaller, no GC, addressable by offset. This is why shuffle data, broadcast variables, and cached `MEMORY_ONLY` blocks are so much smaller than the equivalent RDD.

## Scale notes

- **GC pause reduction**: a real workload of 100M rows aggregating in RDD-land might spend 20–30% of CPU in GC. The same in DataFrame-land: typically <5%.
- **Memory footprint**: Tungsten-cached DataFrame ≈ 0.5–0.7× the equivalent serialized RDD; uncached vs Java object RDD it can be 3–5× smaller.
- **Throughput**: simple `filter+project+sum` on 1B rows on a 16-core machine — RDD ~120s, DataFrame ~10s. The gap is almost entirely Tungsten.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `BatchEvalPython` in plan, slow stage | Python UDF | Replace with `F.expr` / Pandas UDF |
| `WholeStageCodegen disabled` warning | Plan exceeds `spark.sql.codegen.hugeMethodLimit` (default 65535 bytecode) | Reduce projected columns, split the query |
| OOM in executor right after a wide shuffle | `UnsafeRow` is off-heap; off-heap memory not sized | Set `spark.memory.offHeap.enabled=true` and `spark.memory.offHeap.size` |

## References

- 📺 [Deep Dive into Project Tungsten — Reynold Xin, Spark Summit](https://www.youtube.com/results?search_query=spark+summit+tungsten+reynold+xin)
- [LS Ch.3] — "Project Tungsten"
- [HPS Ch.2 §"Tungsten"] and Ch.4 on codegen-friendly transformations
- [DAS Ch.1] — execution model
