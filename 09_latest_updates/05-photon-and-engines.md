# 05 — Photon and alternative execution engines

Spark's open-source execution layer is JVM-based. Several projects re-implement it in native code (C++ or Rust) for speed. Most relevant: Photon (Databricks), Velox (Meta), Gluten (Intel), DataFusion (Apache Arrow project).

## Why a new engine matters

The classic Spark engine has several costs:
- **JVM overhead** — GC pauses, JIT warmup, indirection.
- **Tungsten codegen** is good but limited to certain operators.
- **Serialization** between Java objects and off-heap memory.

A native engine eliminates much of this. The win: 2–10× speedup on the same hardware, on supported workloads.

## Photon

Databricks' proprietary native execution engine. Closed-source. Bundled with Databricks runtime ≥ 10.4 LTS.

What it accelerates:
- DataFrame and SQL operations.
- Scans of Parquet, Delta.
- Most operators: filters, projections, joins, aggregations.
- Some UDFs (compiled Python UDFs via Arrow).

What it doesn't:
- RDD operations.
- Some streaming operations (improving).
- Custom JARs / Scala UDFs (often fall back to JVM).

### Cost model

Photon is enabled per cluster (or per cluster type). It's typically more expensive per DBU but consumes fewer DBUs per query — net cost often lower.

### How you see it in plans

```
== Physical Plan ==
... (Photon) ...
*(2) PhotonHashAggregate(keys=[country], functions=[sum(amount)])
+- PhotonShuffleExchangeSink ...
   +- *(1) PhotonHashAggregate(keys=[country], functions=[partial_sum(amount)])
      +- PhotonScan parquet [country, amount]
```

`Photon` prefixes indicate Photon-accelerated operators. A mixed plan (some Photon, some not) is normal — fallback happens for unsupported operators.

## Velox

Meta's open-source C++ execution library. Used inside Presto, RaptorX, and others. Gluten brings Velox to Spark.

## Gluten

Open-source plugin that runs Spark queries on Velox (or ClickHouse engine). [https://github.com/apache/incubator-gluten](https://github.com/apache/incubator-gluten)

What it does:
- Intercepts Spark SQL plans.
- Translates supported operators to Velox.
- Falls back to JVM for unsupported ops.

It's the "open Photon" — same idea, OSS. Production adoption is growing; ecosystem maturity lags Photon.

## DataFusion

Apache Arrow's Rust-based query engine. Standalone (not a Spark plugin). Different shape — it's a query engine you can embed, not a Spark accelerator.

Relevant because:
- Same architectural philosophy (native, vectorized, Arrow-based).
- DuckDB-like for batch query workloads on Parquet/CSV.
- For small-medium data, native engines are often faster than Spark and need a fraction of the resources.

## What this means for you

### If you use Databricks
- Photon is the default for newer DBR. You get it.
- Avoid Python UDFs (or use pandas UDFs); they don't accelerate as well.
- Avoid RDD operations. Use DataFrame / SQL.
- Avoid operations that fall back to JVM (rare; check plans).

### If you self-host Spark on K8s
- Gluten + Velox is the OSS path to Photon-like performance.
- Maturity is improving but lags Photon. Test thoroughly.
- The performance gains can justify the integration cost for large workloads.

### If you're choosing a stack
- For < 1 TB analytic workloads, DuckDB / DataFusion are often simpler and faster than Spark.
- For TB-PB Spark workloads with predictable shape, Photon / Gluten + Velox shine.
- For complex streaming, custom code, multi-language Spark is still the path.

## What's NOT changing

The DataFrame API is the same. Your code doesn't change between Photon-on and Photon-off. The execution underneath differs, but `df.groupBy(...).agg(...)` is `df.groupBy(...).agg(...)`.

That's the point: the API is a contract. The engine evolves under it.

## References

- Photon paper: https://www.databricks.com/blog/2021/06/17/photon-blazingly-fast-query-engine-for-databricks.html
- Velox: https://velox-lib.io/
- Gluten: https://github.com/apache/incubator-gluten
- DataFusion: https://datafusion.apache.org/
- 📺 [Photon — Databricks](https://www.youtube.com/results?search_query=databricks+photon+engine)

## TL;DR

JVM-based Spark is good but slow compared to native engines. Photon (Databricks), Gluten+Velox (OSS), and DataFusion (different shape) are the alternatives. Same DataFrame API; faster execution. Worth knowing about so you can recognize plans, choose a stack, and not be surprised by 5× cost differences.
