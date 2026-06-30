# 06 — Caching and persistence

## Why this matters

Spark recomputes lineage on every action. If you `count()` a DataFrame and then `show()` it, the entire pipeline runs *twice*. Caching pins an intermediate result to memory or disk so subsequent actions skip the upstream work.

Used right: 10× speedup for iterative jobs.
Used wrong: OOM, slow jobs, and "why did caching make it slower" tickets.

## How to cache

```python
df.cache()                              # storage level = MEMORY_AND_DISK
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
df.unpersist()                          # release
```

`cache()` is shorthand for `persist(MEMORY_AND_DISK)` (since Spark 2.x, for DataFrames).

[LS Ch.7 §"Caching and Persistence of Data"]

## The storage levels

| Level | Memory | Disk | Serialized? | Replication | When to use |
|---|---|---|---|---|---|
| `MEMORY_ONLY` | yes | no | no | 1 | RDDs of small data; fast access |
| `MEMORY_ONLY_SER` | yes | no | yes | 1 | RDDs that don't fit deserialized |
| `MEMORY_AND_DISK` (default) | yes | spill | mixed | 1 | DataFrames — default and usually right |
| `MEMORY_AND_DISK_SER` | yes | spill | yes | 1 | RDDs too big for memory; saves space |
| `DISK_ONLY` | no | yes | yes | 1 | Reuse expensive computation, no memory for it |
| `OFF_HEAP` | off-heap | no | yes | 1 | Reduce GC; needs off-heap memory configured |
| `MEMORY_AND_DISK_2` | yes | yes | mixed | 2 | Replicated; survives executor loss |

For DataFrames, Tungsten already serializes to UnsafeRow internally, so `MEMORY_AND_DISK` and `MEMORY_AND_DISK_SER` are essentially the same — use the former.

## When caching is *correct*

A cache pays off only if you read the dataset **at least twice**, and the cost to compute it is significant.

```python
# YES — this benefits from caching
features = build_features(raw)  # expensive
features.cache()

train_model(features.filter("split = 'train'"))
evaluate(features.filter("split = 'test'"))
features.unpersist()
```

```python
# NO — this doesn't
df = spark.read.parquet("...")
df.cache()
df.count()          # only action; cache is wasted
```

## When caching is *wrong*

| Anti-pattern | What goes wrong |
|---|---|
| Caching the raw input | Parquet/ORC already gives projection pushdown for free; cache hides it |
| Caching before a filter | You cache 100 GB to use 1 GB. Filter first. |
| Caching everything "just in case" | OOM, eviction storms, slower jobs |
| Forgetting `unpersist()` | Memory leaks across jobs in a long-running session |
| Caching a DataFrame that's only used once | Pure overhead |

## How to verify it cached

```python
df.cache()
df.count()  # materialize the cache (lazy!)

# In the Spark UI → Storage tab, see the RDD listed with size and fraction cached.
# In code:
print(df.storageLevel)  # StorageLevel(...)
print(df.is_cached)     # True / False (RDD API; on DF: df.rdd.isCheckpointed etc.)
```

**Caching is lazy.** `df.cache()` just sets a flag. The data is only stored when the next action runs. So always pair `.cache()` with a triggering action (`count()` is the canonical one).

## Eviction

When memory fills up, Spark evicts LRU. For `MEMORY_AND_DISK`, evicted blocks spill to disk; on `MEMORY_ONLY`, they're recomputed from lineage. Eviction patterns to watch for in the UI:
- "Fraction cached: 60%" → 40% gets recomputed every access. Often slower than not caching at all.
- Many spilled blocks → consider reducing the cached dataset (filter, project) or using a larger executor.

## Checkpointing — caching's bigger sibling

`df.checkpoint()` writes the DataFrame to durable storage (HDFS/S3) and **truncates lineage**. Used for:
1. **Iterative jobs** where the DAG grows unbounded (ML, graph). Spark planning slows down with deep lineage.
2. **Recovery** — checkpoint survives executor loss; cache doesn't (without replication).

```python
spark.sparkContext.setCheckpointDir("s3://my-bucket/checkpoints/")
df = df.checkpoint(eager=True)
```

Use `eager=True` to materialize immediately. Without it, the checkpoint happens on next action.

[HPS Ch.5 §"Checkpointing"]

## Cache vs checkpoint vs write/read

| Need | Use |
|---|---|
| Reuse 2–5× in a single job | `cache()` |
| Reuse across long-running session | `cache()` + remember to `unpersist()` |
| Iterative ML/graph job with growing DAG | `checkpoint()` |
| Reuse across separate Spark applications | `write.parquet()` then read |
| Snapshot you can restart from after a crash | `checkpoint()` or write |

## Scale notes

- **Compression effect**: a 50 GB Parquet table → ~150–200 GB cached (decompressed UnsafeRow). Many people forget this and OOM.
- **Cache hit ratio**: aim for >95% cached in the UI Storage tab. If you can't fit, you may need `MEMORY_AND_DISK_SER`, a bigger cluster, or aggressive filtering first.
- **Hot path optimization**: caching a hot lookup table joined many times is one of the highest-ROI optimizations. Couple with broadcast for small ones.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Cached but seemed not to help | Forgot the triggering action; cache is lazy | `.count()` after `.cache()` |
| Driver OOM after cache | Used `collect()` instead of `cache()` | Use cache, not collect |
| "Fraction cached: 60%" | Not enough executor memory | Filter/project before caching, scale up, or `_SER` |
| Old data after `.cache()` | Source updated underneath | `unpersist()` and re-cache; cache doesn't refresh |
| Same job slow on rerun | Lineage truncation needed | Use `checkpoint()` instead |
| Memory leak across jobs | `unpersist()` never called | Call it explicitly or use `try/finally` |

## References

- 📺 [Caching and Persistence in Spark — Databricks Academy](https://www.youtube.com/results?search_query=spark+caching+persistence+databricks)
- [LS Ch.7 §"Caching and Persistence of Data"]
- [HPS Ch.5 §"Caching, Persistence, and Checkpointing"]
- [DAS Ch.4 §"Caching in Spark"]
