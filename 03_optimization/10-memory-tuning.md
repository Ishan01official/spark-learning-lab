# 10 — Memory tuning

## Why this matters

OOM kills are the most common Spark failure. Most of them come from misunderstanding executor memory layout — people give Spark 16 GB and wonder why a 4 GB DataFrame OOMs. This note shows the layout and the knobs.

## Executor memory layout

```
+-----------------------------------------------------+ spark.executor.memory  (e.g. 16 GB)
|  Reserved memory             (~300 MB)              |
+-----------------------------------------------------+
|  User memory  (1 - spark.memory.fraction) × usable  |
|    UDF state, broadcast vars (deserialized),        |
|    user data structures                             |
+-----------------------------------------------------+
|  Unified memory (spark.memory.fraction × usable)    |
|    +- Storage region                                |
|    |   (cached blocks, broadcasts)                  |
|    +- Execution region                              |
|        (shuffle, aggregation, sort, join hash)      |
|    storage:execution boundary moves dynamically     |
+-----------------------------------------------------+
+-----------------------------------------------------+ spark.executor.memoryOverhead
|  Off-heap memory                                    |
|    JVM overhead, Python workers (PySpark!),         |
|    direct buffers, container overhead               |
+-----------------------------------------------------+
```

Container total = `spark.executor.memory + spark.executor.memoryOverhead`. YARN/K8s kills the container if it exceeds this.

[LS Ch.7 §"Memory Management"], [HPS Ch.7 §"Memory and Garbage Collection"]

## The knobs

| Setting | Default | What it controls |
|---|---|---|
| `spark.executor.memory` | 1g | JVM heap for the executor |
| `spark.executor.memoryOverhead` | max(384MB, 10% of memory) | Off-heap reserve (Python workers, direct buffers) |
| `spark.memory.fraction` | 0.6 | Fraction of (heap - 300MB) for unified memory |
| `spark.memory.storageFraction` | 0.5 | Fraction of unified that's storage-reserved (won't evict below this) |
| `spark.memory.offHeap.enabled` | false | Use off-heap for Tungsten |
| `spark.memory.offHeap.size` | 0 | How much off-heap if enabled |
| `spark.executor.cores` | varies | Cores per executor (= concurrent tasks) |
| `spark.driver.memory` | 1g | Driver JVM heap |
| `spark.driver.maxResultSize` | 1g | Hard cap on `collect()` size |

## PySpark special concerns

In PySpark, each executor spawns one Python worker process per core. Each worker has its own memory, **separate from the JVM heap**. That's why `executor.memoryOverhead` matters so much for PySpark: it's where Python lives.

Rule of thumb for PySpark:
- `executor.memoryOverhead` ≥ 20–30% of `executor.memory`.
- For heavy Pandas UDFs: 50%+ overhead, since each Pandas UDF batch lives in Python memory.

```python
# A reasonable PySpark executor
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "3g")  # 8g × 37%, generous for Pandas UDF
spark.conf.set("spark.executor.cores", 4)
```

## Sizing executors

Three common shapes:

| Shape | Memory | Cores | When |
|---|---|---|---|
| **Fat** | 32 GB | 8 | Heavy aggregations, big caches, joins |
| **Standard** | 16 GB | 4 | Default — most workloads |
| **Thin** | 8 GB | 2 | Many small tasks, low memory per task |

The classic constraint: **cores per executor ≤ 5**. Past that, HDFS/network IO contention overwhelms gains. Memory beyond ~64 GB per executor gives diminishing returns due to GC pauses.

For PySpark specifically: **fewer cores per executor is often better**, because each core = one Python worker = more memory pressure.

## When memory pressure kills you

| Symptom | Cause | Fix |
|---|---|---|
| `Container killed by YARN for exceeding memory limits` | Container exceeded `executor.memory + memoryOverhead` | Raise `memoryOverhead`; for PySpark, raise it a lot |
| `java.lang.OutOfMemoryError: Java heap space` | JVM heap exhausted | Raise `executor.memory`, lower partition size |
| `java.lang.OutOfMemoryError: GC overhead limit exceeded` | Spending > 98% in GC | Reduce cache, smaller partitions, fewer cores per executor |
| Tasks slow then suddenly all complete | GC pause — long pause then catchup | Smaller heap, G1GC, more executors |
| Driver `collect()` OOM | Pulled too much data to driver | Don't; write to storage instead |
| Cached fraction drops over time | Eviction; cache too big | Raise `memory.fraction`, or cache less, or scale up |

## Garbage collection tuning

Modern default is G1GC and usually fine. For very large heaps (>32 GB) or persistent pause issues:

```
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions \
  -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35"
```

For PySpark, GC matters less because most memory is in Python. For Scala/RDD workloads, GC is the boss.

## Dynamic allocation

```python
spark.conf.set("spark.dynamicAllocation.enabled", True)
spark.conf.set("spark.dynamicAllocation.minExecutors", 2)
spark.conf.set("spark.dynamicAllocation.maxExecutors", 50)
spark.conf.set("spark.shuffle.service.enabled", True)
```

Requires external shuffle service so executors can be removed without losing shuffle output. Lets clusters scale down between stages — saves cost in cloud, but adds startup latency at scale-up.

## Scale notes

- **1 GB input → ~3 GB executor memory** in flight, given decompression, intermediate dicts, shuffle buffers.
- **Caching adds ~3× input size** to memory pressure (decompressed UnsafeRow + replication overhead).
- **A 100M-row group-by with 1M unique keys** keeps roughly `1M × (key_size + agg_state)` in execution memory per executor. For a 64-key map and 5 doubles aggregated, ~600 MB. Plan accordingly.

## Failure modes (additional)

| Symptom | Cause | Fix |
|---|---|---|
| `Total size ... exceeds maxResultSize` | `collect()` returned > 1 GB | Don't `collect`; or raise `driver.maxResultSize` |
| Job runs but very slow shuffles | Frequent spill to disk | Raise `executor.memory` or `shuffle.partitions` |
| Wide variance in executor memory usage | Skew | Salt or AQE skew handling |
| Python worker killed mid-task | Pandas UDF OOM | Smaller `arrow.maxRecordsPerBatch`, raise `memoryOverhead` |
| Driver hangs at job start | Broadcasting too-big DataFrame | Lower threshold or don't broadcast |

## References

- 📺 [Apache Spark Memory Management — Daniel Tomes](https://www.youtube.com/results?search_query=spark+memory+management+daniel+tomes)
- [LS Ch.7 §"Memory Management"]
- [HPS Ch.7] — the deepest dive on this topic
- [DAS Ch.8 §"Memory Tuning"]
