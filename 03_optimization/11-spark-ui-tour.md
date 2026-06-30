# 11 — Spark UI tour

## Why this matters

The Spark UI is where you go to figure out *why* a job is slow. Every other note in this module sends you here eventually. This is the field guide.

Access:
- **Local mode**: `http://localhost:4040`
- **Databricks**: the "View" link next to a job in the notebook.
- **EMR/HDInsight/K8s**: through the Spark History Server, usually on port 18080.

## The tabs

| Tab | Use it to |
|---|---|
| **Jobs** | Overview: which jobs ran, how long, how many stages, did anything fail |
| **Stages** | Per-stage drill-down: task distribution, shuffle metrics, the histograms that catch skew |
| **Storage** | What's cached, how much, hit ratio |
| **Environment** | All Spark configs in effect; great for "why does my override not apply" |
| **Executors** | Per-executor memory/CPU/task counts; spot a hot or dead executor |
| **SQL / DataFrame** | Per-query DAG + executed plan + per-node metrics. **The most important tab.** |
| **Structured Streaming** | Batch timing, input rate, processing rate (only present if streaming) |

## Jobs tab

Each row = one action (`count`, `write`, etc.). Click into a job to see its stages.

- **Duration** — wall clock. Compare across runs.
- **Tasks (succeeded/total)** — failures show here first.
- **Description** — auto-filled with code line if available.

Jobs with `skipped` stages have benefited from caching/whole-stage reuse — that's good.

## Stages tab — the most-read page

Click any stage to see the per-task breakdown. This is where you find skew, spill, and stragglers.

### The task summary table

| Metric | Healthy | Red flag |
|---|---|---|
| Duration: Min / Median / Max | Max ≤ 2× Median | Max ≫ Median → straggler/skew |
| Shuffle Read Size: Min / Median / Max | Max ≤ 2× Median | Max ≫ Median → skew |
| Shuffle Write Size | 50–200 MB/task | Larger → coalescing too much |
| Spill (Memory) | 0 | > 0 → executor too small or partitions too few |
| Spill (Disk) | 0 | > 0 → same |
| GC Time | < 10% of duration | > 25% → too little memory or too many cores |
| Input Size | 100–200 MB/task | Larger → split inputs |

The **percentile histogram** at the top of the page is your best skew detector. A pancake-flat curve = balanced. A spike at the right = stragglers.

### Event timeline

Per-task timeline showing scheduling delay, deserialization, run time, GC, shuffle write. Look for:
- **Long red bars** (GC) → memory pressure.
- **Long blue scheduling delays** → fewer executors than tasks, or external shuffle issues.
- **Gaps** → driver bottleneck (planning, broadcasts).

## SQL tab — the cockpit

For every DataFrame action, the SQL tab has a *graphical* DAG with metrics on each node.

### What to look at

| Node | Metric | Meaning |
|---|---|---|
| Scan parquet | "files read", "bytes read" | Pushdown working? Compare to total table size |
| Filter | "number of output rows" | Selectivity: how much did this filter prune? |
| Project | "output rows" | Should equal input rows |
| Exchange | "data size", "shuffle records" | The shuffle volume — biggest cost number on the page |
| HashAggregate | "spill size" | If > 0, exec memory short |
| BroadcastExchange | "data size" | The broadcast size — should be small |
| SortMergeJoin | "build side rows", "stream side rows" | Confirms which side is which |
| AQEShuffleRead | "partitions" | After AQE coalesce |

### Reading the executed plan

The SQL tab's "Details" section shows the actual physical plan that ran (post-AQE). This is what you want, not `df.explain()`'s pre-execution plan.

For "why is this so slow", **work right-to-left** along the DAG looking for the operator with the biggest time/output ratio.

## Executors tab

Per-executor stats:
- **RDD Blocks / Storage Memory** — caching footprint.
- **Active Tasks** — should match `executor.cores`; if always 0 for some, executor is broken.
- **Failed Tasks / Total Tasks** — high ratio = a flaky node.
- **GC Time / Task Time** — > 10% = trouble.

Hot executor = receiving more partitions, often because of skew. Dead executor = check logs.

## Storage tab

Lists every cached RDD/DataFrame:
- **Fraction Cached** = `cached_in_memory / total`. Want ≥ 95%.
- **Size in Memory / Disk** — distribution after spill.
- **Storage Level** — confirms the level you intended.

If Fraction Cached is low, your cache is constantly being evicted/recomputed. Either fit more in memory, cache less, or accept it.

## Structured Streaming tab

Per-query metrics:
- **Input Rate** — rows/sec from sources.
- **Process Rate** — rows/sec consumed.
- **Batch Duration** — should be < trigger interval.
- **Watermark** — lag behind event time.

If Process Rate < Input Rate consistently, you're falling behind. Scale up or simplify.

## Field-guide checklist for "this job is slow"

1. **Jobs tab**: which job/stage took the longest?
2. **SQL tab** on that query: find the node with the largest time. Right-to-left, biggest reader/exchange.
3. **Stages tab** for that stage:
   - Max vs Median task time → skew?
   - Spill (Memory/Disk) → memory issue?
   - GC Time → too much GC?
   - Shuffle read/write size → reduce shuffle?
4. **Executors tab**: any executor with disproportionate load or failures?
5. **Environment**: confirm the Spark configs you *thought* you set are actually in effect.

[LS Ch.7 §"Inspecting the Spark UI"]

## Failure modes (often-overlooked)

| Symptom | Cause | Fix |
|---|---|---|
| Stage has 1 task | Forgot to partition; or `coalesce(1)` upstream | Add `repartition` |
| UI says executor lost | Container kill, network partition | Logs in YARN/K8s; bump memory overhead |
| Always the same task # is slowest | Locality issue or skew on key%N | Repartition with different N |
| SQL tab missing for a job | Not a Spark SQL job (raw RDD) | Use DataFrame API to see it |
| Configs show wrong values | Set after `SparkSession` already started | Set before `getOrCreate`, or via spark-submit |

## References

- 📺 [Spark UI Deep Dive — Databricks Academy](https://www.youtube.com/results?search_query=spark+ui+deep+dive+databricks)
- 📺 [The Spark UI Tour — Holden Karau](https://www.youtube.com/results?search_query=spark+ui+holden+karau)
- [LS Ch.7 §"Inspecting the Spark UI"]
- [HPS Ch.7 §"Profiling Spark Applications"]
- [Cert Guide] — heavy UI screenshots; expect questions
