# Case Study - Small Files

## Symptoms

- Reads are slow despite small total data size.
- Spark creates too many tasks.
- Cloud storage listing takes too long.
- Delta table metadata grows quickly.

## Root Cause

Many tiny files create scheduling overhead, metadata overhead, and inefficient scans. This often happens after many small streaming micro-batches or over-partitioned writes.

## What To Check First

```bash
find output -type f | wc -l
```

In Spark or Databricks, inspect table file counts and average file size.

## Fix Options

- Compact files.
- Reduce partition cardinality.
- Write fewer output partitions with `coalesce` when appropriate.
- Use optimized writes or auto compaction where available.
- Choose partition columns based on query pattern and data volume.

## Prevention

- Avoid partitioning by high-cardinality columns.
- Avoid one output file per small group.
- Monitor file count and average file size.

## Interview Explanation

"The small file problem is a metadata and scheduling problem. Spark may spend more time listing and opening files than processing data. I would measure file count and average size, compact existing files, and change the write strategy to prevent recurrence."
