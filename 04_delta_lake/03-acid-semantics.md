# 03 — ACID semantics in practice

## Why this matters

Delta gives you ACID, but ACID-on-an-object-store has corners that ACID-on-a-database doesn't. Knowing where the corners are saves a 3am page.

## Atomicity — what's actually atomic

**One commit = one log file write.** Whatever set of `add`/`remove` actions is in that JSON is either fully visible or fully invisible.

But: the *Parquet file writes* happen *before* the log write. If your job crashes after writing 100 Parquet files but before the log commit, those 100 files are orphaned (still on disk, but no log entry refers to them). VACUUM cleans them up eventually.

```
1. Write Parquet files                 <- still invisible to readers
2. Write _delta_log/N.json (atomic)    <- the "commit" point
3. Now visible                          <- readers may see new version
```

The reverse — log entry exists but files don't — is impossible: log entry is written last.

## Consistency

Delta enforces:
- **Schema** matches the `metaData` action.
- **CHECK constraints** if defined on the table.
- **NOT NULL** on columns marked non-nullable.
- **Protocol versions** (reader/writer compat).

What it doesn't enforce:
- Foreign keys between tables.
- Uniqueness (no UNIQUE constraint).
- Cross-table consistency.

You can layer your own validations with `expect`-style data quality tooling (Delta Live Tables, Great Expectations, Soda).

## Isolation — the only level: serializable... mostly

Delta uses **optimistic concurrency control** (OCC). Two writers don't lock the table; they prepare their writes in parallel and try to commit. Conflicts are detected when the second writer attempts version `N+1` that already exists.

### Conflict detection

When two writers race:

| Both writes... | Result |
|---|---|
| Touched disjoint files (e.g. different partitions) | Both succeed |
| Touched the same file (read-modify-write) | Second writer must retry against the new state |
| Both performed metadata changes (schema, properties) | One succeeds, other gets `MetadataChangedException` |
| Insert-only appends with no read | Almost always succeed concurrently |

Delta validates conflicts using the **read set** (what the writer read to compute its change) vs the **write set** of competing commits.

### The isolation guarantees

- **Snapshot Isolation by default** for writes — a writer's view is a consistent snapshot of the table at the time it started.
- **Serializable** for writes that conflict — OCC + retry ensures serial order at the file level.
- Readers always see one consistent snapshot — never a half-written commit.

### `WriteSerializable` vs `Serializable`

| | `WriteSerializable` (default) | `Serializable` |
|---|---|---|
| Two appends (no conflict) | Always succeed concurrently | Same |
| Append + update on disjoint rows | Append always allowed | Update may invalidate the append |
| Speed | Faster (fewer retries) | Slower for concurrent updates |
| Risk | Theoretically not strictly serializable | Always serial |

Default `WriteSerializable` is fine for nearly everyone. Use `Serializable` only when you have a strong invariant that must hold across concurrent writers (rare).

## Durability

Delta inherits durability from the underlying storage. S3 / ADLS / GCS / HDFS all give "11 nines" durability. Delta adds replication of metadata in the log itself; the JSON commit *is* the durable record.

What durability does NOT cover:
- Accidental `DROP TABLE` — gone unless you have storage versioning enabled.
- VACUUM with `RETAIN 0 HOURS` — deletes everything; time travel broken.
- Bucket lifecycle policies that expire old files — silent data loss if you also have old commits referencing them.

## How concurrent writes look in code

```python
# Two jobs running simultaneously — append to disjoint partitions
# Job A:
df_us.write.format("delta").mode("append").save("/tables/events")
# Job B (different process):
df_de.write.format("delta").mode("append").save("/tables/events")

# Both succeed. Log will show two commits: version N+1 and N+2 (in some order).
```

```python
# Two MERGE statements on overlapping keys — conflict
# Job A:
DeltaTable.forPath(spark, "/tables/users") \
    .alias("t") \
    .merge(updates_a.alias("s"), "t.id = s.id") \
    .whenMatchedUpdateAll() \
    .execute()
# Job B at the same time, same keys: gets ConcurrentAppendException, retries.
```

## Reading mid-write

```python
# At T=0, writer starts: reads version 5, computes new files.
# At T=1, reader queries: sees version 5 (writer hasn't committed yet).
# At T=2, writer commits version 6.
# At T=3, reader queries again: sees version 6.

# Reader at T=1 NEVER sees a partial mix of version 5 + 6. Snapshot isolation.
```

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `ConcurrentAppendException` | Two writers touched overlapping partitions | Re-run; partition more granularly; or serialize |
| `ConcurrentDeleteReadException` | Reader's view files were deleted by a parallel optimization | Re-read; or run OPTIMIZE in a maintenance window |
| `MetadataChangedException` | Schema change while you were writing | Coordinate schema changes; retry job |
| `ProtocolChangedException` | Table protocol was upgraded mid-flight | Upgrade your Delta client; re-run |
| Stuck commit (S3 backend) | Failed conditional put, partial state | Wait for transient backend issue; retry |
| "I see old data after writing" | Cached metadata; or read from a cached path | `spark.sql("REFRESH TABLE ...")` |

## References

- *Delta Lake: The Definitive Guide* — Ch.3 "ACID Transactions"
- Delta concurrency docs: https://docs.delta.io/latest/concurrency-control.html
- [LS Ch.9] — coverage of ACID in the Spark context
- 📺 [Delta Lake Concurrency Control — Databricks](https://www.youtube.com/results?search_query=delta+lake+concurrency+control+databricks)
