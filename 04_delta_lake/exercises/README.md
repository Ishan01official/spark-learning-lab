# Module 04 — Delta Lake exercises

---

## 1. Inspect the transaction log by hand

Run `examples/01_create_and_write_delta.py`. Then:

- `ls /tmp/delta_demo/orders/_delta_log/` — count the JSON files.
- `cat 00000000000000000000.json | python -m json.tool` — what `add` actions are there?
- Open one JSON from a later commit and find the `add` and `remove` actions. Which Parquet files did the latest commit retire?
- Find the `commitInfo` for the most recent write. What operation, what user, what mode?

Write up: a one-paragraph description of "what happened on each commit".

---

## 2. Force a `ConcurrentAppendException`

Open two terminals. In both, start a Python REPL that opens a Spark session pointed at the same Delta table on the *same path* (use a managed local table is fine).

- Have both write `df.write.format("delta").mode("append").save(path)` of small DataFrames at the same time. Most times, both succeed.
- Now have both write with `replaceWhere` covering the same partition. One should fail with `ConcurrentAppendException`.
- Why does the first case succeed and the second fail?

---

## 3. Idempotent retries

Write a function `safe_lambda_write(batch_df, batch_id, path)` that:

- Writes `batch_df` to `path` as Delta in a way that retrying with the same `batch_id` is a no-op.

Use either:
- `txnAppId` + `txnVersion`, or
- `replaceWhere` with a `batch_id` partition.

Test by calling it three times with the same arguments. Assert that the table has exactly one batch's worth of rows.

(This is the same pattern as Ishan's supplier-terms-enriched Lambda — keep it.)

---

## 4. MERGE performance: with and without Z-ORDER

Generate a 100M-row Delta table with a `user_id` (cardinality 1M).

- Run a MERGE that updates 1000 rows by `user_id`. Time it.
- Run `OPTIMIZE ZORDER BY (user_id)`. Run the same MERGE again. Time it.
- In the Spark UI SQL tab, find the FileScan and compare "files read" before and after Z-ORDER.

How much improvement? What's the cost-benefit if MERGE is run hourly vs daily?

---

## 5. Recover from "VACUUM RETAIN 0 HOURS"

Imagine someone ran `VACUUM ... RETAIN 0 HOURS` and now `versionAsOf 5` returns "file not found".

- Demonstrate the failure: in a sandbox table, create 10 versions, run `VACUUM RETAIN 0 HOURS`, then try to read version 0.
- What's recoverable? (Answer: nothing without backup.)
- Document the four guardrails you'd put in place to prevent this in production. Cite specific Delta configs.

---

## 6. Compare table format claims

For each of these claims, decide if it's true for Delta, Iceberg, both, or neither. Cite a source.

1. "Partition columns can be changed without rewriting data."
2. "Time travel by timestamp or version number is supported."
3. "Optimistic concurrency control is used for writers."
4. "The metadata is queryable as a table."
5. "Native ACID guarantees on plain Parquet, no special files needed."
6. "Built-in Change Data Feed."
7. "Stream-first / merge-on-read by default."

---

## 7. SCD2 pipeline robustness

Take `examples/04_scd2_merge.py` and harden it:

- What happens if a `customer_id` appears twice in one input batch?
- What happens if the same batch is applied twice (late retry)?
- What happens if `event_time` is null?

Add input validation and dedup steps. Document the invariants the pipeline maintains.

---

## 8. Design a maintenance schedule

You operate a Delta table:
- 500 GB total, growing 5 GB/day.
- 200 streaming writers, each appending 1 MB every 30 s.
- Queried interactively with `WHERE event_date AND user_id IN (...)` constantly.

Design a maintenance schedule. Specify:
- How often you run `OPTIMIZE`.
- Whether you use `ZORDER BY` or liquid clustering.
- What `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` to set.
- When you run `VACUUM`.
- What you monitor to detect "things are degrading".

Defend the choices with the trade-offs (cost vs query latency vs time-travel reach).
