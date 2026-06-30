# orders-etl exercises

---

## 1. Verify idempotency

Run `01_ingest_bronze --batch-date 2024-09-15` three times in a row. Confirm:
- The row count for that batch_date stays the same.
- The Delta history has new commits each time, but no duplicate data.

How does `replaceWhere` make this idempotent?

---

## 2. Add a quarantine for malformed records in bronze

Currently, bronze keeps malformed records as raw text. Modify it so that:
- Records that fail a basic structural check (have `order_id` after JSON parse) go to a `bronze.dead_letter` table.
- Bronze itself only contains records that *might* be valid JSON orders.

Justify this design choice. (Hint: it's a trade-off between "bronze is faithful" and "downstream silver doesn't need to handle garbage".)

---

## 3. Convert bronze → silver to streaming `availableNow`

Rewrite `02_build_silver` as a `availableNow` streaming query:
- Source: Delta stream from bronze.
- Sink: foreachBatch + MERGE.
- Run as a one-shot job that processes only new bronze data.

Compare runtime to the batch version on the same data. When would you prefer one over the other?

---

## 4. Add OPTIMIZE / VACUUM maintenance

Write a `04_maintenance.py` job that:
- Runs `OPTIMIZE` on silver (compact and Z-ORDER by `customer_id`).
- Runs `VACUUM` on silver with a 7-day retention.
- Logs the number of files before and after.

Schedule this conceptually: hourly? daily? weekly? Defend the choice.

---

## 5. Backfill silver from a specific bronze version

A downstream user reports silver had bad data between 2024-09-15 09:00 and 11:00. You suspect a buggy version of the silver job.

- Find the silver version from that period using `dt.history()`.
- Time-travel silver to before the bad version (`RESTORE TO VERSION`).
- Re-run the silver build job from the bronze data.
- Verify the silver row counts match expectation.

Document the runbook for "fix bad silver".

---

## 6. Add a data quality check on gold

Add to `03_build_gold` a validation step:
- Total revenue today is within ±20% of the 7-day average.
- No region has 0 orders.
- The top SKU represents < 30% of total revenue (anti-monopoly check).

On failure, log a warning. Decide: do you fail the job, or write the gold and alert? Why?

---

## 7. Stress test

Modify `gen_data.py` to produce 1M rows/day across 30 days. Re-run the pipeline. Measure:
- Per-stage runtime.
- Number of shuffle partitions used.
- Bronze, silver, gold storage size.

Tune `spark.sql.shuffle.partitions` for this size. Add `OPTIMIZE` calls where needed.
