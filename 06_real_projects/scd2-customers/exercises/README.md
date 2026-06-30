# scd2-customers exercises

---

## 1. Add a surrogate key

Add a `customer_key BIGINT` column that:
- Is unique per row (different versions of the same customer get different `customer_key`s).
- Is monotonically increasing globally.
- Is used by fact tables to "join to the version current at the time of the fact".

Use `F.monotonically_increasing_id()` carefully — it's not stable across recompute.

---

## 2. Handle deletes

If a customer is removed from the source feed, your current pipeline doesn't notice. Add a delete-detection step:

- Detect via `whenNotMatchedBySourceUpdate` (close out customers not in the source).
- OR add an `is_active` column to source and process delete events explicitly.

What if the source is a partial feed (only changes)? Then `whenNotMatchedBySource` would close active customers incorrectly. How do you tell the two apart?

---

## 3. Add a "current_X" Type 6 column

For `country` and `tier`, add `current_country` and `current_tier` columns to *every row in history*. When alice changes country, update ALL of alice's historical rows to reflect the new current_country.

Then show how a "fact joined to dim using current customer_country" query works without a `where is_current` filter.

---

## 4. Performance: Z-ORDER

Run the demo at scale (1M customers, 5M historical rows). Then:
- Time a point-in-time query.
- Run `OPTIMIZE ... ZORDER BY (customer_id)`.
- Time the same query again.

Document the improvement.

---

## 5. Late-arriving data

The pipeline as written compares `source_updated_at` against current row. But what if data arrives out of order — a 2024-03-01 update lands AFTER a 2024-06-01 update?

- Modify the merge to handle out-of-order arrivals correctly.
- The CLOSE/INSERT logic must place the new row in the correct historical position.
- This is the hardest case in SCD2; don't expect a one-line fix.

---

## 6. Backfill from a CDC log

You're handed a Kafka topic of 6 months of CDC events. Implement:
- Read all CDC events in event-time order.
- Apply them to the SCD2 table one batch at a time.
- The final SCD2 table should reflect every change correctly.

Tip: sort by `(customer_id, source_updated_at)` first.
