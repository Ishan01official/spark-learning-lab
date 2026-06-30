# dedup-at-scale exercises

---

## 1. Understand the skew

Run `run_dedup.py` and look at the Spark UI Stages tab during the `row_number` strategy.
- What is Max vs Median task duration?
- Is the hot key consistent with the ratio you expect?

---

## 2. AQE skew-join handling

Enable AQE skew handling and disable manual salting. Re-run the `row_number` strategy. Does AQE help with `groupBy`-style operations? (Hint: it's *join-skew* AQE handles, not aggregation-skew.)

---

## 3. Conflict handling

The 1% "same event_id, different payload" rows: which one wins under each strategy?
- For `dropDuplicates`, the winner is arbitrary.
- For `row_number`, latest `event_time` wins.
- What if you wanted a deterministic tiebreaker (e.g., lexicographic on payload)?

---

## 4. Try a bloom filter pre-filter

For workflows where the input is mostly already-seen events, use a bloom filter to quickly drop known-seen keys:

```python
seen_bloom = (target.select("event_id")
                  .stat
                  .approxDistinct("event_id"))  # or build a real bloom
```

Implement the pattern: filter new events against a bloom of recent target keys, then do the expensive merge. Measure the speedup when 95% of new events are dupes of recent ones.

---

## 5. Convert to streaming dedup

Convert `strategy_salted` to a streaming pattern:
- Source: a streaming events source (rate or file).
- Sink: a Delta table after dedup.
- Use `dropDuplicates` with a watermark to bound state.

What's the trade-off vs the batch salted version?
