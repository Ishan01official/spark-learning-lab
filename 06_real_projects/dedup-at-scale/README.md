# Project: dedup-at-scale

Deduplicating a large, skewed event stream.

## What this project demonstrates

- Three deduplication strategies, with the same input, comparing correctness and speed.
- How key skew breaks the naive approach.
- Salting as the standard fix.
- Using bloom filters as a pre-filter.

## The setup

Generate 10M events where:
- 5% are exact duplicates (same `event_id`, same payload).
- 1% have the same `event_id` but different payloads ("conflict").
- 1 hot key represents ~30% of all events (skew).

Goal: produce a deduplicated table where each `event_id` appears once, with the *latest* `event_time` winning.

## Strategies

### Strategy 1: `dropDuplicates`

```python
events.dropDuplicates(["event_id"])
```

Simple, but loses the "latest wins" semantic — it picks an arbitrary row per key.

### Strategy 2: `row_number()` window + filter

```python
w = Window.partitionBy("event_id").orderBy(F.desc("event_time"))
events.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
```

Correct semantic. But for skewed keys, one task gets all rows for the hot key.

### Strategy 3: Salted window + final dedup

```python
# Stage 1: salt + local dedup
salted = (events
    .withColumn("salt", (F.rand() * N_SALTS).cast("int"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("event_id", "salt").orderBy(F.desc("event_time"))))
    .filter("rn = 1")
    .drop("rn", "salt"))

# Stage 2: final dedup, now manageable
final = (salted
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("event_id").orderBy(F.desc("event_time"))))
    .filter("rn = 1")
    .drop("rn"))
```

Two passes, but balanced. The hot key now distributes across N_SALTS partitions in stage 1.

## Files

- `run_dedup.py` — generates the skewed dataset, runs all three strategies, prints timings.

## How to run

```bash
python run_dedup.py
```

## Expected results

For 10M events on a 4-core local mode:
- Strategy 1: ~30s (arbitrary winner)
- Strategy 2: ~60s (one straggler task)
- Strategy 3: ~25s (balanced)

## When to use which

| Strategy | Use when |
|---|---|
| `dropDuplicates` | You don't care which dup wins; low skew |
| `row_number` | Latest-wins semantic, low skew |
| Salted two-pass | Latest-wins, known skewed key |
