# 03 — Data quality patterns

## Why this matters

If bad data reaches gold, dashboards lie. If bad data fails the pipeline, work stops. The choice between *fail fast* and *quarantine* is the central data-quality decision; this note covers both, plus the framework to implement them.

## Three categories of "bad data"

1. **Malformed** — can't parse. Bad JSON, missing required column, wrong type.
2. **Schema-valid but illegal** — parses fine but violates a constraint. Negative amount, future date, NULL where required.
3. **Statistically anomalous** — within constraints but suspicious. Sudden volume spike, distribution shift, missing partition.

Each requires a different response.

## Fail fast vs quarantine

| | Fail fast | Quarantine |
|---|---|---|
| Bad row → | job fails | bad row routed to dead-letter table |
| Pipeline | stops | continues |
| Use when | data must be correct (finance, compliance) | acceptable to drop a percentage (clickstream) |
| Alerting | immediate, per-error | scheduled, on bad-row rate threshold |
| Recovery | block until fixed | fix dead-letter when convenient |

In practice: bronze quarantines, silver fails fast. The medallion layers naturally separate these.

## Implementing fail-fast

### Schema enforcement at the source

```python
schema = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("amount",   DecimalType(18, 2), nullable=False),
    StructField("status",   StringType(), nullable=False),
])

(spark.read.json("input/", schema=schema, mode="FAILFAST")
    .write.format("delta")
    .save("silver_orders"))
```

`mode="FAILFAST"` raises on the first bad row. Loud and clear.

### Delta CHECK constraints

```sql
ALTER TABLE silver_orders ADD CONSTRAINT amount_positive CHECK (amount > 0);
ALTER TABLE silver_orders ADD CONSTRAINT status_valid CHECK (status IN ('NEW','PAID','SHIPPED'));
```

Violations fail the write. Pipeline stops; alert fires.

### Assertion-based

```python
def assert_no_nulls(df, cols):
    n = df.where(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in cols])).count()
    if n > 0:
        raise ValueError(f"NULL values in {cols}: {n} rows")

assert_no_nulls(silver_df, ["order_id", "amount", "status"])
```

Cheap, explicit. Pairs well with a `validate_silver_orders.py` step in your DAG.

## Implementing quarantine

### Pattern: split good and bad

```python
parsed = bronze.withColumn(
    "_payload",
    F.from_json(F.col("value"), schema)
)

good = parsed.filter("_payload IS NOT NULL").select("_payload.*", "kafka_ts")
bad  = parsed.filter("_payload IS NULL").select("value", "kafka_ts",
                                                  F.lit("malformed_json").alias("reason"))

good.write.format("delta").mode("append").save("silver_events")
bad.write.format("delta").mode("append").save("dead_letter_events")
```

The dead-letter table is just another Delta table; you can query, alert on, and replay it.

### Pattern: tag and route

```python
# Add quality flags to every row; downstream consumers can filter
tagged = silver.withColumn(
    "_dq_flags",
    F.array(
        F.when(F.col("amount") <= 0, "amount_non_positive"),
        F.when(F.col("status").isNull(), "status_null"),
        F.when(F.col("created_at") > F.current_timestamp(), "future_timestamp"),
    )
).withColumn(
    "_dq_pass",
    F.size(F.filter(F.col("_dq_flags"), lambda x: x.isNotNull())) == 0
)

# Gold consumes _dq_pass = true
```

Keeps everything in one table; consumers choose the strictness.

## Monitoring rate-based quality

Some metrics aren't about individual rows but rates:

```python
today = spark.read.format("delta").load("silver_orders") \
    .filter(F.col("event_date") == today_date)

count_today = today.count()
expected = expected_volume_for(today_date)

if count_today < 0.5 * expected:
    alert("orders volume dropped 50%")
if today.filter("_dq_pass = false").count() / count_today > 0.05:
    alert("bad-row rate exceeded 5%")
```

Track these as metrics in your monitoring system; alert on thresholds.

## Where to put quality checks

Three places, in order of preference:

1. **At ingest, on the source schema.** Best because failure is closest to the cause.
2. **As Delta CHECK constraints.** Persistent; enforced on every future write.
3. **As post-write validation.** Run after the table is built; if it fails, alert or roll back.

A typical job sequence:

```
parse(bronze) -> validate_schema -> apply_dq_rules
              -> write(silver)    -> verify_invariants_after_write
              -> publish_metrics  -> alert_if_threshold
```

## A reusable check library

Code can be kept simple:

```python
from dataclasses import dataclass
from typing import Callable

@dataclass
class DQRule:
    name: str
    predicate: Callable  # df -> Column (True = passes)
    severity: str        # "error" or "warn"
    threshold: float = 0.0   # max fraction failing before raising

def apply_rules(df, rules):
    failures = {}
    total = df.count()
    for r in rules:
        n_fail = df.filter(~r.predicate(df)).count()
        rate = n_fail / total if total else 0
        if rate > r.threshold:
            failures[r.name] = (n_fail, rate, r.severity)
    return failures

# Usage
RULES = [
    DQRule("amount_positive",  lambda df: F.col("amount") > 0,            "error"),
    DQRule("status_valid",     lambda df: F.col("status").isin(VALID_STATUSES), "error"),
    DQRule("email_format",     lambda df: F.col("email").rlike(EMAIL_RE), "warn", 0.01),
]
```

For more, consider:
- **Delta Live Tables (Databricks)**: built-in `EXPECT` declarations.
- **Great Expectations**: open-source DQ framework.
- **Soda Core**: declarative YAML-based checks.
- **dbt tests**: if your stack includes dbt.

## Anti-patterns

| Don't | Why |
|---|---|
| Silently drop bad rows | You'll never notice the upstream broke |
| Fail fast in bronze | Then you can't even capture the bad data for diagnosis |
| Validate only nullability | The interesting bugs are referential, statistical, temporal |
| Run validation as a side script | It will get skipped on retry; embed in the pipeline |
| Manual validation only | Will not scale; codify the rules |

## Failure modes (for the quality layer itself)

| Symptom | Cause | Fix |
|---|---|---|
| Dead-letter table grows unboundedly | Source schema drift; no one watching the DLT | Alert on DLT growth rate |
| Quality check expensive on big data | Counting + filtering is O(N) | Sample; do exact only when sample fails |
| Constraint added on existing table fails | Existing rows violate it | Backfill / cleanse first, then add |
| Validation passes but data still wrong | Rules don't cover the failure mode | Add rules every time you find a new bug |
| Alert fatigue | Every rule fires daily | Tighten thresholds; group by category |

## References

- *Fundamentals of Data Engineering* (Reis & Housley) — Ch.9 on data quality
- Great Expectations docs: https://docs.greatexpectations.io/
- Delta Live Tables docs (for `EXPECT` syntax)
- 📺 [Data Quality in Modern Data Pipelines — Databricks](https://www.youtube.com/results?search_query=delta+live+tables+data+quality)
