# 01 — The medallion architecture (bronze / silver / gold)

## Why this matters

Every production data platform looks like this, even if it's not called "medallion":

```
raw landing -> bronze -> silver -> gold -> consumers
```

The pattern is so universal because it solves three problems at once: **separating ingest from transformation**, **isolating quality issues**, and **letting different consumers pick the right level of refinement** for their use case.

## The three layers

### Bronze — raw, append-only, schema-tolerant

- **Source**: whatever the upstream gives you (Kafka, S3 files, API dumps).
- **Format**: same as source if possible (JSON, Avro), otherwise Parquet with `STRING` columns for ambiguous fields.
- **Schema**: loose. Parse as little as you can get away with.
- **Writes**: append-only. Never edit, never delete.
- **Retention**: weeks to years — bronze is the cheapest insurance for "we need to reprocess".

The defining property: **bronze must be reproducible from the source**. If you lose silver and gold, you can rebuild them from bronze. If you lose bronze, you're in trouble.

### Silver — cleaned, validated, joined

- **Source**: bronze.
- **Format**: Delta, well-typed.
- **Schema**: strict — columns typed, constraints applied.
- **Writes**: MERGE / upsert, sometimes append.
- **Retention**: longer than bronze (this is where most queries actually hit).

Silver is where:
- Schemas are enforced.
- Bad rows are dropped or quarantined.
- Reference data is joined in (countries, products, users).
- Deduplication happens.
- Type 1 / Type 2 SCD logic runs.

### Gold — business-ready, aggregated, denormalized

- **Source**: silver.
- **Format**: Delta, often partitioned by query keys.
- **Schema**: domain-specific — "sales by region by day", "customer 360".
- **Writes**: rebuild or incremental refresh.
- **Retention**: as needed by consumers.

Gold is what BI tools, dashboards, ML feature stores, and external APIs read from. Multiple gold tables can be derived from the same silver, each tuned for a specific consumer.

[*Delta Definitive Guide* Ch.11]

## Why the separation matters

Consider a problem: a vendor changes the timestamp format in their JSON payload from `Z` to `+00:00`. Without the medallion:
- Your silver MERGE breaks because timestamps don't parse.
- Your downstream gold aggregations are stale.
- Your dashboards have a hole.

With medallion:
- Bronze keeps absorbing the raw JSON — it doesn't care about the timestamp format.
- Silver breaks. But you've quarantined the issue.
- Fix the silver parser. Reprocess silver from bronze. Gold rebuilds.

The pattern bounds the blast radius of upstream changes.

## Data flow patterns

### Pure batch

```
bronze (full reload daily)
  -> silver (rebuild from bronze)
    -> gold (rebuild from silver)
```

Simple, robust, but high latency. Common for "we need yesterday's data by 8 AM" workloads.

### Incremental

```
bronze (append-only)
  -> silver (MERGE based on a watermark / batch_id)
    -> gold (incremental aggregation, or scheduled rebuild)
```

Each layer reads only "new since last run". Requires watermark/batch_id tracking but scales much better.

### Streaming

```
Kafka
  -> bronze (Structured Streaming append)
    -> silver (Structured Streaming foreachBatch + MERGE)
      -> gold (Structured Streaming + windowed agg)
```

End-to-end seconds-of-latency. The hardest to operate. Use when you actually need the latency.

### Hybrid (most common)

```
Kafka -> bronze (streaming)
Bronze -> silver (scheduled batch every 15 min)
Silver -> gold (scheduled batch hourly)
```

Streaming where you need it, batch where you don't. Pragmatic.

## What you record in each layer

Common columns to keep at every layer for traceability:

| Column | Bronze | Silver | Gold |
|---|---|---|---|
| `source_file` / `topic` / `kafka_offset` | ✅ | ✅ | — |
| `ingestion_time` | ✅ | ✅ | — |
| `processing_time` | — | ✅ | ✅ |
| `record_hash` / `event_id` | ✅ | ✅ | — |
| `_corrupt_record` (parser errors) | ✅ | — | — |
| `batch_id` (for idempotent rebuilds) | ✅ | ✅ | ✅ |

## Anti-patterns

| Don't | Why |
|---|---|
| Parse heavily in bronze | If the parser changes you can't reuse bronze |
| MERGE into bronze | Loses the audit trail; bronze must be append-only |
| Skip silver, go bronze → gold | Brittle; bad rows poison aggregates |
| Mix layers in one job | Failure in stage 2 stops stage 1 ingest |
| One giant silver | Hard to evolve; partition by domain instead |
| Use gold for ad-hoc queries | Gold is shape-fitted to specific consumers; analysts should hit silver |

## Naming convention

A common scheme — use whatever your team already uses, but be consistent:

```
bronze.raw_orders_json
bronze.raw_users_jdbc

silver.orders
silver.users

gold.orders_daily_by_region
gold.customer_360
gold.product_revenue_top100
```

## Scale notes

For a real warehouse:
- Bronze: terabytes per day, mostly write-once.
- Silver: typically 1–3× source size after typing and dedup; queried often.
- Gold: small (gigabytes), highly partitioned, denormalized.

Maintenance: OPTIMIZE bronze daily on recent partitions, silver weekly, gold rebuilt on a schedule.

## References

- *Delta Lake: The Definitive Guide* — Ch.11 "Building a Lakehouse"
- "What's a Lakehouse?" — Databricks blog (search for it)
- *Fundamentals of Data Engineering* (Reis & Housley) — Ch.6 on data architecture
- 📺 [Bronze, Silver, Gold — Databricks](https://www.youtube.com/results?search_query=medallion+architecture+bronze+silver+gold+databricks)
