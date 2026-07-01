# Project Index

Every project should teach a reusable production pattern. A complete project contains a README, runnable code, small local data or generated data, a diagram, exercises, and interview questions.

## Current Projects

| Project | Level | Pattern | Status | Location |
| --- | --- | --- | --- | --- |
| Orders ETL | Intermediate | Bronze to Silver to Gold batch ETL | Implemented | `06_real_projects/orders-etl/` |
| SCD2 Customers | Intermediate | Slowly Changing Dimension Type 2 | Implemented | `06_real_projects/scd2-customers/` |
| Dedup At Scale | Intermediate | Deduplication with large/skewed data | Implemented | `06_real_projects/dedup-at-scale/` |
| Data Quality Framework | Intermediate | Rule-based checks and quarantine | Implemented | `06_real_projects/dq-framework/` |
| Clickstream Stream | Intermediate | Structured Streaming style pipeline | Implemented | `06_real_projects/clickstream-stream/` |

## Planned Portfolio Projects

| Project | What It Teaches | Minimum Deliverables |
| --- | --- | --- |
| Basic ETL pipeline | Read, clean, write, validate | one source, one target, one validation report |
| Sales analytics pipeline | Dimensional modeling and aggregations | orders, products, customers, daily revenue table |
| Incremental load pipeline | Watermarks, high-water marks, idempotency | append-only source, replay test, duplicate protection |
| SCD Type 2 customer dimension | Historical tracking | MERGE logic, active flag, validity dates |
| Deduplication at scale | Window dedup and skew | duplicate generator, before/after metrics |
| Data quality framework | Declarative validation | rules config, quarantine table, failure summary |
| Clickstream pipeline | Sessionization | events, sessions, funnel metrics |
| Streaming Kafka pipeline | Checkpoints and late data | local rate/Kafka-like source, watermark, restart test |
| Delta Lake medallion pipeline | Lakehouse storage design | Bronze/Silver/Gold Delta tables |
| Performance tuning case study | Plan reading and improvement | slow query, optimized query, measured result |
| Skewed join debugging project | Hot key diagnosis | `11_case_studies/labs/01_skewed_join_lab.py`, salting fix, Spark UI checklist |
| Small files optimization project | File sizing | `11_case_studies/labs/02_small_files_lab.py`, file-count metric, output layout notes |
| Production incident RCA project | Debugging narrative | timeline, symptoms, root cause, prevention |
| End-to-end lakehouse project | Full platform thinking | ingestion, storage, orchestration, quality, serving |
| Architect-level platform design | Tradeoff communication | requirements, diagram, security, cost, operations |

## Standard Project README Template

```text
# Project name

## Problem statement
## Business context
## Architecture diagram
## Data flow
## How to run locally
## Table design
## Code walkthrough
## Tests and validation
## Failure modes
## Performance notes
## Exercises
## Interview questions
## Production extensions
```
