# 06 — Real-world projects

Five end-to-end projects that combine everything from modules 00–05. Each one is a self-contained subfolder with its own README, source code, and exercise prompts.

The intent: practice the patterns, then steal them for production work.

## What you should be able to do after this

- Design a medallion (bronze/silver/gold) Lakehouse pipeline end-to-end.
- Implement SCD Type 2 with full audit history.
- Deduplicate large skewed datasets.
- Build a data quality framework that fails fast on bad inputs.
- Ingest a Kafka-style clickstream into a queryable warehouse.

## The projects

1. [`orders-etl/`](orders-etl/) — Medallion ETL for an orders dataset (bronze → silver → gold).
2. [`scd2-customers/`](scd2-customers/) — Slowly Changing Dimension Type 2 for customer history.
3. [`dedup-at-scale/`](dedup-at-scale/) — De-duplicating skewed events at TB scale.
4. [`dq-framework/`](dq-framework/) — A reusable data-quality validation framework.
5. [`clickstream-stream/`](clickstream-stream/) — Streaming clickstream ingestion with sessionization.

## Notes (background reading)

1. [The medallion architecture (bronze/silver/gold)](01-medallion-architecture.md)
2. [Idempotency in ETL — why it's table stakes](02-idempotency-patterns.md)
3. [Data quality — failing fast vs quarantining](03-data-quality-patterns.md)
4. [SCD types — when to use which](04-scd-types.md)
5. [Production deployment patterns](05-deployment-patterns.md)

## How to work through this module

Read the notes first — they give context. Then work through projects in order: each builds on patterns established by the prior one. Each project has a `README.md` explaining the design and a numbered set of files you can run incrementally.

## Book references

- *Fundamentals of Data Engineering* by Reis & Housley — high-level architecture context.
- *Delta Lake: The Definitive Guide* — Ch.11 on Lakehouse patterns.
- [DAS Chapters 9–12] — algorithmic patterns at scale.
- [HPS Ch.11] — production deployment.
