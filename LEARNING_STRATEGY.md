# Learning Strategy

This repo is built around one loop:

```text
read -> explain simply -> run the code -> break it -> debug it -> fix it -> write it up
```

## How A Beginner Should Use This Repo

Start with `00_setup/`, `18_python_for_pyspark/`, and `17_sql_for_spark/`. Do not rush into optimization until you can read a DataFrame, transform it, write it, and explain what action triggered the job.

Daily rule: one concept, one example, one written explanation.

## 30-Day Plan

| Days | Focus | Output |
| --- | --- | --- |
| 1-5 | Setup, Python, SQL, terminal | local Spark running, first Spark UI screenshot or notes |
| 6-12 | Spark fundamentals | explain driver, executor, job, stage, task |
| 13-22 | Daily PySpark | run core DataFrame examples |
| 23-27 | Joins, windows, nulls, nested data | solve exercises |
| 28-30 | Mini ETL | small Bronze/Silver/Gold pipeline |

## 60-Day Plan

Add optimization, Delta Lake, and one real project.

- Week 5: Catalyst, query plans, Spark UI.
- Week 6: joins, shuffle, partitioning, caching.
- Week 7: Delta Lake basics, MERGE, time travel.
- Week 8: build Orders ETL or SCD2 Customers.

## 90-Day Plan

Add streaming, Databricks, and interview prep.

- Month 1: fundamentals and PySpark.
- Month 2: production ETL, Delta, optimization.
- Month 3: streaming, Databricks production, interviews, portfolio polish.

## 6-Month Senior Data Engineer Plan

| Month | Focus | Evidence |
| --- | --- | --- |
| 1 | Foundations and PySpark | core examples run locally |
| 2 | ETL and Delta | medallion project |
| 3 | Optimization | tuning case study |
| 4 | Streaming | checkpoint and watermark project |
| 5 | Databricks and cloud | deployment design |
| 6 | Architecture and interviews | system design portfolio |

## Feynman Technique

For every concept, write:

1. One simple definition.
2. One analogy.
3. One code example.
4. One production failure.
5. One interview answer.

If you cannot do all five, you do not own the concept yet.

## Active Recall

Close the note and answer:

- What problem does this solve?
- What Spark UI tab proves it happened?
- What breaks at scale?
- What would I say in an interview?

## Spaced Repetition

Review each topic after:

- 1 day
- 3 days
- 7 days
- 21 days

Use `INTERVIEW_BANK.md` as the question source.

## Portfolio Practice

A portfolio project is ready when it has:

- runnable code
- README
- architecture diagram
- failure handling
- validation
- tuning notes
- interview explanation

Do not publish a notebook-only project and call it production-ready.
