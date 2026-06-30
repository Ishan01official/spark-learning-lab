# 01 — Databricks Certified Associate Developer: overview

## The exam

| | |
|---|---|
| Full name | Databricks Certified Associate Developer for Apache Spark Using Python |
| Format | Multiple-choice, ~60 questions |
| Time | ~120 minutes (2 hours) |
| Passing score | ~70% (varies; Databricks doesn't publish exact threshold) |
| Cost | ~$200 USD |
| Delivery | Online proctored, by Kryterion |
| Re-take wait | 24 hours minimum after a fail |
| Validity | 2 years |

Verify current details on the official page: https://www.databricks.com/learn/certification/apache-spark-developer-associate

## Topic weightings (approximate)

| Domain | % of exam |
|---|---|
| Spark Architecture (concepts) | 15% |
| Spark Architecture (applied) | 5% |
| Spark DataFrames (low level) | 10% |
| Spark DataFrames (transformations & actions) | 30% |
| Spark DataFrames (advanced — joins, windows) | 15% |
| Spark SQL | 10% |
| Streaming basics | 10% |
| Spark UI & debugging | 5% |

The exam strongly favors the *DataFrames API* over RDDs. Streaming gets a handful of questions; most are conceptual.

## What's NOT on the exam

- Scala API (Python only).
- Most of Delta Lake (a few questions max).
- Cluster/cost optimization beyond basics.
- ML / MLlib.
- GraphX, GraphFrames.
- Production deployment specifics.

If a question asks about Delta MERGE or time travel — it's probably a misread. Stick to vanilla Spark.

## What you'll be tested on

The cert is essentially:
1. **Can you read a PySpark snippet and predict the output / plan / failure?**
2. **Do you know the API well enough to pick the right method?**
3. **Can you reason about narrow vs wide transformations, partitions, joins?**
4. **Do you know what the Spark UI is showing you?**

## How to study

Three pieces:

1. **Read the cert prep guide** (Skiba's O'Reilly book). It maps directly to the exam.
2. **Run the examples** in this lab. Read each plan with `df.explain()`.
3. **Drill code-reading**. Use the practice questions here and in [LS] chapter end-of-chapter sets.

## On the day

- The exam is open-question, not adaptive. Skip and return.
- You can flag and review. Use this for the long code-reading questions.
- Watch your time — questions vary in length. 1.5–2 min average.
- No code execution allowed. You're reading and predicting.
- Most "trick" questions are about subtle API differences: `count()` vs `countDistinct()`, `coalesce()` vs `repartition()`, `withColumn` vs `select`.

## What "Associate" means

This is the entry-level developer cert. The follow-ups:
- **Databricks Certified Professional Data Engineer** — production patterns, optimization, Delta, streaming at scale.
- **Databricks Certified Professional ML Engineer** — MLflow, MLlib, model serving.

If you pass Associate Developer and want to keep going, Professional Data Engineer is the natural next step. Module 06's projects and module 03's optimization material map to that exam.

## References

- *Databricks Certified Associate Developer for Apache Spark Using Python* — Skiba (O'Reilly, 2025)
- Official cert page: https://www.databricks.com/learn/certification/apache-spark-developer-associate
- Spark API reference (PySpark): https://spark.apache.org/docs/latest/api/python/index.html
- 📺 [Databricks Cert exam tips](https://www.youtube.com/results?search_query=databricks+spark+developer+associate+exam+tips)
