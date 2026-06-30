# Spark Interview Question Bank

Use this as a rolling question bank. Keep answers short first, then link to deeper module notes.

## Fundamentals

1. What is the difference between a Spark application, job, stage, and task?
2. What runs on the driver, and what runs on executors?
3. Why does Spark use lazy evaluation?
4. What is lineage, and how does it help with fault tolerance?
5. When would you still use an RDD instead of a DataFrame?

## DataFrames And SQL

1. Why are DataFrames usually faster than RDDs?
2. What is Catalyst?
3. What is Tungsten?
4. What is schema inference, and when should you avoid it?
5. How do `select`, `withColumn`, `filter`, and `where` differ in practice?

## Joins

1. Explain broadcast hash join.
2. When should you avoid broadcasting a table?
3. What causes a shuffle join?
4. How do you detect skew in a join?
5. How would you fix one hot key that dominates a join?

## Partitioning And Shuffle

1. What is a partition?
2. What is the difference between `repartition` and `coalesce`?
3. Why are wide transformations expensive?
4. What does `spark.sql.shuffle.partitions` control?
5. How do small files hurt Spark performance?

## Memory And Performance

1. What does caching do?
2. When can caching make a job slower?
3. How do you read an explain plan?
4. What is AQE?
5. What metrics in the Spark UI tell you a job is spilling?

## Delta Lake

1. What problem does Delta Lake solve on top of Parquet?
2. What is the Delta transaction log?
3. How does time travel work?
4. What is `MERGE INTO` used for?
5. What is the difference between `OPTIMIZE` and `VACUUM`?

## Structured Streaming

1. What is a micro-batch?
2. What is checkpointing?
3. What is a watermark?
4. What does exactly-once mean in Spark streaming?
5. Why is `foreachBatch` useful?

## Scenario Questions

1. A daily ETL job used to finish in 15 minutes and now takes 2 hours. What do you check first?
2. A join works in dev but fails with executor OOM in production. How do you debug it?
3. A streaming job produces duplicate records after restart. What are likely causes?
4. A Delta table has thousands of tiny files. What is the fix?
5. A job writes partial output and then fails. How do you make it idempotent?

## Answer Pattern

For interviews, answer in this order:

1. Define the concept simply.
2. Explain why it matters at scale.
3. Name the failure mode.
4. Give the operational fix.
5. Mention where you would verify it in the Spark UI.
