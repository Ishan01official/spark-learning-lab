# 01_fundamentals — exercises

Each exercise targets one mental model. Solve, then check against the relevant note.

## Exercise 1 — count the stages

Given:
```python
df = (spark.read.csv("data/orders.csv", header=True, inferSchema=True)
        .filter(F.col("amount") > 100)
        .groupBy("country").sum("amount")
        .orderBy(F.desc("sum(amount)")))
df.show()
```
**Question:** How many stages will this produce, and why? (Hint: count the shuffle boundaries.)

**Expected:** 3 stages. Stage 0 = scan + filter. Stage 1 = local partial aggregate + shuffle write + groupBy aggregate after shuffle. Stage 2 = orderBy (second shuffle, range-partitioned).

## Exercise 2 — predict tasks

You run an aggregation on a 4 GB CSV with `spark.sql.files.maxPartitionBytes=128MB`. Default `spark.sql.shuffle.partitions=200`. Cluster has 4 executors × 4 cores.

- How many tasks in stage 0 (scan)?
- How many tasks in stage 1 (post-shuffle aggregate)?
- How many tasks run concurrently?

**Expected:** Stage 0 ≈ 4096/128 = 32 tasks. Stage 1 = 200 tasks. Concurrency = 4×4 = 16 tasks at once.

## Exercise 3 — narrow or wide?

For each, say narrow / wide:

1. `df.filter(col("x") > 0)`
2. `df.withColumn("y", col("x") * 2)`
3. `df.distinct()`
4. `df.repartition(10)`
5. `df.coalesce(10)`
6. `df.dropDuplicates(["id"])`
7. `df.union(other)`
8. `df.join(other, "id")` (no broadcast)
9. `df.join(broadcast(other), "id")`
10. `df.orderBy("x")`

**Expected:** narrow: 1, 2, 5, 7, 9 — wide: 3, 4, 6, 8, 10.

## Exercise 4 — explain() reading

Run example `04_narrow_wide_demo.py`. In the output for the third query (with `orderBy` after `groupBy`), identify:

- Each `Exchange` node in the plan.
- Whether it is a `hashpartitioning` or `rangepartitioning` shuffle.
- Which Spark stage corresponds to each segment between Exchange nodes.

## Exercise 5 — lineage and recovery

```python
rdd1 = sc.textFile("data/orders.csv")
rdd2 = rdd1.filter(lambda l: "USA" in l)
rdd3 = rdd2.map(lambda l: l.upper())
print(rdd3.toDebugString().decode())
```
Now imagine an executor holding partitions 5 and 6 of `rdd3` dies. What does Spark recompute, and from where? (No cache is set.)

**Expected:** Spark recomputes just partitions 5 and 6, starting from `rdd1` (re-reads those input splits), applies filter then map. Other partitions and other RDDs are untouched.

## Exercise 6 — find the action

```python
df = spark.read.parquet("s3://bucket/events")
df2 = df.filter(F.col("event") == "click").cache()
df2.printSchema()
df2.explain()
df2.count()
df2.show(5)
```
- How many Spark jobs does this submit?
- Is the cache populated before or after the count?

**Expected:** 2 jobs (count, show). printSchema and explain are not actions. cache is populated *during* the count (the first action that hits df2).

## Going further

- Pull up the Spark UI (`http://localhost:4040`) after each exercise. Verify your predictions against the actual DAG and stage timeline.
- For each wrong prediction, write a one-line note in your own words about what surprised you. That note is the learning, not the right answer.
