# 02_pyspark_core — exercises

## Exercise 1 — schema vs inference

Take `examples/03_schema_explicit.py`. Modify the generated data so that **5 random rows in the middle have a non-numeric `amount`** (e.g. `"oops"`). Now read with:
- `inferSchema=true` — what type does Spark pick for `amount`?
- explicit schema with `DecimalType` and `mode=PERMISSIVE` — what happens to the bad rows?
- explicit schema with `mode=FAILFAST` — what happens?

Write a one-paragraph answer about which mode you'd use in production and why.

## Exercise 2 — partitioning sanity check

Write 100 000 orders partitioned by `country` (4 values), then partitioned by `customer_id` (1 000 distinct values). Compare:
- Number of files generated.
- Total bytes on disk.
- Time to read with `WHERE country='US'`.
- Time to read with `WHERE customer_id = 12345`.

Which layout is better for which query? Why?

## Exercise 3 — joins in the wild

For each of these joins, predict whether Spark will pick BHJ, SMJ, or SHJ, then verify with `explain(mode="formatted")`:

1. `orders` (1 GB) `JOIN` `countries` (5 KB) — default settings.
2. Same as 1, but with `spark.sql.autoBroadcastJoinThreshold=-1`.
3. `orders` (1 GB) `JOIN` `users` (300 MB) — default settings.
4. Same as 3, but with `.hint("broadcast")` on users.
5. `orders` (1 GB) `JOIN` `events` (1 GB) — default settings.

## Exercise 4 — skew detection

Generate `orders` where 70% of rows have `country='US'`. Join with `countries` (small, sort-merge forced). Run with:
- AQE skew handling off (`spark.sql.adaptive.skewJoin.enabled=false`).
- AQE skew handling on.

Compare the **Max task duration** and the **number of stage tasks** in the Spark UI.

## Exercise 5 — window patterns

For the orders data from Example 5, write window queries for:

1. Each customer's first and last order date.
2. For each (country, year), the customer who spent the most.
3. Each order, with the 7-day rolling total for its customer ending on that order.
4. Each order, marked as part of a session (gap > 30 min → new session).

## Exercise 6 — UDFs the right way

You have a column `payload` containing JSON strings like `{"user":{"id":42,"name":"alice"},"event":"click"}`. You need to extract `user.id` and `event` into columns.

Solve it three ways:
- A row-at-a-time UDF using `json.loads`.
- A Pandas UDF using `pd.json_normalize`.
- Built-ins (`from_json`, `get_json_object`).

Time each on 1M rows. Which would you ship? Why?

## Exercise 7 — read it like the Spark UI

Pick the most complex query you built in this module. Run it, then in the Spark UI:
- Locate the SQL tab → the query.
- Identify every Exchange (shuffle) and what triggered it.
- Identify the join strategy used.
- Identify the slowest stage by total task time, and write down what it's doing.

Write a 1-page "post-mortem" of your own query. (This skill is what separates senior data engineers from juniors.)
