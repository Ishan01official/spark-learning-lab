# 10 — Spark SQL and the catalog

## Why this matters

`spark.sql("SELECT ...")` and the DataFrame API are **the same engine** — they share Catalyst, Tungsten, AQE, the plan optimizer, everything. The choice is only about readability. Many real pipelines mix both: data quality logic in Python, business logic in SQL.

## Three ways to register a query target

### 1. Temp view — session-local
```python
df.createOrReplaceTempView("orders")
spark.sql("SELECT country, SUM(amount) FROM orders GROUP BY country").show()
```
- Disappears when the session ends.
- Visible only in the current SparkSession.
- Doesn't write any data — it's a view on the existing DataFrame.

### 2. Global temp view — across sessions, same app
```python
df.createOrReplaceGlobalTempView("orders")
spark.sql("SELECT * FROM global_temp.orders").show()
```
Rare; mainly useful in apps that spawn multiple sessions.

### 3. Managed/external table — persistent
```python
df.write.mode("overwrite").saveAsTable("analytics.orders")     # managed
df.write.mode("overwrite").option("path", "s3://bucket/orders").saveAsTable("analytics.orders")  # external
```
- Lives in the metastore (Hive, Glue, Unity Catalog).
- Survives sessions and clusters.
- Managed: Spark owns the data; `DROP TABLE` deletes files.
- External: Spark owns only the metadata; `DROP TABLE` leaves files.

## Querying with SQL

```python
result = spark.sql("""
SELECT
    country,
    DATE_TRUNC('month', created_at) AS month,
    SUM(amount)                      AS revenue,
    COUNT(DISTINCT customer_id)      AS buyers
FROM analytics.orders
WHERE created_at >= '2024-01-01'
GROUP BY country, DATE_TRUNC('month', created_at)
ORDER BY country, month
""")
result.show()
```

`spark.sql(...)` returns a DataFrame. You can chain DataFrame operations on top:
```python
spark.sql("SELECT * FROM orders WHERE amount > 100").groupBy("country").count().show()
```

## Mixing SQL and DataFrame freely

```python
clean = (spark.read.parquet("raw/orders/")
            .filter(F.col("amount") > 0)
            .withColumn("amount", F.col("amount").cast("decimal(18,2)")))
clean.createOrReplaceTempView("orders_clean")

summary = spark.sql("""
    SELECT country, percentile_approx(amount, 0.5) AS median_amount
    FROM orders_clean
    GROUP BY country
""")

# Continue with DataFrame API
top10 = summary.orderBy(F.col("median_amount").desc()).limit(10)
```

## SQL functions you'll use a lot

The DataFrame `F.*` functions all have SQL equivalents. A few SQL-only conveniences:
- `CASE WHEN ... THEN ... ELSE ... END`
- `CAST(x AS DECIMAL(18,2))`
- `EXTRACT(YEAR FROM ts)`
- `LATERAL VIEW EXPLODE(arr) t AS item` — same as `explode` but inline
- CTE: `WITH x AS (...) SELECT ...`
- Set ops: `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT`

## The catalog API

```python
spark.catalog.listDatabases()
spark.catalog.listTables("analytics")
spark.catalog.listColumns("analytics.orders")
spark.catalog.tableExists("analytics.orders")
spark.catalog.refreshTable("analytics.orders")        # after files change outside Spark
spark.catalog.setCurrentDatabase("analytics")         # no more "analytics." prefix needed
spark.catalog.dropTempView("orders")
```

## DDL — creating tables in SQL

```sql
CREATE TABLE analytics.orders (
    order_id     BIGINT,
    customer_id  BIGINT,
    country      STRING,
    amount       DECIMAL(18,2),
    created_at   TIMESTAMP
)
USING parquet
PARTITIONED BY (country)
LOCATION 's3://bucket/orders/'
TBLPROPERTIES ('delta.minReaderVersion'='2');
```

Spark SQL DDL is mostly Hive-compatible. For Delta, use `USING delta`.

## When SQL is clearly better

- Multi-CTE pipelines reading like a story.
- Window functions with long frame specs.
- Onboarding analysts who already know SQL.
- Reusing queries between BI tools and Spark.

## When DataFrame API is clearly better

- Programmatic schemas (loop over columns).
- Dynamic logic (build the query at runtime).
- Reusable functions (`def with_audit_cols(df): ...`).
- Unit testing — DataFrames compose; SQL strings concatenate.

## Industry use case: hybrid pipeline

```python
def with_audit_cols(df):
    return df.withColumn("ingested_at", F.current_timestamp())

raw = with_audit_cols(spark.read.parquet("raw/orders/"))
raw.createOrReplaceTempView("orders_raw")

silver = spark.sql("""
    SELECT
        order_id,
        customer_id,
        COALESCE(country, 'UNK') AS country,
        CAST(amount AS DECIMAL(18,2)) AS amount,
        created_at,
        ingested_at
    FROM orders_raw
    WHERE amount > 0
""")
silver.write.mode("overwrite").saveAsTable("silver.orders")
```
DataFrame for plumbing, SQL for transformations, DataFrame for write.

## Scale notes

SQL and DataFrame APIs produce identical physical plans, so they have identical performance. Pick by readability.

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| `Table or view not found: orders` | view was registered in a different session | re-register or save as a real table |
| Catalog stale after files written outside Spark | metastore cache is stale | `spark.catalog.refreshTable(...)` |
| `CREATE TABLE` fails: "Hive support not enabled" | no metastore configured | `.enableHiveSupport()` and a warehouse dir |
| SQL works in Databricks but not locally | uses functions only in DBR runtime (e.g. `IDENTIFIER`, `EXPLAIN COST`) | stick to OSS Spark SQL features locally |

## References

- 📚 [LS Ch.4 (Spark SQL fundamentals)]
- 📚 [LS Ch.5 §"Tables and Views"]
- 📚 [HPS Ch.3 §"SparkSQL"]
- 📚 [DAS — throughout, SQL/DF freely mixed]
- 📺 [Apache Spark SQL — official docs](https://spark.apache.org/docs/latest/sql-ref.html)
