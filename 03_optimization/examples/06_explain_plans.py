"""
06 — Tour of explain() output

Shows the four explain modes and what signals to look for in each.

Run:
    python 06_explain_plans.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("explain-tour")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 4)
            .getOrCreate())


def setup(spark):
    # Two tables: orders (big) and customers (small) — both Parquet for pushdown.
    (spark.range(1_000_000)
        .withColumn("customer_id", (F.col("id") % 10_000).cast("long"))
        .withColumn("amount", (F.rand() * 500).cast("int"))
        .withColumn("country", F.expr("element_at(array('US','DE','JP','IN'), int(rand()*4)+1)"))
        .write.mode("overwrite").parquet("/tmp/orders"))

    (spark.range(10_000)
        .withColumn("customer_id", F.col("id"))
        .withColumn("name", F.concat(F.lit("cust_"), F.col("id").cast("string")))
        .write.mode("overwrite").parquet("/tmp/customers"))


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    setup(spark)

    orders    = spark.read.parquet("/tmp/orders")
    customers = spark.read.parquet("/tmp/customers")

    # A query that exercises pushdown, projection pruning, broadcast join, aggregation.
    query = (orders
        .filter(F.col("country") == "US")
        .filter(F.col("amount") > 100)
        .join(customers, "customer_id")
        .groupBy("name")
        .agg(F.sum("amount").alias("total")))

    print("=" * 70)
    print("1) DEFAULT — flat physical plan")
    print("=" * 70)
    query.explain()
    # Look for: PushedFilters on FileScan, BroadcastHashJoin (since customers ~ 200KB),
    # HashAggregate, Exchange between them.

    print("\n" + "=" * 70)
    print("2) ALL — parsed, analyzed, optimized, physical")
    print("=" * 70)
    query.explain(True)
    # Trace columns from analyzed (resolved) -> optimized (filter pushdown, projection prune)
    # -> physical (specific operators).

    print("\n" + "=" * 70)
    print("3) FORMATTED — operator IDs and per-node detail")
    print("=" * 70)
    query.explain("formatted")
    # Best mode for reviewing nontrivial plans.

    print("\n" + "=" * 70)
    print("4) COST — plan with size statistics (only useful when CBO/AQE has stats)")
    print("=" * 70)
    query.explain("cost")
    # sizeInBytes / rowCount on each node tells you why the planner picked broadcast vs SMJ.

    print("\n" + "=" * 70)
    print("After execution, the AQE-rewritten plan is visible only on a materialized query.")
    print("=" * 70)
    query.count()  # materialize so AQE has a chance to rewrite
    query.explain()  # now this shows the actually-executed plan

    input("\nPress Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
