"""
Example 05 — Aggregations: groupBy, agg, rollup, cube, pivot, approx.

Run:
    python 05_aggregations_demo.py
"""

from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
              .appName("02-core-05-agg")
              .master("local[*]")
              .config("spark.sql.shuffle.partitions", "8")
              .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(
        [
            (1, "US", 2024, 100.0, "alice"),
            (2, "US", 2024, 200.0, "alice"),
            (3, "US", 2024,  50.0, "bob"),
            (4, "US", 2025, 300.0, "alice"),
            (5, "IN", 2024,  80.0, "cara"),
            (6, "IN", 2025,  90.0, "cara"),
            (7, "IN", 2025,  10.0, "dan"),
            (8, "DE", 2024, 500.0, "evan"),
        ],
        schema="order_id LONG, country STRING, year INT, amount DOUBLE, customer STRING",
    )

    # ---- Global aggregation ----
    print("Global:")
    df.agg(
        F.count("*").alias("n_orders"),
        F.sum("amount").alias("total"),
        F.avg("amount").alias("aov"),
        F.countDistinct("customer").alias("uniq_customers"),
        F.approx_count_distinct("customer", rsd=0.05).alias("uniq_customers_approx"),
    ).show()

    # ---- Group by country ----
    print("Grouped by country:")
    (df.groupBy("country")
       .agg(F.sum("amount").alias("revenue"),
            F.countDistinct("customer").alias("buyers"),
            F.expr("percentile_approx(amount, 0.5)").alias("median_amount"))
       .orderBy(F.desc("revenue"))
       .show())

    # ---- Multiple aggregations per column ----
    print("Multiple aggs on amount per country/year:")
    (df.groupBy("country", "year")
       .agg(F.min("amount"), F.max("amount"), F.avg("amount"), F.sum("amount"))
       .orderBy("country", "year")
       .show())

    # ---- Rollup (subtotal per country, then grand total) ----
    print("Rollup (country, year):")
    (df.rollup("country", "year")
       .agg(F.sum("amount").alias("revenue"))
       .orderBy("country", "year")
       .show())

    # ---- Cube (every combination) ----
    print("Cube (country, year):")
    (df.cube("country", "year")
       .agg(F.sum("amount").alias("revenue"))
       .orderBy("country", "year")
       .show())

    # ---- Pivot ----
    print("Pivot year across columns:")
    (df.groupBy("country")
       .pivot("year", [2024, 2025])
       .agg(F.sum("amount"))
       .show())

    # ---- Show the plan for a grouped aggregation ----
    print("Plan for groupBy.country.sum(amount):")
    (df.groupBy("country").agg(F.sum("amount"))).explain(mode="formatted")

    print("Press Enter to stop...")
    input()
    spark.stop()


if __name__ == "__main__":
    main()
