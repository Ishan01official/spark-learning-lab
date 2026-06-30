"""
03 — Build gold from silver.

Gold design:
  - Rebuild from silver each run (cheap; idempotent by construction).
  - Two outputs:
      gold.revenue_by_region_daily — totals by region per day
      gold.top_skus_30d            — top 20 SKUs by 30-day revenue

Run:
    python -m orders_etl.jobs.03_build_gold
"""

from pyspark.sql import functions as F

from orders_etl.config import CONFIG
from orders_etl.session import get_spark


def main() -> None:
    spark = get_spark("03_build_gold")

    silver = spark.read.format("delta").load(CONFIG.silver_orders_path)
    customers = spark.read.format("delta").load(CONFIG.silver_customers_path)

    enriched = silver.join(F.broadcast(customers), "customer_id", "left")

    # ----- Gold 1: revenue by region by day -----
    revenue = (enriched
        .groupBy("order_date", "region", "tier")
        .agg(F.sum("amount").alias("revenue"),
             F.count("*").alias("order_count"),
             F.countDistinct("customer_id").alias("unique_customers")))

    (revenue.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("order_date")
        .save(CONFIG.gold_revenue_path))

    print(f"[gold.revenue] {revenue.count()} rows")

    # ----- Gold 2: top SKUs in last 30 days -----
    cutoff = F.current_date() - F.expr("interval 30 days")
    top = (enriched
        .where(F.col("order_date") >= cutoff)
        .groupBy("sku")
        .agg(F.sum("amount").alias("revenue_30d"),
             F.count("*").alias("orders_30d"))
        .orderBy(F.desc("revenue_30d"))
        .limit(20))

    (top.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(CONFIG.gold_top_skus_path))

    print(f"[gold.top_skus] {top.count()} rows")

    print("\nSample revenue by region:")
    revenue.orderBy(F.desc("revenue")).show(10)
    print("\nSample top SKUs:")
    top.show(10)

    spark.stop()


if __name__ == "__main__":
    main()
