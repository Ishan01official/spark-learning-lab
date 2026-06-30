"""
02 — Build silver from bronze.

Silver design:
  - Parses JSON, applies strict schema, drops bad rows to dead-letter.
  - Deduplicates by order_id (latest wins by order_time).
  - MERGEs into the silver orders table; idempotent across re-runs.
  - Loads customers from the static reference file (Type 1).

Run:
    python -m orders_etl.jobs.02_build_silver
"""

import os
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from orders_etl.config import CONFIG
from orders_etl.session import get_spark
from orders_etl.schemas import ORDER_JSON_SCHEMA


def init_target_if_missing(spark) -> DeltaTable:
    """Idempotently ensure the target silver table exists."""
    if not os.path.exists(CONFIG.silver_orders_path):
        empty = spark.createDataFrame([], schema="""
            order_id STRING,
            customer_id STRING,
            sku STRING,
            amount DECIMAL(18,2),
            region STRING,
            order_time TIMESTAMP,
            order_date DATE,
            updated_at TIMESTAMP,
            batch_id STRING
        """)
        (empty.write.format("delta")
            .partitionBy("order_date")
            .save(CONFIG.silver_orders_path))
    return DeltaTable.forPath(spark, CONFIG.silver_orders_path)


def build_customers(spark):
    """Build customer dim (Type 1 overwrite) from the customers.json reference."""
    path = f"{CONFIG.landing_dir}/../customers.json"
    cust = (spark.read.json(path)
        .select("customer_id", "country", "tier"))

    (cust.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(CONFIG.silver_customers_path))
    print(f"[silver.customers] wrote {cust.count()} rows")


def main() -> None:
    spark = get_spark("02_build_silver")
    target = init_target_if_missing(spark)
    build_customers(spark)

    # Read bronze
    bronze = spark.read.format("delta").load(CONFIG.bronze_path)
    print(f"[silver] reading {bronze.count()} rows from bronze")

    # Parse the raw_value JSON column against our strict schema
    parsed = bronze.withColumn(
        "_payload",
        F.from_json(F.col("raw_value"), ORDER_JSON_SCHEMA)
    )

    good = (parsed
        .filter("_payload IS NOT NULL AND _payload.order_id IS NOT NULL")
        .select("_payload.*", "batch_id"))
    bad = (parsed
        .filter("_payload IS NULL OR _payload.order_id IS NULL")
        .select("raw_value", "source_file", "batch_id", "ingested_at",
                F.lit("schema_violation").alias("reason")))

    bad_count = bad.count()
    if bad_count:
        (bad.write.format("delta")
            .mode("append")
            .save(CONFIG.silver_dlt_path))
        print(f"[silver] {bad_count} bad rows routed to dead-letter")

    # Add derived columns + ingest time
    silver_rows = (good
        .withColumn("order_date", F.to_date("order_time"))
        .withColumn("updated_at", F.current_timestamp()))

    # Dedup: latest order_time wins per order_id
    window = F.row_number().over(
        F.Window.partitionBy("order_id").orderBy(F.desc("order_time")))
    silver_rows = (silver_rows
        .withColumn("_rn", window)
        .where("_rn = 1")
        .drop("_rn"))

    # MERGE — idempotent on order_id
    (target.alias("t")
        .merge(silver_rows.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.order_time > t.order_time")
        .whenNotMatchedInsertAll()
        .execute())

    cnt = spark.read.format("delta").load(CONFIG.silver_orders_path).count()
    print(f"[silver.orders] table now has {cnt} rows")

    spark.stop()


if __name__ == "__main__":
    main()
