"""
05 — OPTIMIZE and Z-ORDER

Demonstrates:
  - Many small files problem
  - OPTIMIZE compacting them
  - Z-ORDER improving filter selectivity
  - VACUUM removing the old files

Open the _delta_log/ directory between steps to see the commits accumulating.

Run:
    python 05_optimize_zorder.py
"""

import os
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    builder = (SparkSession.builder
        .appName("delta-optimize")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


PATH = "/tmp/delta_demo/optimize_zorder"


def count_data_files(path: str) -> int:
    """Count Parquet files (excluding the log)."""
    n = 0
    for root, _, files in os.walk(path):
        if "_delta_log" in root:
            continue
        for f in files:
            if f.endswith(".parquet"):
                n += 1
    return n


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("[1] Simulate many tiny writes (10 batches, ~100 rows each)")
    for i in range(10):
        df = (spark.range(100)
              .withColumn("user_id", (F.col("id") + i * 100) % 1_000_000)
              .withColumn("event_type", F.expr("element_at(array('click','view','buy'), int(rand()*3)+1)"))
              .withColumn("ts", F.lit(f"2024-09-{i+1:02d}").cast("timestamp")))
        mode = "overwrite" if i == 0 else "append"
        df.write.format("delta").mode(mode).save(PATH)

    print(f"  data files after 10 small writes: {count_data_files(PATH)}")

    # ----- OPTIMIZE (compact) -----
    print("\n[2] OPTIMIZE — compact small files")
    spark.sql(f"OPTIMIZE delta.`{PATH}`")
    print(f"  data files after OPTIMIZE: {count_data_files(PATH)}")
    print("  (old files still on disk until VACUUM, but no longer referenced)")

    # ----- Q1: filter without Z-ORDER -----
    print("\n[3] Query: filter on user_id (no Z-ORDER)")
    q1 = (spark.read.format("delta").load(PATH)
              .filter("user_id BETWEEN 500000 AND 500100"))
    q1.explain("formatted")
    print(f"  result count: {q1.count()}")

    # ----- Z-ORDER -----
    print("\n[4] OPTIMIZE ... ZORDER BY (user_id) — cluster by query key")
    spark.sql(f"OPTIMIZE delta.`{PATH}` ZORDER BY (user_id)")

    # ----- Q2: same filter, post Z-ORDER -----
    print("\n[5] Query: same filter, post Z-ORDER")
    q2 = (spark.read.format("delta").load(PATH)
              .filter("user_id BETWEEN 500000 AND 500100"))
    q2.explain("formatted")
    # In the formatted plan, look at the FileScan's PushedFilters and the number
    # of files actually opened. With Z-ORDER, far fewer files are touched.
    print(f"  result count: {q2.count()}")

    # ----- VACUUM (with the standard 7-day safety) -----
    print("\n[6] VACUUM — by default, only deletes files older than 7 days")
    # For demo purposes we use a 0-hour retention; in production, DON'T.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM delta.`{PATH}` RETAIN 0 HOURS")
    print(f"  data files after VACUUM: {count_data_files(PATH)}")
    print("  NOTE: in production, use the default 7-day retention to preserve time travel.")

    print("\n[7] Final history:")
    DeltaTable.forPath(spark, PATH).history().select(
        "version", "operation", "operationParameters"
    ).show(20, truncate=False)

    input("\nPress Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
