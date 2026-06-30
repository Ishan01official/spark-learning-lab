"""
02 — Time travel

Demonstrates:
  - versionAsOf and timestampAsOf reads
  - history() inspection
  - restoreToVersion to roll back a bad write

Run:
    python 02_time_travel.py
"""

import time
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    builder = (SparkSession.builder
        .appName("delta-time-travel")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


PATH = "/tmp/delta_demo/time_travel"


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Version 0: create table with 100 rows
    print("[v0] Initial write — 100 rows")
    df0 = spark.range(100).withColumn("value", F.col("id") * 10)
    df0.write.format("delta").mode("overwrite").save(PATH)
    time.sleep(2)

    # Version 1: append 50 more
    print("[v1] Append 50 rows")
    df1 = spark.range(100, 150).withColumn("value", F.col("id") * 10)
    df1.write.format("delta").mode("append").save(PATH)
    time.sleep(2)

    # Version 2: a "bad" write that overwrites everything with junk
    print("[v2] BAD WRITE — overwriting with junk data (oops)")
    junk = spark.range(10).withColumn("value", F.lit(-1))
    junk.write.format("delta").mode("overwrite").save(PATH)

    # Show what each version looks like
    dt = DeltaTable.forPath(spark, PATH)
    print("\nHistory:")
    dt.history().select("version", "timestamp", "operation",
                        "operationParameters").show(truncate=False)

    print("\nCURRENT version contents (junk):")
    spark.read.format("delta").load(PATH).orderBy("id").show(20)

    print("\nVersion 0 (initial):")
    (spark.read.format("delta")
        .option("versionAsOf", 0)
        .load(PATH).count())
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(PATH)
    print(f"  count: {df_v0.count()}")

    print("\nVersion 1 (after first append):")
    df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(PATH)
    print(f"  count: {df_v1.count()}")

    # Restore to v1
    print("\n[restore] Rolling back to version 1")
    dt.restoreToVersion(1)

    print("\nAfter restore — current state:")
    print(f"  count: {spark.read.format('delta').load(PATH).count()}")
    print("History now has an additional RESTORE commit:")
    dt.history().select("version", "operation").show(truncate=False)

    input("\nPress Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
