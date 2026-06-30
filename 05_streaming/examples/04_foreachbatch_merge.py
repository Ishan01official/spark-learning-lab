"""
04 — foreachBatch + Delta MERGE (the canonical CDC pattern)

Demonstrates:
  - A streaming source of "change events"
  - foreachBatch wrapping a DeltaTable.merge call
  - Idempotent: re-running a batch doesn't duplicate (via txnAppId/txnVersion
    behavior of Delta — same batch_id, same outcome)

Run:
    python 04_foreachbatch_merge.py
"""

import time
import os
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


TARGET = "/tmp/streaming_demo/users_target"
CHECKPOINT = "/tmp/streaming_demo/ck_users_merge"


def get_spark() -> SparkSession:
    builder = (SparkSession.builder
        .appName("foreachbatch-merge")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def init_target_if_missing(spark) -> None:
    if not os.path.exists(TARGET):
        print(f"[init] Creating empty target table at {TARGET}")
        empty = spark.createDataFrame(
            [(0, "placeholder", "placeholder@x.com", 0)],
            ["user_id", "name", "email", "updated_at"]
        ).filter("user_id = -1")  # write schema but no rows
        empty.write.format("delta").mode("overwrite").save(TARGET)


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    init_target_if_missing(spark)

    # Simulate a CDC stream from rate.
    # value -> user_id (mod 20, so 20 unique users churning)
    # batch_id -> updated_at (monotonic)
    base = (spark.readStream
        .format("rate")
        .option("rowsPerSecond", 5)
        .load())

    cdc = (base
        .withColumn("user_id", (F.col("value") % 20).cast("long"))
        .withColumn("name",
                    F.concat(F.lit("user_"), F.col("user_id").cast("string")))
        .withColumn("email",
                    F.concat(F.col("name"), F.lit("@v"),
                             F.col("value").cast("string"), F.lit(".com")))
        .withColumn("updated_at", F.col("value"))
        .select("user_id", "name", "email", "updated_at"))

    target = DeltaTable.forPath(spark, TARGET)

    def merge_to_target(batch_df, batch_id):
        # Dedup the batch by user_id, keeping the latest updated_at
        deduped = (batch_df
            .withColumn("rn", F.row_number().over(
                F.Window.partitionBy("user_id").orderBy(F.desc("updated_at"))))
            .filter("rn = 1")
            .drop("rn"))

        (target.alias("t")
           .merge(deduped.alias("s"), "t.user_id = s.user_id")
           .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
           .whenNotMatchedInsertAll()
           .execute())

        print(f"  [merge] batch {batch_id}: source rows={batch_df.count()}, "
              f"merged into target")

    query = (cdc.writeStream
        .foreachBatch(merge_to_target)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start())

    print(f"\nCDC merge stream into {TARGET}. Press Ctrl+C to stop.\n")

    try:
        while query.isActive:
            time.sleep(15)
            cnt = spark.read.format("delta").load(TARGET).count()
            print(f"  [status] current target row count: {cnt}")
    except KeyboardInterrupt:
        query.stop()

    print("\nFinal target snapshot:")
    spark.read.format("delta").load(TARGET).orderBy("user_id").show(25, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
