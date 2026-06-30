"""
Convenience runner: spawn bronze, silver, gold queries in the same Spark session.

In production, each of these is a separate job. This is for the demo only.

Run:
    python run_all.py
"""

import os
import time
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from session import get_spark, LANDING, BRONZE, SILVER, GOLD, CK_ROOT


EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),    False),
    StructField("user_id",    StringType(),    False),
    StructField("event_type", StringType(),    False),
    StructField("url",        StringType(),    True),
    StructField("event_time", TimestampType(), False),
])


def init_silver(spark):
    if not os.path.exists(SILVER):
        empty = spark.createDataFrame([], schema="""
            event_id STRING, user_id STRING, event_type STRING,
            url STRING, event_time TIMESTAMP, silver_processed_at TIMESTAMP
        """)
        empty.write.format("delta").partitionBy("event_type").save(SILVER)


def main() -> None:
    spark = get_spark("run_all")
    init_silver(spark)

    # ---- bronze ----
    bronze_q = ((spark.readStream
        .format("text")
        .option("maxFilesPerTrigger", 10)
        .load(LANDING))
        .withColumn("source_file", F.input_file_name())
        .withColumn("bronze_date", F.current_date())
        .withColumn("ingested_at", F.current_timestamp())
        .withColumnRenamed("value", "raw_value")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CK_ROOT}/bronze")
        .partitionBy("bronze_date")
        .trigger(processingTime="5 seconds")
        .start(BRONZE))

    # ---- silver ----
    target = DeltaTable.forPath(spark, SILVER)

    def silver_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        parsed = (batch_df
            .withColumn("_p", F.from_json("raw_value", EVENT_SCHEMA))
            .filter("_p IS NOT NULL AND _p.event_id IS NOT NULL")
            .select("_p.*")
            .withColumn("silver_processed_at", F.current_timestamp()))
        parsed = (parsed
            .withColumn("_rn", F.row_number().over(
                F.Window.partitionBy("event_id").orderBy(F.desc("event_time"))))
            .filter("_rn = 1").drop("_rn"))
        (target.alias("t")
            .merge(parsed.alias("s"), "t.event_id = s.event_id")
            .whenMatchedUpdateAll(condition="s.event_time > t.event_time")
            .whenNotMatchedInsertAll()
            .execute())

    silver_q = ((spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(BRONZE))
        .writeStream
        .foreachBatch(silver_batch)
        .option("checkpointLocation", f"{CK_ROOT}/silver")
        .trigger(processingTime="15 seconds")
        .start())

    # ---- gold ----
    gold_q = ((spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(SILVER))
        .withWatermark("event_time", "15 minutes")
        .groupBy(F.session_window("event_time", "5 minutes"), "user_id")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("event_type").alias("event_type_count"),
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"))
        .withColumn("session_duration_s",
                    F.unix_timestamp("session_end") - F.unix_timestamp("session_start"))
        .select("user_id", "session_start", "session_end", "session_duration_s",
                "event_count", "event_type_count")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CK_ROOT}/gold")
        .trigger(processingTime="30 seconds")
        .start(GOLD))

    print("\nAll three streams started.")
    print(f"  bronze ({bronze_q.name}) -> {BRONZE}")
    print(f"  silver ({silver_q.name}) -> {SILVER}")
    print(f"  gold   ({gold_q.name}) -> {GOLD}")
    print("\nPress Ctrl+C to stop all queries.\n")

    try:
        while bronze_q.isActive and silver_q.isActive and gold_q.isActive:
            time.sleep(30)
            for q in [bronze_q, silver_q, gold_q]:
                if q.lastProgress:
                    p = q.lastProgress
                    print(f"  [{q.name}] batch={p['batchId']} rows={p['numInputRows']}")
            print("---")
    except KeyboardInterrupt:
        for q in [bronze_q, silver_q, gold_q]:
            q.stop()
        spark.stop()
        print("Stopped.")


if __name__ == "__main__":
    main()
