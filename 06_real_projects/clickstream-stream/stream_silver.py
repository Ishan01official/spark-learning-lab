"""
Silver stream: bronze Delta source → silver Delta MERGE (deduped on event_id).

Run:
    python stream_silver.py
"""

import os
import time
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from session import get_spark, BRONZE, SILVER, CK_ROOT


EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),    False),
    StructField("user_id",    StringType(),    False),
    StructField("event_type", StringType(),    False),
    StructField("url",        StringType(),    True),
    StructField("event_time", TimestampType(), False),
])


def init_silver(spark) -> DeltaTable:
    """Create the silver table if it doesn't exist."""
    if not os.path.exists(SILVER):
        empty = spark.createDataFrame([], schema="""
            event_id STRING,
            user_id STRING,
            event_type STRING,
            url STRING,
            event_time TIMESTAMP,
            silver_processed_at TIMESTAMP
        """)
        (empty.write.format("delta")
            .partitionBy("event_type")
            .save(SILVER))
    return DeltaTable.forPath(spark, SILVER)


def main() -> None:
    spark = get_spark("silver")
    target = init_silver(spark)

    bronze_stream = (spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(BRONZE))

    def write_silver_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        parsed = batch_df.withColumn(
            "_p", F.from_json(F.col("raw_value"), EVENT_SCHEMA)
        ).filter("_p IS NOT NULL AND _p.event_id IS NOT NULL")

        events = parsed.select("_p.*").withColumn(
            "silver_processed_at", F.current_timestamp())

        # Dedup within the batch (latest event_time wins)
        events = (events
            .withColumn("_rn", F.row_number().over(
                F.Window.partitionBy("event_id").orderBy(F.desc("event_time"))))
            .filter("_rn = 1")
            .drop("_rn"))

        (target.alias("t")
            .merge(events.alias("s"), "t.event_id = s.event_id")
            .whenMatchedUpdateAll(condition="s.event_time > t.event_time")
            .whenNotMatchedInsertAll()
            .execute())

        print(f"  [silver] batch {batch_id}: processed {events.count()} unique events")

    query = (bronze_stream.writeStream
        .foreachBatch(write_silver_batch)
        .option("checkpointLocation", f"{CK_ROOT}/silver")
        .trigger(processingTime="15 seconds")
        .start())

    print(f"[silver] {BRONZE} -> {SILVER} (MERGE)")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
