"""
Gold stream: silver Delta source → session windows per user.

Run:
    python stream_gold.py
"""

import time
from pyspark.sql import functions as F

from session import get_spark, SILVER, GOLD, CK_ROOT


def main() -> None:
    spark = get_spark("gold")

    silver = (spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(SILVER))

    # Session window: a session ends when there's no activity for 5 minutes.
    # 15-minute watermark gives late events a chance to land.
    sessions = (silver
        .withWatermark("event_time", "15 minutes")
        .groupBy(
            F.session_window("event_time", "5 minutes"),
            "user_id")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("event_type").alias("event_type_count"),
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end")
        )
        .withColumn("session_duration_s",
                    F.unix_timestamp("session_end") - F.unix_timestamp("session_start"))
        .select("user_id", "session_start", "session_end", "session_duration_s",
                "event_count", "event_type_count"))

    query = (sessions.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CK_ROOT}/gold")
        .trigger(processingTime="30 seconds")
        .start(GOLD))

    print(f"[gold] {SILVER} -> {GOLD} (session windows)")
    print("  Sessions emit only after 5-min gap + 15-min watermark — expect delay.")

    try:
        while query.isActive:
            time.sleep(30)
            if query.lastProgress:
                p = query.lastProgress
                print(f"  [gold] batch={p['batchId']}, rows_in={p['numInputRows']}, "
                      f"watermark={p.get('eventTime', {}).get('watermark', 'n/a')}")
    except KeyboardInterrupt:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
