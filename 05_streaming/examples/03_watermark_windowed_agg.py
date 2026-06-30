"""
03 — Watermarks and windowed aggregation

Demonstrates:
  - Generating a stream with synthetic out-of-order event times
  - Tumbling window aggregation with a watermark
  - append output mode (windows emitted once when closed)
  - Observing dropped-by-watermark counts

Run:
    python 03_watermark_windowed_agg.py
"""

import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType


def get_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("watermark-window")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 4)
            .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Use rate as a base and synthesize an `event_time` that is slightly
    # out-of-order to make the watermark behavior interesting.
    base = (spark.readStream
        .format("rate")
        .option("rowsPerSecond", 50)
        .load())

    # `event_time` is (current ts) - random lag in seconds (0..30).
    # Sometimes lag is 0; sometimes a row arrives "from 30 seconds ago".
    synthesized = (base
        .withColumn("lag_secs", (F.rand() * 30).cast("int"))
        .withColumn("event_time",
                    F.expr("timestamp - make_interval(0, 0, 0, 0, 0, 0, lag_secs)"))
        .withColumn("user_id", F.col("value") % 10)
        .withColumn("event_type",
                    F.element_at(F.array(F.lit("click"), F.lit("view"), F.lit("buy")),
                                 (F.col("value") % 3 + 1).cast("int"))))

    # 5-second tumbling window, 10-second watermark.
    # That means: rows up to 10s late are accepted. Windows are emitted in
    # `append` mode once watermark passes their end.
    counts = (synthesized
        .withWatermark("event_time", "10 seconds")
        .groupBy(F.window("event_time", "5 seconds"), "event_type")
        .count()
        .select(
            F.col("window.start").alias("win_start"),
            F.col("window.end").alias("win_end"),
            "event_type",
            "count"))

    query = (counts.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start())

    print("Watermarked windowed counts. Press Ctrl+C to stop.")
    print("Note: rows emit only AFTER watermark passes the window end (~10-15s lag).\n")

    try:
        while query.isActive:
            time.sleep(15)
            prog = query.lastProgress
            if prog and prog.get("stateOperators"):
                st = prog["stateOperators"][0]
                print(f"  [state] rows total={st.get('numRowsTotal')}, "
                      f"updated={st.get('numRowsUpdated')}, "
                      f"dropped_by_watermark={st.get('numRowsDroppedByWatermark')}")
    except KeyboardInterrupt:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
