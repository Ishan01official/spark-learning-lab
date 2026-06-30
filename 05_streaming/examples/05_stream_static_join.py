"""
05 — Stream-static join

A streaming event source enriched with a static `users` dimension table.

Run:
    python 05_stream_static_join.py
"""

import time
from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("stream-static-join")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 4)
            .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Build a static users dim (in real life: spark.read.format("delta").load(...))
    users = spark.createDataFrame(
        [(i, f"user_{i}", ["US","DE","JP","IN","BR"][i % 5]) for i in range(20)],
        ["user_id", "name", "country"]
    )

    # Streaming events
    events = (spark.readStream
        .format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .withColumn("user_id", (F.col("value") % 20).cast("long"))
        .withColumn("event_type", F.element_at(
            F.array(F.lit("click"), F.lit("view"), F.lit("buy")),
            (F.col("value") % 3 + 1).cast("int")))
        .withColumn("event_time", F.col("timestamp"))
        .drop("value", "timestamp"))

    # Broadcast the static side; left-outer to keep events with no user match
    enriched = events.join(F.broadcast(users), "user_id", "left")

    query = (enriched.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start())

    print("Stream-static join. Press Ctrl+C to stop.\n")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
