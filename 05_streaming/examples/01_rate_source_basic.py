"""
01 — Streaming hello world with the rate source

Uses the built-in `rate` source to generate synthetic data, applies a
simple transformation, and writes to the console sink.

Run:
    python 01_rate_source_basic.py
"""

from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("streaming-rate-basic")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 4)
            .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # `rate` emits rows with two columns: timestamp, value (monotonic long).
    events = (spark.readStream
        .format("rate")
        .option("rowsPerSecond", 100)
        .load())

    enriched = (events
        .withColumn("bucket", F.col("value") % 5)
        .withColumn("category",
                    F.when(F.col("value") % 2 == 0, "even").otherwise("odd")))

    counts = (enriched
        .groupBy("bucket", "category")
        .count())

    # `complete` output mode is required here because we have an aggregation
    # without a watermark.
    query = (counts.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start())

    print("Streaming started. Press Ctrl+C to stop.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
