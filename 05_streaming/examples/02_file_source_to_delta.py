"""
02 — File source -> Delta sink

Watches a directory for new JSON files and writes them to a Delta table.
This is the canonical "S3 landing zone -> bronze layer" pattern.

Run in TWO terminals:

  Terminal 1: python 02_file_source_to_delta.py
  Terminal 2: drop JSON files into /tmp/streaming_demo/landing/
              (the script's drop_test_files() helper does this for you)
"""

import json
import os
import time
from pathlib import Path
from threading import Thread

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, LongType, TimestampType


LANDING = "/tmp/streaming_demo/landing"
BRONZE  = "/tmp/streaming_demo/bronze"
CHECKPOINT = "/tmp/streaming_demo/ck_bronze"


def get_spark() -> SparkSession:
    builder = (SparkSession.builder
        .appName("file-to-delta")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def drop_test_files(stop_event):
    """Background helper to drop one new JSON file every 3 seconds."""
    Path(LANDING).mkdir(parents=True, exist_ok=True)
    i = 0
    while not stop_event.is_set():
        path = Path(LANDING) / f"events_{int(time.time())}.json"
        records = [
            {"user_id": (i * 7) % 1000,
             "event_type": ["click", "view", "buy"][i % 3],
             "amount": (i * 13) % 500,
             "event_time": time.strftime("%Y-%m-%d %H:%M:%S")}
            for _ in range(10)
        ]
        with path.open("w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
        print(f"  [helper] wrote {path}")
        i += 1
        time.sleep(3)


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Mandatory: schema for the file source
    schema = (StructType()
        .add("user_id", LongType())
        .add("event_type", StringType())
        .add("amount", LongType())
        .add("event_time", TimestampType()))

    Path(LANDING).mkdir(parents=True, exist_ok=True)

    events = (spark.readStream
        .format("json")
        .schema(schema)
        .option("maxFilesPerTrigger", 5)
        .load(LANDING))

    enriched = (events
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("amount_usd", F.col("amount") / 100.0))

    query = (enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .start(BRONZE))

    # Start file-drop helper in background
    import threading
    stop = threading.Event()
    helper = Thread(target=drop_test_files, args=(stop,), daemon=True)
    helper.start()

    print(f"\nStreaming from {LANDING} -> {BRONZE}")
    print("Helper is dropping JSON files every 3s. Press Ctrl+C to stop.\n")

    try:
        while query.isActive:
            time.sleep(10)
            prog = query.lastProgress
            if prog:
                print(f"  [progress] batch {prog['batchId']}: "
                      f"{prog['numInputRows']} rows, "
                      f"{prog['inputRowsPerSecond']:.1f} rps")
    except KeyboardInterrupt:
        stop.set()
        query.stop()

    # Show what landed in bronze
    print("\nFinal bronze table:")
    spark.read.format("delta").load(BRONZE).groupBy("event_type").count().show()
    spark.stop()


if __name__ == "__main__":
    main()
