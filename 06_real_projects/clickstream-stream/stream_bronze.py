"""
Bronze stream: file source → Delta append.

Run:
    python stream_bronze.py
"""

import time
from pyspark.sql import functions as F

from session import get_spark, LANDING, BRONZE, CK_ROOT


def main() -> None:
    spark = get_spark("bronze")

    raw = (spark.readStream
        .format("text")                       # read as text — defer JSON parse to silver
        .option("maxFilesPerTrigger", 10)
        .load(LANDING))

    enriched = (raw
        .withColumn("source_file", F.input_file_name())
        .withColumn("bronze_date", F.current_date())
        .withColumn("ingested_at", F.current_timestamp())
        .withColumnRenamed("value", "raw_value"))

    query = (enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CK_ROOT}/bronze")
        .partitionBy("bronze_date")
        .trigger(processingTime="5 seconds")
        .start(BRONZE))

    print(f"[bronze] {LANDING} -> {BRONZE}")
    try:
        while query.isActive:
            time.sleep(15)
            if query.lastProgress:
                p = query.lastProgress
                print(f"  [bronze] batch={p['batchId']}, "
                      f"rows={p['numInputRows']}, "
                      f"rate={p['inputRowsPerSecond']:.1f}/s")
    except KeyboardInterrupt:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
