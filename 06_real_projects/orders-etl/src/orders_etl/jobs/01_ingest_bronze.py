"""
01 — Ingest landing JSON to bronze Delta.

Bronze design:
  - Append-only, partitioned by `batch_date`.
  - Idempotent via `replaceWhere` on batch_date.
  - Minimal schema enforcement: only `_corrupt_record` filter.
  - Adds tracking columns: ingested_at, source_file, batch_id.

Run:
    python -m orders_etl.jobs.01_ingest_bronze --batch-date 2024-09-15
"""

import argparse
import sys
import uuid
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from orders_etl.config import CONFIG
from orders_etl.session import get_spark


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--batch-date", required=True,
                   help="YYYY-MM-DD; identifies the source file to ingest")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("01_ingest_bronze")

    source_file = f"{CONFIG.landing_dir}/orders_{args.batch_date.replace('-', '')}.json"
    if not Path(source_file).exists():
        print(f"ERROR: source file not found: {source_file}", file=sys.stderr)
        sys.exit(1)

    batch_id = str(uuid.uuid4())
    print(f"[bronze] ingest {source_file}, batch_id={batch_id}")

    # Read raw JSON as STRING so we don't drop malformed records
    raw = (spark.read
        .text(source_file)
        .withColumn("source_file", F.lit(source_file))
        .withColumn("batch_date", F.lit(args.batch_date).cast("date"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumnRenamed("value", "raw_value"))

    # Idempotent write: replace any data for this batch_date
    (raw.write.format("delta")
        .mode("overwrite")
        .partitionBy("batch_date")
        .option("replaceWhere", f"batch_date = '{args.batch_date}'")
        .save(CONFIG.bronze_path))

    cnt = spark.read.format("delta").load(CONFIG.bronze_path) \
        .where(F.col("batch_date") == args.batch_date).count()
    print(f"[bronze] wrote {cnt} rows for batch_date={args.batch_date}")

    spark.stop()


if __name__ == "__main__":
    main()
