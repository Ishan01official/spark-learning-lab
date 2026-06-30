"""
Example 02 — Read and write across formats.

Run:
    python 02_read_write_formats.py

What it demonstrates:
- Generate a small sample DataFrame in memory.
- Write it as CSV, JSON, Parquet (partitioned).
- Read each back and inspect the schemas and file layouts.
- Show how partitioning shows up in the path.
"""

import os
import shutil
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType, DateType,
)


OUT = "/tmp/spark_io_demo"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("02-core-02-io")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def make_sample(spark: SparkSession):
    # Inline data — avoids needing an external file
    data = [
        (1, "alice", "US", "2024-01-15", "10.50"),
        (2, "bob",   "IN", "2024-02-03", "120.00"),
        (3, "cara",  "US", "2024-02-19", "55.75"),
        (4, "dan",   "IN", "2024-03-08", "8.20"),
        (5, "evan",  "US", "2024-03-21", "999.99"),
    ]
    schema = StructType([
        StructField("order_id",    LongType(),         False),
        StructField("customer",    StringType(),       False),
        StructField("country",     StringType(),       False),
        StructField("order_date",  StringType(),       False),     # parse below
        StructField("amount_str",  StringType(),       False),     # cast below
    ])
    df = (spark.createDataFrame(data, schema=schema)
            .withColumn("order_date", F.to_date("order_date"))
            .withColumn("amount", F.col("amount_str").cast(DecimalType(18, 2)))
            .drop("amount_str"))
    return df


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Fresh output dir
    if os.path.exists(OUT):
        shutil.rmtree(OUT)
    os.makedirs(OUT, exist_ok=True)

    df = make_sample(spark)
    df.show(truncate=False)
    df.printSchema()

    # ---- Write: Parquet (partitioned by country) ----
    parquet_path = f"{OUT}/parquet"
    (df.repartition(2, "country")                            # 2 files per country
       .write.mode("overwrite")
       .partitionBy("country")
       .parquet(parquet_path))
    print("\n[Parquet] layout:")
    for root, _, files in os.walk(parquet_path):
        for f in files:
            if not f.startswith("."):
                print(" ", os.path.join(root, f))

    # ---- Write: CSV (single file) ----
    csv_path = f"{OUT}/csv"
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", "true")
       .csv(csv_path))

    # ---- Write: JSON (NDJSON) ----
    json_path = f"{OUT}/json"
    (df.coalesce(1)
       .write.mode("overwrite")
       .json(json_path))

    # ---- Read back ----
    print("\n[Parquet read back, no schema needed]")
    spark.read.parquet(parquet_path).show()

    print("\n[Parquet with filter pushdown — only country=IN files are scanned]")
    spark.read.parquet(parquet_path).filter(F.col("country") == "IN").explain(mode="formatted")

    print("\n[CSV read back — note inferSchema=false → everything is string]")
    spark.read.option("header", "true").csv(csv_path).printSchema()

    print("\n[CSV with explicit schema — types preserved]")
    csv_schema = "order_id LONG, customer STRING, order_date DATE, amount DECIMAL(18,2)"
    (spark.read.option("header", "true").schema(csv_schema).csv(csv_path)
          .printSchema())

    print("\nDone. Output dir:", OUT)
    print("Press Enter to stop...")
    input()
    spark.stop()


if __name__ == "__main__":
    main()
