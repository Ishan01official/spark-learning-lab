"""
Example 03 — Explicit schema vs inference (and the cost difference).

Run:
    python 03_schema_explicit.py
"""

import os
import time
import shutil
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType, TimestampType,
)


OUT = "/tmp/schema_demo"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("02-core-03-schema")
        .master("local[*]")
        .getOrCreate()
    )


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    if os.path.exists(OUT):
        shutil.rmtree(OUT)
    os.makedirs(OUT, exist_ok=True)

    # ---- Generate a moderately-sized CSV ----
    n = 200_000
    src = (spark.range(n)
             .withColumn("customer_id", (F.col("id") % 1000).cast("long"))
             .withColumn("country", F.element_at(
                 F.array(F.lit("US"), F.lit("IN"), F.lit("DE"), F.lit("BR")),
                 ((F.col("id") % 4) + 1).cast("int")))
             .withColumn("amount", (F.rand(seed=42) * 1000).cast(DecimalType(18, 2)))
             .withColumn("created_at", F.current_timestamp())
             .withColumnRenamed("id", "order_id"))
    csv_path = f"{OUT}/orders_csv"
    src.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    print(f"Wrote {n:,} rows to {csv_path}")

    # ---- Read 1: inferSchema=true (slow path — scans twice) ----
    print("\n[1] inferSchema=true")
    t0 = time.time()
    df_inf = (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_path))
    df_inf.count()                        # force the read
    print(f"  took {time.time()-t0:.2f}s")
    df_inf.printSchema()

    # ---- Read 2: explicit schema (fast path) ----
    print("\n[2] explicit schema (DDL string)")
    ddl = "order_id LONG, customer_id LONG, country STRING, amount DECIMAL(18,2), created_at TIMESTAMP"
    t0 = time.time()
    df_exp = (spark.read
                .option("header", "true")
                .schema(ddl)
                .csv(csv_path))
    df_exp.count()
    print(f"  took {time.time()-t0:.2f}s")
    df_exp.printSchema()

    # ---- Read 3: explicit schema (programmatic) ----
    print("\n[3] explicit schema (StructType)")
    schema = StructType([
        StructField("order_id",    LongType(),         False),
        StructField("customer_id", LongType(),         False),
        StructField("country",     StringType(),       False),
        StructField("amount",      DecimalType(18, 2), True),
        StructField("created_at",  TimestampType(),    True),
    ])
    t0 = time.time()
    df_pg = (spark.read
                .option("header", "true")
                .schema(schema)
                .csv(csv_path))
    df_pg.count()
    print(f"  took {time.time()-t0:.2f}s")

    print("\nObserve: the inferred read is ~2× the explicit read on this small input.")
    print("On 100 GB+ files the gap is huge — and you also get correct types every time.\n")

    print("Press Enter to stop...")
    input()
    spark.stop()


if __name__ == "__main__":
    main()
