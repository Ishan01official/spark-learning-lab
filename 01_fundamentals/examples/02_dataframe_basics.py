"""DataFrame basics — the daily-driver API.

Run:
    python 01_fundamentals/examples/02_dataframe_basics.py

Walks through:
  - creating a DataFrame with a schema
  - column expressions, withColumn, select, filter
  - groupBy + agg
  - explain (logical vs physical plan)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType,
)


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("df_basics")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = get_spark()

    # --- 1) Declare a schema explicitly. Don't rely on inferSchema in production. ---
    schema = StructType([
        StructField("order_id", IntegerType(), nullable=False),
        StructField("country",  StringType(),  nullable=False),
        StructField("status",   StringType(),  nullable=False),
        StructField("amount",   DoubleType(),  nullable=False),
    ])

    data = [
        (1, "IN", "paid",     120.0),
        (2, "IN", "pending",   80.0),
        (3, "US", "paid",     300.0),
        (4, "US", "paid",      50.0),
        (5, "DE", "paid",     450.0),
        (6, "DE", "cancelled", 90.0),
        (7, "IN", "paid",     220.0),
    ]
    df = spark.createDataFrame(data, schema=schema)

    print("Schema:")
    df.printSchema()
    df.show()

    # --- 2) Narrow transformations: filter, select, withColumn ---
    paid = (
        df.filter(F.col("status") == "paid")          # narrow
          .withColumn("amount_usd",                   # narrow: adds a derived column
                      F.col("amount") * F.lit(1.0))   # F.lit wraps a Python value
          .select("order_id", "country", "amount", "amount_usd")
    )
    paid.show()

    # --- 3) Wide transformation: groupBy + agg (forces a shuffle) ---
    revenue = (
        paid.groupBy("country")
            .agg(
                F.count("*").alias("order_count"),
                F.sum("amount").alias("revenue"),
                F.avg("amount").alias("avg_order_value"),
            )
            .orderBy(F.desc("revenue"))               # another shuffle (global sort)
    )
    revenue.show()

    # --- 4) See the plan. Look for "Exchange" nodes — those are shuffles. ---
    print("=" * 60)
    print("Physical plan:")
    revenue.explain()  # default = physical plan only

    print("=" * 60)
    print("Formatted plan (all four phases):")
    revenue.explain(mode="formatted")

    input("\nOpen http://localhost:4040 then press Enter to stop.")
    spark.stop()


if __name__ == "__main__":
    main()
