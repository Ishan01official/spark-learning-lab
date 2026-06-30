"""Narrow vs wide transformations — see the shuffle.

Run:
    python 01_fundamentals/examples/04_narrow_wide_demo.py

Then open http://localhost:4040 → SQL tab → click the latest query → look for
the `Exchange` nodes. Each Exchange is a shuffle = a stage boundary.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("narrow_wide")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = get_spark()

    # 1M rows, 8 starting partitions
    df = (
        spark.range(1_000_000)
             .withColumn("country", F.element_at(
                 F.split(F.lit("IN,US,DE,UK,FR,JP"), ","),
                 (F.col("id") % 6 + 1).cast("int")
             ))
             .withColumn("amount", (F.col("id") % 1000).cast("double"))
             .repartition(8)
    )

    # --- Narrow-only pipeline: stays in ONE stage ---
    print("=" * 60)
    print("Narrow only (filter + withColumn):")
    narrow = df.filter("country in ('IN','US')").withColumn("amount2", F.col("amount") * 2)
    narrow.explain()           # no Exchange node
    print(f"  count: {narrow.count()}")   # 1 job, 1 stage

    # --- Wide pipeline: groupBy forces a shuffle (one Exchange) ---
    print("=" * 60)
    print("With groupBy (1 shuffle):")
    wide1 = df.groupBy("country").agg(F.sum("amount").alias("rev"))
    wide1.explain()             # see one Exchange
    wide1.show()                 # 1 job, 2 stages

    # --- Wider pipeline: groupBy + orderBy = TWO shuffles ---
    print("=" * 60)
    print("groupBy + orderBy (2 shuffles):")
    wide2 = wide1.orderBy(F.desc("rev"))
    wide2.explain()             # two Exchanges
    wide2.show()                 # 1 job, 3 stages

    input("\nOpen http://localhost:4040 → SQL tab to inspect each query. Press Enter to stop.")
    spark.stop()


if __name__ == "__main__":
    main()
