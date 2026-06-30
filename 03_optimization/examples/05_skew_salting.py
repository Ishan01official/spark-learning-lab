"""
05 — Skew handling with salting

Builds a deliberately skewed join, demonstrates the straggler problem,
then fixes it with salting.

Read the Spark UI Stages tab to see the Max/Median task duration ratio
shrink dramatically after salting.

Run:
    python 05_skew_salting.py
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("skew-demo")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 8)
            .config("spark.sql.adaptive.enabled", False)  # turn off so we see raw skew
            .getOrCreate())


def make_skewed_big(spark, total: int = 1_000_000):
    """80% of rows have user_id=999 (the 'hot' key)."""
    return (spark.range(total)
            .withColumn(
                "user_id",
                F.when(F.col("id") < total * 0.8, F.lit(999))
                 .otherwise((F.col("id") % 100).cast("int"))
            )
            .withColumn("amount", (F.rand() * 100).cast("int")))


def make_small(spark):
    return (spark.range(100)
            .withColumn("user_id", F.col("id"))
            .withColumn("name", F.concat(F.lit("user_"), F.col("id"))))


def time_action(label, action):
    t0 = time.time()
    result = action()
    t1 = time.time()
    print(f"  {label}: {t1-t0:.2f}s, rows={result}")


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    big = make_skewed_big(spark)
    small = make_small(spark)

    # ----- 1) Naive join — expect skew (one task dominates) -----
    print("\n[naive join] one task will be ~80% of total work")
    time_action("naive", lambda: big.join(small, "user_id").count())
    print("  Check Stages tab: Max task duration >> Median (skew!).")

    # ----- 2) Salted join — distribute the hot key -----
    N_SALTS = 10
    print(f"\n[salted join] N_SALTS={N_SALTS}, all keys salted")

    # Salt every row on the big side: rand 0..N-1
    big_salted = big.withColumn("salt", (F.rand() * N_SALTS).cast("int"))

    # Replicate the small side N times, one per salt value
    small_salted = (small
        .withColumn("salt_array", F.array([F.lit(i) for i in range(N_SALTS)]))
        .withColumn("salt", F.explode("salt_array"))
        .drop("salt_array"))

    time_action("salted",
                lambda: big_salted.join(small_salted, ["user_id", "salt"]).count())
    print("  Check Stages tab: Max ~ Median (skew gone).")

    # ----- 3) Selective salting — only the known hot key -----
    HOT = [999]
    print(f"\n[selective salting] only hot keys {HOT} are salted")

    big_sel = big.withColumn(
        "salt",
        F.when(F.col("user_id").isin(HOT), (F.rand() * N_SALTS).cast("int"))
         .otherwise(F.lit(0))
    )
    small_sel = (small
        .withColumn(
            "salt_array",
            F.when(F.col("user_id").isin(HOT),
                   F.array([F.lit(i) for i in range(N_SALTS)]))
             .otherwise(F.array(F.lit(0)))
        )
        .withColumn("salt", F.explode("salt_array"))
        .drop("salt_array"))

    time_action("selective",
                lambda: big_sel.join(small_sel, ["user_id", "salt"]).count())
    print("  Smaller multiplier on the small side — even cheaper than full salting.")

    input("\nPress Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
