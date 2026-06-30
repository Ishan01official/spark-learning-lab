"""
Example 06 — Join strategies: SMJ vs BHJ vs SHJ, with explain().

Run:
    python 06_joins_demo.py

Show:
- The default sort-merge plan for a big × big join.
- BroadcastHashJoin via the broadcast hint.
- left_semi vs left_anti vs left outer.
"""

from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
              .appName("02-core-06-joins")
              .master("local[*]")
              .config("spark.sql.shuffle.partitions", "8")
              .config("spark.sql.autoBroadcastJoinThreshold", "-1")    # disable auto-broadcast to force SMJ on demo 1
              .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ---- "Big" facts and a small dim table ----
    orders = (spark.range(0, 20_000)
                .withColumnRenamed("id", "order_id")
                .withColumn("country", F.element_at(
                    F.array(F.lit("US"), F.lit("IN"), F.lit("DE"), F.lit("BR")),
                    ((F.col("order_id") % 4) + 1).cast("int")))
                .withColumn("amount", (F.rand(seed=1) * 100).cast("double")))

    countries = spark.createDataFrame(
        [("US", "United States"), ("IN", "India"), ("DE", "Germany"), ("BR", "Brazil")],
        schema="country STRING, country_full STRING",
    )

    # ---- 1) SortMergeJoin (auto-broadcast disabled) ----
    print("\n[1] SortMergeJoin (autoBroadcastJoinThreshold=-1):")
    smj = orders.join(countries, "country")
    smj.explain(mode="formatted")
    print("Row count:", smj.count())

    # ---- 2) Broadcast hint forces BHJ ----
    print("\n[2] BroadcastHashJoin via broadcast() hint:")
    bhj = orders.join(F.broadcast(countries), "country")
    bhj.explain(mode="formatted")
    print("Row count:", bhj.count())

    # ---- 3) left_semi vs left_anti ----
    favored = spark.createDataFrame([("US",), ("IN",)], schema="country STRING")

    print("\n[3a] left_semi — rows in orders whose country IS in favored:")
    (orders.join(F.broadcast(favored), "country", "left_semi").groupBy("country").count().show())

    print("[3b] left_anti — rows in orders whose country IS NOT in favored:")
    (orders.join(F.broadcast(favored), "country", "left_anti").groupBy("country").count().show())

    # ---- 4) Skew demo (artificial) ----
    skewed = orders.withColumn(
        "country",
        F.when(F.rand(seed=99) < 0.7, "US").otherwise(F.col("country")))
    print("\n[4] Skewed country distribution:")
    skewed.groupBy("country").count().orderBy(F.desc("count")).show()

    # AQE on this small input won't help noticeably, but the principle is:
    # On a big cluster, set spark.sql.adaptive.skewJoin.enabled=true and watch the
    # Stages tab in Spark UI for split partitions.

    print("Press Enter to stop...")
    input()
    spark.stop()


if __name__ == "__main__":
    main()
