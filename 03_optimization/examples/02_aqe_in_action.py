"""
02 — AQE in action

Shows AQE switching a SortMergeJoin to a BroadcastHashJoin at runtime
based on the actual filtered size of one side.

Run:
    python 02_aqe_in_action.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark(aqe: bool) -> SparkSession:
    return (SparkSession.builder
            .appName(f"aqe-demo-{aqe}")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 200)
            .config("spark.sql.adaptive.enabled", aqe)
            .config("spark.sql.autoBroadcastJoinThreshold", "100MB")
            .getOrCreate())


def run(aqe: bool) -> None:
    spark = get_spark(aqe)
    spark.sparkContext.setLogLevel("WARN")

    # Big side: 5M rows.
    big = (spark.range(5_000_000)
                .withColumn("user_id", F.col("id") % 1_000_000)
                .withColumn("amount", (F.rand() * 100).cast("int")))

    # Small side: nominally 1M users, but we filter it down to ~1k.
    small = (spark.range(1_000_000)
                  .withColumn("user_id", F.col("id"))
                  .withColumn("country", F.expr("element_at(array('US','DE','JP'), int(rand()*3)+1)"))
                  .filter(F.col("country") == "JP")    # ~33% → ~330k
                  .filter(F.col("user_id") < 1000)     # final size ~330 rows
                  .drop("id"))

    print(f"\n{'=' * 60}")
    print(f"AQE = {aqe}")
    print(f"{'=' * 60}")

    # The static planner sees small as 1M rows (~25 MB) — borderline; with stats
    # missing it may not broadcast. AQE sees the actual ~330 rows after the
    # filters run and switches to broadcast at runtime.
    joined = big.join(small, "user_id")
    joined.count()  # materialize so AQE has a chance to rewrite

    # Re-run explain AFTER execution to see the final (possibly AQE-rewritten) plan.
    joined.explain()

    spark.stop()


def main() -> None:
    print("\n*** Run 1: AQE OFF — expect SortMergeJoin")
    run(aqe=False)

    print("\n*** Run 2: AQE ON — expect BroadcastHashJoin after rewrite")
    run(aqe=True)


if __name__ == "__main__":
    main()
