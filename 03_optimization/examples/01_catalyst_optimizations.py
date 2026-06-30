"""
01 — Catalyst in action

Shows three Catalyst optimizations by running them and reading explain() output:
  1. Predicate pushdown into Parquet
  2. Projection pruning
  3. UDF blocking the optimizer

Run:
    python 01_catalyst_optimizations.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def get_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("catalyst-demo")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 4)
            .config("spark.sql.adaptive.enabled", False)  # turn off so the static plan is what we see
            .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Build a 1M-row table with 5 columns. Write as Parquet so pushdown applies.
    (spark.range(1_000_000)
        .withColumn("amount", (F.rand() * 1000).cast("int"))
        .withColumn("country", F.expr("element_at(array('US','DE','JP','IN','BR'), int(rand()*5)+1)"))
        .withColumn("status", F.expr("element_at(array('OK','OK','OK','OK','FAIL'), int(rand()*5)+1)"))
        .withColumn("ts", F.current_timestamp())
        .write.mode("overwrite").parquet("/tmp/catalyst_demo"))

    df = spark.read.parquet("/tmp/catalyst_demo")

    print("\n" + "=" * 60)
    print("1) PREDICATE PUSHDOWN — filter pushed into Parquet scan")
    print("=" * 60)
    # Filter should appear in PushedFilters on the FileScan node.
    df.filter(F.col("amount") > 800).select("id").explain()

    print("\n" + "=" * 60)
    print("2) PROJECTION PRUNING — only the columns we select are read")
    print("=" * 60)
    # Look at ReadSchema in the FileScan: should be id, amount only.
    df.select("id", "amount").filter(F.col("amount") > 500).explain()

    print("\n" + "=" * 60)
    print("3) UDF blocks the optimizer — filter NOT pushed down")
    print("=" * 60)
    # A Python UDF after the scan blocks pushdown and codegen.
    @F.udf(IntegerType())
    def double_amount(x: int) -> int:
        return x * 2

    (df.withColumn("doubled", double_amount("amount"))
       .filter(F.col("doubled") > 800)
       .select("id")
       .explain())

    print("\n" + "=" * 60)
    print("4) FIX — same logic using F.expr instead of a Python UDF")
    print("=" * 60)
    # No UDF, so the filter can be pushed down.
    (df.withColumn("doubled", F.col("amount") * 2)
       .filter(F.col("doubled") > 800)
       .select("id")
       .explain())

    input("\nPress Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
