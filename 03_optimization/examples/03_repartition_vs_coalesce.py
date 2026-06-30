"""
03 — repartition vs coalesce

Demonstrates:
  - coalesce is narrow (no shuffle), repartition is wide (shuffle).
  - coalesce can produce skewed partitions; repartition is balanced.
  - When to use each before writing.

Run:
    python 03_repartition_vs_coalesce.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("repartition-vs-coalesce")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 8)
            .getOrCreate())


def show_partition_sizes(df, label: str) -> None:
    # Count rows per partition (use mapPartitions on the RDD).
    counts = df.rdd.glom().map(len).collect()
    print(f"\n{label}:  num_partitions={len(counts)}  sizes={counts}")


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.range(0, 1_000_000).repartition(16)  # start with 16 balanced partitions
    show_partition_sizes(df, "after repartition(16) — balanced")

    print("\nPLAN: df.coalesce(4)")
    df.coalesce(4).explain()

    print("\nPLAN: df.repartition(4)")
    df.repartition(4).explain()

    show_partition_sizes(df.coalesce(4),   "after coalesce(4)   — concatenated, possibly skewed")
    show_partition_sizes(df.repartition(4), "after repartition(4) — hash-shuffled, balanced")

    # The classic small-files use case.
    # Imagine you've just done 200-way shuffle (groupBy) and want to write 4 files.
    print("\nWriting 4 files using repartition (balanced):")
    (df.repartition(4)
       .write.mode("overwrite")
       .parquet("/tmp/rp_demo"))

    print("Writing 4 files using coalesce (cheaper, possibly uneven):")
    (df.coalesce(4)
       .write.mode("overwrite")
       .parquet("/tmp/co_demo"))

    print("\nRule of thumb:")
    print("  - coalesce: reducing partitions, no shuffle, OK if uneven.")
    print("  - repartition: balanced output (and only choice if going UP in count).")

    input("\nPress Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
