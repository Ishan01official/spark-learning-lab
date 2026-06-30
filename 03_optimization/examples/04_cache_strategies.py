"""
04 — Cache strategies

Three demos:
  1. Cache is lazy — needs an action to materialize.
  2. Without caching, the upstream pipeline reruns on every action.
  3. unpersist() releases memory.

Run with Spark UI open at http://localhost:4040 to see the Storage tab populate.

Run:
    python 04_cache_strategies.py
"""

import time
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("cache-demo")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 4)
            .getOrCreate())


def expensive_pipeline(spark):
    """A multi-stage transform we don't want to repeat."""
    return (spark.range(5_000_000)
            .withColumn("g", F.col("id") % 100)
            .withColumn("v", F.col("id") * 3 + 1)
            .groupBy("g")
            .agg(F.sum("v").alias("sum_v"),
                 F.avg("v").alias("avg_v"),
                 F.count("*").alias("n")))


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ---- 1) WITHOUT cache: pipeline runs twice ----
    print("\n[no cache] running pipeline twice")
    df = expensive_pipeline(spark)

    t0 = time.time()
    n1 = df.count()                # full pipeline run #1
    n2 = df.filter("avg_v > 100").count()  # full pipeline run #2 — recomputed
    t1 = time.time()
    print(f"  rows={n1}, filtered={n2}, time={t1-t0:.2f}s")

    # ---- 2) WITH cache: second action is fast ----
    print("\n[cache] caching then running twice")
    df2 = expensive_pipeline(spark).cache()
    df2.count()  # materialize the cache (mandatory — cache is lazy!)

    t0 = time.time()
    n1 = df2.count()
    n2 = df2.filter("avg_v > 100").count()
    t1 = time.time()
    print(f"  rows={n1}, filtered={n2}, time={t1-t0:.2f}s   <- expect much faster")

    print(f"  is_cached     = {df2.is_cached}")
    print(f"  storage_level = {df2.storageLevel}")

    # ---- 3) Storage level choice: MEMORY_AND_DISK_SER ----
    print("\n[MEMORY_AND_DISK_SER] explicit level")
    df3 = expensive_pipeline(spark)
    df3.persist(StorageLevel.MEMORY_AND_DISK_SER)
    df3.count()
    print(f"  storage_level = {df3.storageLevel}")

    # ---- 4) Always unpersist when done ----
    df2.unpersist()
    df3.unpersist()
    print("\nunpersisted both DataFrames")

    input("\nPress Enter to stop Spark (check the Storage tab before you do!)...")
    spark.stop()


if __name__ == "__main__":
    main()
