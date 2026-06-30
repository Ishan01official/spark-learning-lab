"""Lazy evaluation demo.

Build a 5-step pipeline. Print timing at each step.
You'll see steps 1-4 finish in milliseconds because nothing actually runs.
Step 5 (an action) does ALL the work.

Run:
    python 01_fundamentals/examples/03_lazy_eval_demo.py
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("lazy_eval")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def timed(label: str):
    """Context-manager-like: returns a function you call at the end to print elapsed."""
    start = time.perf_counter()
    def done():
        elapsed = (time.perf_counter() - start) * 1000
        print(f"  {label:40s} {elapsed:9.2f} ms")
    return done


def main() -> None:
    spark = get_spark()

    print("Step 1: spark.range(1_000_000)   (transformation — no work)")
    t = timed("range built")
    df = spark.range(1_000_000)
    t()

    print("\nStep 2: filter  (transformation — no work)")
    t = timed("filter added")
    df2 = df.filter(F.col("id") % 7 == 0)
    t()

    print("\nStep 3: withColumn  (transformation — no work)")
    t = timed("withColumn added")
    df3 = df2.withColumn("squared", F.col("id") * F.col("id"))
    t()

    print("\nStep 4: groupBy + agg  (transformation — STILL no work)")
    t = timed("agg planned")
    df4 = df3.groupBy(F.col("id") % 3).agg(F.sum("squared").alias("s"))
    t()

    print("\n  printSchema() and explain() are also free (no execution):")
    df4.printSchema()
    df4.explain()

    print("\nStep 5: count()  (ACTION — entire pipeline runs now)")
    t = timed("action: count")
    n = df4.count()
    t()
    print(f"  Got {n} rows")

    # Same pipeline, same action, but with cache: second action is faster
    df_filtered = df.filter(F.col("id") % 7 == 0).cache()  # mark for cache
    print("\nFirst count on cached DataFrame (materializes cache):")
    t = timed("first cached count")
    df_filtered.count()
    t()

    print("Second count on cached DataFrame (served from RAM):")
    t = timed("second cached count")
    df_filtered.count()
    t()

    input("\nOpen http://localhost:4040 → Storage tab to see the cache. Press Enter to stop.")
    spark.stop()


if __name__ == "__main__":
    main()
