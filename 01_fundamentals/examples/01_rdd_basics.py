"""RDD basics — see how Spark's original abstraction behaves.

Run:
    python 01_fundamentals/examples/01_rdd_basics.py
"""
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("rdd_basics")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = get_spark()
    sc = spark.sparkContext

    # --- 1) Create an RDD from a Python collection ---
    # numSlices=4 → 4 partitions → up to 4 tasks running in parallel
    nums = sc.parallelize(range(1, 11), numSlices=4)
    print(f"Partitions: {nums.getNumPartitions()}")
    print(f"Elements:   {nums.collect()}")  # collect() is an ACTION

    # --- 2) Narrow transformations (no shuffle) ---
    doubled = nums.map(lambda x: x * 2)
    evens = doubled.filter(lambda x: x % 4 == 0)
    print(f"\nNarrow (map then filter): {evens.collect()}")

    # See what's in each partition (this is a per-partition op, narrow)
    print("\nPer-partition view of the original RDD:")
    for i, part in enumerate(nums.glom().collect()):
        print(f"  partition {i}: {part}")

    # --- 3) Wide transformation: groupByKey vs reduceByKey ---
    # Key-value RDD: (word, 1) pairs
    words = sc.parallelize(
        ["spark", "spark", "is", "fast", "spark", "is", "fun"]
    ).map(lambda w: (w, 1))

    # reduceByKey reduces LOCALLY per partition first, then shuffles small partial sums
    counts_efficient = words.reduceByKey(lambda a, b: a + b).collect()
    print(f"\nreduceByKey (efficient): {sorted(counts_efficient)}")

    # groupByKey ships every value across the network, then sums.
    # Functionally equivalent here. On a real dataset with high duplication, reduceByKey
    # is 10-100x faster. NEVER use groupByKey unless you have a good reason.
    counts_naive = words.groupByKey().mapValues(lambda vs: sum(vs)).collect()
    print(f"groupByKey (naive):     {sorted(counts_naive)}")

    # --- 4) Lineage — the secret of fault tolerance ---
    print("\nLineage of `evens`:")
    print(evens.toDebugString().decode())

    # --- 5) Action that does NOT collect everything ---
    print(f"\ntake(3): {nums.take(3)}")     # only computes 1 partition
    print(f"first(): {nums.first()}")
    print(f"count(): {nums.count()}")

    input("\nOpen http://localhost:4040 then press Enter to stop.")
    spark.stop()


if __name__ == "__main__":
    main()
