"""Smallest possible PySpark program.

Run:
    python 00_setup/examples/01_hello_spark.py

Then open http://localhost:4040 before pressing Enter.
"""
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Build a local SparkSession.

    `local[*]` = run on this machine using every available CPU core.
    On a real cluster, omit `.master(...)`; spark-submit / Databricks sets it.
    """
    spark = (
        SparkSession.builder
        .appName("hello_spark")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = get_spark()
    # spark.range(n) is the fastest way to get a DataFrame: 1 column 'id' from 0..n-1.
    df = spark.range(10)
    print("Schema:")
    df.printSchema()

    print("Rows:")
    df.show()

    print(f"Spark version: {spark.version}")
    print(f"App ID:        {spark.sparkContext.applicationId}")
    print(f"Default parallelism (≈ cores): {spark.sparkContext.defaultParallelism}")

    input("Spark UI is at http://localhost:4040 — press Enter to stop.")
    spark.stop()


if __name__ == "__main__":
    main()
