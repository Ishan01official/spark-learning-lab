"""
Example 01 — SparkSession configuration.

Run:
    python 01_spark_session.py

Demonstrates the builder pattern and inspects what we got back.
"""

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("02-core-01-session")
        .master("local[*]")                                     # all local cores
        .config("spark.sql.shuffle.partitions", "8")            # small data → small shuffle width
        .config("spark.sql.adaptive.enabled", "true")           # AQE
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=== Spark version ===")
    print(spark.version)

    print("\n=== Application ID & name ===")
    print(spark.sparkContext.applicationId, "/", spark.sparkContext.appName)

    print("\n=== Key configs ===")
    for key in [
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.sql.files.maxPartitionBytes",
        "spark.sql.session.timeZone",
        "spark.serializer",
    ]:
        print(f"  {key:55s} = {spark.conf.get(key)}")

    print("\n=== Spark UI ===")
    print(f"Open in browser: {spark.sparkContext.uiWebUrl}")

    print("\nPress Enter to stop the session...")
    input()
    spark.stop()


if __name__ == "__main__":
    main()
