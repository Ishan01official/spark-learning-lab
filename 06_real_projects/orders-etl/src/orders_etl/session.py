"""
SparkSession factory with Delta support and reasonable defaults.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    builder = (SparkSession.builder
        .appName(f"orders-etl/{app_name}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 8)
        .config("spark.sql.adaptive.enabled", True)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
