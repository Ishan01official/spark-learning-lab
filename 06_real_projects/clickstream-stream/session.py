"""
SparkSession factory with Delta + streaming defaults.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app: str) -> SparkSession:
    builder = (SparkSession.builder
        .appName(f"clickstream/{app}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.adaptive.enabled", False)  # off so plan signals are stable
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # RocksDB state store for large state
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"))
    s = configure_spark_with_delta_pip(builder).getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    return s


# Common paths
LANDING = "/tmp/clickstream_demo/landing"
BRONZE  = "/tmp/clickstream_demo/bronze"
SILVER  = "/tmp/clickstream_demo/silver"
GOLD    = "/tmp/clickstream_demo/gold"
CK_ROOT = "/tmp/clickstream_demo/checkpoints"
