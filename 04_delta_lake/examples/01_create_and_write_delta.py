"""
01 — Create and write a Delta table

Demonstrates:
  - Building a SparkSession with Delta support
  - Path-based vs managed tables
  - Append / overwrite / replaceWhere / dynamic partition overwrite
  - Inspecting history() and detail()

Run:
    python 01_create_and_write_delta.py
"""

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    builder = (SparkSession.builder
        .appName("delta-create-write")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


TABLE_PATH = "/tmp/delta_demo/orders"


def make_orders(spark, day: str, n: int):
    return (spark.range(n)
            .withColumn("order_id", F.col("id") + F.lit(hash(day) % 100000))
            .withColumn("customer_id", (F.rand() * 1000).cast("int"))
            .withColumn("amount", (F.rand() * 500).cast("decimal(18,2)"))
            .withColumn("status", F.expr("element_at(array('NEW','PAID','SHIPPED'), int(rand()*3)+1)"))
            .withColumn("event_date", F.lit(day).cast("date"))
            .drop("id"))


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ----- 1) Initial write (creates the table) -----
    print("\n[1] Creating table with first day's data")
    day1 = make_orders(spark, "2024-09-01", 1000)
    (day1.write.format("delta")
         .mode("overwrite")
         .partitionBy("event_date")
         .save(TABLE_PATH))

    dt = DeltaTable.forPath(spark, TABLE_PATH)
    dt.detail().select("format", "location", "numFiles", "sizeInBytes",
                       "partitionColumns").show(truncate=False)

    # ----- 2) Append a second day -----
    print("\n[2] Appending second day")
    day2 = make_orders(spark, "2024-09-02", 800)
    (day2.write.format("delta")
         .mode("append")
         .save(TABLE_PATH))

    print(f"Row count after 2 appends: {spark.read.format('delta').load(TABLE_PATH).count()}")

    # ----- 3) replaceWhere — surgical overwrite of one day -----
    print("\n[3] Replacing just 2024-09-01 (replaceWhere)")
    day1_fixed = make_orders(spark, "2024-09-01", 500)  # different size, simulating a fix
    (day1_fixed.write.format("delta")
              .mode("overwrite")
              .option("replaceWhere", "event_date = '2024-09-01'")
              .save(TABLE_PATH))

    counts = (spark.read.format("delta").load(TABLE_PATH)
              .groupBy("event_date").count()
              .orderBy("event_date"))
    counts.show()
    # Expect: 2024-09-01 = 500, 2024-09-02 = 800

    # ----- 4) Dynamic partition overwrite — same idea, less explicit -----
    print("\n[4] Dynamic partition overwrite: write day-3 batch")
    day3 = make_orders(spark, "2024-09-03", 600)
    # partitionOverwriteMode=dynamic was set on the SparkSession;
    # this only overwrites partitions present in `day3` (i.e., 2024-09-03)
    (day3.write.format("delta")
         .mode("overwrite")
         .partitionBy("event_date")
         .save(TABLE_PATH))

    print("Final partition row counts:")
    (spark.read.format("delta").load(TABLE_PATH)
        .groupBy("event_date").count().orderBy("event_date").show())

    # ----- 5) Inspect history -----
    print("\n[5] Table history (most recent first)")
    dt.history().select("version", "timestamp", "operation",
                        "operationParameters").show(truncate=False)

    input("\nPress Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
