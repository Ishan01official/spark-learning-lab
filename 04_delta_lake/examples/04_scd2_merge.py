"""
04 — SCD Type 2 with MERGE

Implements the classic "two-row trick" for SCD2:
  - When an attribute changes for an existing entity, close the current row
    (set valid_to, is_current=false) AND insert a new row (is_current=true).

The trick: pre-stage two records per change (a "close" and a "new"), then
MERGE both at once.

Run:
    python 04_scd2_merge.py
"""

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    builder = (SparkSession.builder
        .appName("delta-scd2")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


PATH = "/tmp/delta_demo/scd2_customers"


def init_table(spark):
    """Create the initial dimension table with 3 customers."""
    initial = spark.createDataFrame([
        (1, "alice", "US",  "2024-01-01", None, True),
        (2, "bob",   "DE",  "2024-01-01", None, True),
        (3, "carol", "JP",  "2024-01-01", None, True),
    ], ["customer_id", "name", "country", "valid_from", "valid_to", "is_current"])

    initial.write.format("delta").mode("overwrite").save(PATH)
    return DeltaTable.forPath(spark, PATH)


def apply_scd2_merge(spark, target: DeltaTable, updates):
    """
    Apply SCD2 changes for the given updates DataFrame.
    `updates` schema: customer_id, name, country, event_time
    """

    # Step 1: identify rows in updates that are actually CHANGES vs current state.
    # A change = same customer_id, but some attribute differs from the current row.
    target_current = (target.toDF()
                          .alias("t")
                          .where("is_current = true"))

    changes = (updates.alias("s")
        .join(target_current, F.col("s.customer_id") == F.col("t.customer_id"), "left")
        .where("t.customer_id IS NULL OR s.country != t.country OR s.name != t.name")
        .select("s.*"))

    # Step 2: build the staged DataFrame with TWO rows per existing-customer change:
    #   - one with merge_key = customer_id  -> matches the current row, will CLOSE it
    #   - one with merge_key = NULL          -> won't match, will INSERT the new row
    # For brand-new customers, only the INSERT row is needed.

    existing_customer_ids = [r.customer_id for r in target_current.select("customer_id").collect()]

    # "close" rows — only for existing customers
    close_rows = (changes
        .where(F.col("customer_id").isin(existing_customer_ids))
        .withColumn("merge_key", F.col("customer_id")))

    # "insert" rows — for all changes (existing + brand-new), with merge_key = NULL
    new_rows = changes.withColumn("merge_key", F.lit(None).cast("long"))

    staged = close_rows.unionByName(new_rows)

    # Step 3: perform the MERGE
    (target.alias("t")
        .merge(staged.alias("s"),
               "t.customer_id = s.merge_key AND t.is_current = true")
        # Close existing current row when the staged row has merge_key (close row)
        .whenMatchedUpdate(set={
            "valid_to":  "s.event_time",
            "is_current": "false",
        })
        # Insert new row for every "insert" staged row (merge_key NULL, doesn't match)
        .whenNotMatchedInsert(values={
            "customer_id": "s.customer_id",
            "name":        "s.name",
            "country":     "s.country",
            "valid_from":  "s.event_time",
            "valid_to":    "null",
            "is_current":  "true",
        })
        .execute())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("[init] Initial dimension table")
    target = init_table(spark)
    target.toDF().orderBy("customer_id", "valid_from").show(truncate=False)

    # Batch 1: bob moves from DE to FR, dave (new customer) appears
    print("\n[batch 1] bob: DE -> FR; dave (NEW)")
    batch1 = spark.createDataFrame([
        (2, "bob",  "FR", "2024-06-01"),
        (4, "dave", "BR", "2024-06-01"),
    ], ["customer_id", "name", "country", "event_time"])

    apply_scd2_merge(spark, target, batch1)
    target.toDF().orderBy("customer_id", "valid_from").show(truncate=False)

    # Batch 2: alice changes name (typo fix), carol moves JP -> KR
    print("\n[batch 2] alice: name fix; carol: JP -> KR")
    batch2 = spark.createDataFrame([
        (1, "Alice", "US", "2024-09-01"),
        (3, "carol", "KR", "2024-09-01"),
    ], ["customer_id", "name", "country", "event_time"])

    apply_scd2_merge(spark, target, batch2)
    target.toDF().orderBy("customer_id", "valid_from").show(truncate=False)

    print("\nFor a point-in-time view (e.g. 2024-07-01):")
    (spark.read.format("delta").load(PATH)
        .where("valid_from <= '2024-07-01' AND (valid_to IS NULL OR valid_to > '2024-07-01')")
        .orderBy("customer_id").show(truncate=False))

    input("\nPress Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
