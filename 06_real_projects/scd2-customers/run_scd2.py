"""
End-to-end SCD2 demo on a small customers table.

Run:
    python run_scd2.py
"""

import os
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

from lib.scd2 import apply_scd2_merge


PATH = "/tmp/scd2_demo/customers"
ATTR_COLS = ["name", "email", "country", "tier"]


def get_spark() -> SparkSession:
    builder = (SparkSession.builder
        .appName("scd2-demo")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def init_table(spark) -> DeltaTable:
    """Create an empty SCD2 table with the right schema."""
    if os.path.exists(PATH):
        import shutil; shutil.rmtree(PATH)
    schema = """
        customer_id STRING,
        name STRING,
        email STRING,
        country STRING,
        tier STRING,
        source_updated_at TIMESTAMP,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN,
        attr_hash STRING,
        inserted_at TIMESTAMP
    """
    spark.createDataFrame([], schema=schema) \
        .write.format("delta").save(PATH)
    return DeltaTable.forPath(spark, PATH)


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    target = init_table(spark)

    # ----- Batch 1: insert 5 customers -----
    print("\n[batch 1] insert 5 customers")
    b1 = spark.createDataFrame([
        ("c-001", "alice",   "alice@x.com",   "US", "gold",     "2024-01-15 10:00:00"),
        ("c-002", "bob",     "bob@x.com",     "DE", "silver",   "2024-01-15 10:00:00"),
        ("c-003", "carol",   "carol@x.com",   "JP", "platinum", "2024-01-15 10:00:00"),
        ("c-004", "dave",    "dave@x.com",    "BR", "bronze",   "2024-01-15 10:00:00"),
        ("c-005", "eve",     "eve@x.com",     "IN", "silver",   "2024-01-15 10:00:00"),
    ], ["customer_id", "name", "email", "country", "tier", "source_updated_at"])

    apply_scd2_merge(spark, target, b1,
                     natural_key="customer_id", attr_cols=ATTR_COLS)
    target.toDF().orderBy("customer_id", "valid_from").show(20, truncate=False)

    # ----- Batch 2: bob moves DE->FR, dave moves BR->AR, frank (new) -----
    print("\n[batch 2] bob -> FR, dave -> AR, frank (new)")
    b2 = spark.createDataFrame([
        ("c-002", "bob",     "bob@x.com",     "FR", "silver",   "2024-06-15 14:00:00"),
        ("c-004", "dave",    "dave@x.com",    "AR", "bronze",   "2024-06-15 14:00:00"),
        ("c-006", "frank",   "frank@x.com",   "US", "gold",     "2024-06-15 14:00:00"),
    ], ["customer_id", "name", "email", "country", "tier", "source_updated_at"])

    apply_scd2_merge(spark, target, b2,
                     natural_key="customer_id", attr_cols=ATTR_COLS)
    target.toDF().orderBy("customer_id", "valid_from").show(20, truncate=False)

    # ----- Batch 2 again (idempotency check) — nothing changes -----
    print("\n[batch 2 replay] should produce NO new rows")
    cnt_before = target.toDF().count()
    apply_scd2_merge(spark, target, b2,
                     natural_key="customer_id", attr_cols=ATTR_COLS)
    cnt_after = target.toDF().count()
    assert cnt_before == cnt_after, f"idempotency broken: {cnt_before} -> {cnt_after}"
    print(f"  row count unchanged: {cnt_after}")

    # ----- Batch 3: alice tier upgrade to platinum -----
    print("\n[batch 3] alice gold -> platinum")
    b3 = spark.createDataFrame([
        ("c-001", "alice",   "alice@x.com",   "US", "platinum", "2024-09-15 09:00:00"),
    ], ["customer_id", "name", "email", "country", "tier", "source_updated_at"])

    apply_scd2_merge(spark, target, b3,
                     natural_key="customer_id", attr_cols=ATTR_COLS)
    target.toDF().filter("customer_id = 'c-001'") \
        .orderBy("valid_from").show(truncate=False)

    # ----- Point-in-time: what did alice look like on 2024-04-01? -----
    print("\n[point-in-time] alice on 2024-04-01:")
    (target.toDF()
        .filter("customer_id = 'c-001'")
        .filter("valid_from <= '2024-04-01' AND (valid_to IS NULL OR valid_to > '2024-04-01')")
        .show(truncate=False))

    # ----- Current view -----
    print("\n[current view]")
    target.toDF().filter("is_current = true") \
        .select("customer_id", "name", "country", "tier") \
        .orderBy("customer_id").show()

    spark.stop()


if __name__ == "__main__":
    main()
