"""
06 — Schema evolution

Demonstrates:
  - Enforcement: a mismatched write fails by default
  - mergeSchema=true: adds new columns
  - overwriteSchema=true: replaces schema entirely
  - SQL ALTER TABLE ADD COLUMN
  - Type widening with type-widening table feature (Delta 3.2+)

Run:
    python 06_schema_evolution.py
"""

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException


def get_spark() -> SparkSession:
    builder = (SparkSession.builder
        .appName("delta-schema-evolution")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


PATH = "/tmp/delta_demo/schema_evolution"


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ----- 1) Create initial table -----
    print("[1] Create initial users table: (user_id, name)")
    df1 = spark.createDataFrame([(1, "alice"), (2, "bob")], ["user_id", "name"])
    df1.write.format("delta").mode("overwrite").save(PATH)
    spark.read.format("delta").load(PATH).show()

    # ----- 2) Schema enforcement — mismatched write FAILS -----
    print("\n[2] Trying to write a DataFrame with an extra 'email' column — expect failure")
    df2 = spark.createDataFrame(
        [(3, "carol", "carol@x.com")],
        ["user_id", "name", "email"]
    )
    try:
        df2.write.format("delta").mode("append").save(PATH)
        print("  unexpected success!")
    except AnalysisException as e:
        msg = str(e).splitlines()[0]
        print(f"  rejected (good): {msg}")

    # ----- 3) mergeSchema=true: now the same write succeeds -----
    print("\n[3] Same write with mergeSchema=true — column added, old rows get NULL")
    (df2.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(PATH))

    spark.read.format("delta").load(PATH).orderBy("user_id").show()

    # ----- 4) ALTER TABLE ADD COLUMN (explicit DDL) -----
    print("\n[4] ALTER TABLE ADD COLUMN signup_source STRING")
    spark.sql(f"ALTER TABLE delta.`{PATH}` ADD COLUMN signup_source STRING")
    spark.read.format("delta").load(PATH).printSchema()

    # ----- 5) overwriteSchema: replace schema entirely -----
    print("\n[5] overwriteSchema=true: replacing schema (drops the old columns)")
    df_new = spark.createDataFrame(
        [(100, "DAVE", "dave@y.com", "2024-09-01")],
        ["user_id", "name", "email", "created_at"]
    )
    (df_new.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(PATH))

    spark.read.format("delta").load(PATH).printSchema()
    spark.read.format("delta").load(PATH).show()

    # ----- 6) History of schema changes -----
    print("\n[6] History — schema changes show up as commits")
    DeltaTable.forPath(spark, PATH).history().select(
        "version", "operation", "operationParameters"
    ).show(truncate=False)

    input("\nPress Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
