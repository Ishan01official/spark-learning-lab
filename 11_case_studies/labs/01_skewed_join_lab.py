from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("case-study-skewed-join")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def make_events(spark: SparkSession, rows: int = 300_000) -> DataFrame:
    return (
        spark.range(rows)
        .withColumn(
            "customer_id",
            F.when(F.col("id") < rows * 0.75, F.lit("UNKNOWN"))
            .otherwise(F.concat(F.lit("C"), (F.col("id") % 10_000).cast("string"))),
        )
        .withColumn("event_value", (F.rand(seed=7) * 100).cast("double"))
    )


def make_customers(spark: SparkSession) -> DataFrame:
    normal_customers = spark.range(10_000).select(
        F.concat(F.lit("C"), F.col("id").cast("string")).alias("customer_id"),
        F.concat(F.lit("segment_"), (F.col("id") % 5).cast("string")).alias("segment"),
    )
    unknown = spark.createDataFrame([("UNKNOWN", "bad_default")], ["customer_id", "segment"])
    return normal_customers.unionByName(unknown)


def show_key_distribution(events: DataFrame) -> None:
    print("\nTop customer_id values before join:")
    events.groupBy("customer_id").count().orderBy(F.desc("count")).show(10, truncate=False)


def run_naive_join(events: DataFrame, customers: DataFrame) -> int:
    joined = events.join(customers, "customer_id")
    print("\nNaive join physical plan:")
    joined.explain()
    return joined.count()


def run_selective_salting(events: DataFrame, customers: DataFrame, salts: int = 8) -> int:
    hot_keys = ["UNKNOWN"]
    salt_values = F.array([F.lit(i) for i in range(salts)])

    salted_events = events.withColumn(
        "salt",
        F.when(F.col("customer_id").isin(hot_keys), (F.rand(seed=11) * salts).cast("int"))
        .otherwise(F.lit(0)),
    )

    salted_customers = (
        customers.withColumn(
            "salt_values",
            F.when(F.col("customer_id").isin(hot_keys), salt_values).otherwise(F.array(F.lit(0))),
        )
        .withColumn("salt", F.explode("salt_values"))
        .drop("salt_values")
    )

    joined = salted_events.join(salted_customers, ["customer_id", "salt"])
    print("\nSalted join physical plan:")
    joined.explain()
    return joined.count()


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    events = make_events(spark).repartition(8, "customer_id")
    customers = make_customers(spark)

    show_key_distribution(events)

    naive_count = run_naive_join(events, customers)
    salted_count = run_selective_salting(events, customers)

    print("\nCase-study result:")
    print(f"naive_count={naive_count}")
    print(f"salted_count={salted_count}")
    print("\nOpen Spark UI and compare task duration in the join stages.")
    print("Expected finding: the naive join has a long-tail task caused by customer_id=UNKNOWN.")

    spark.stop()


if __name__ == "__main__":
    main()
