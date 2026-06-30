"""
Example 04 — Columns, expressions, withColumn vs select vs selectExpr.

Run:
    python 04_columns_and_expressions.py
"""

from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
              .appName("02-core-04-cols")
              .master("local[*]")
              .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(
        [
            (1, "alice", "US",  120.0,  None),
            (2, "BOB",   "in",  9.50,  "premium"),
            (3, " cara", "US",  None,   "trial"),
            (4, "dan",   "IN",  1500.0, "premium"),
            (5, "evan",  None,  42.0,  None),
        ],
        schema="order_id LONG, name STRING, country STRING, amount DOUBLE, tier STRING",
    )

    print("Source:")
    df.show(truncate=False)

    # ---- Five ways to reference a column ----
    df.select(
        df.amount,                       # attribute
        df["amount"],                    # bracket
        F.col("amount"),                 # F.col
        F.column("amount"),              # alias
        "amount",                        # bare string (only works in some methods)
    ).show(1)

    # ---- withColumn: add and replace ----
    cleaned = (df
        .withColumn("name", F.initcap(F.trim("name")))                              # "alice" → "Alice"
        .withColumn("country", F.upper(F.coalesce(F.col("country"), F.lit("UNK")))) # "in"/None → "IN"/"UNK"
        .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))              # null → 0
        .withColumn("tier",   F.coalesce(F.col("tier"),   F.lit("bronze")))
        .withColumn("tier_band",
            F.when(F.col("amount") >= 1000, "GOLD")
             .when(F.col("amount") >= 100,  "SILVER")
             .otherwise("BRONZE"))
        .withColumn("amount_with_tax", F.col("amount") * 1.18)
        .withColumn("is_premium", (F.col("tier") == "premium").cast("int"))
    )
    print("After cleaning + tiering:")
    cleaned.show(truncate=False)

    # ---- Same result via one select ----
    print("Same logic expressed as one select() (Catalyst-collapsed anyway):")
    (df.select(
        F.col("order_id"),
        F.initcap(F.trim("name")).alias("name"),
        F.upper(F.coalesce(F.col("country"), F.lit("UNK"))).alias("country"),
        F.coalesce(F.col("amount"), F.lit(0.0)).alias("amount"),
        F.coalesce(F.col("tier"),   F.lit("bronze")).alias("tier"),
    ).show(truncate=False))

    # ---- selectExpr: SQL strings as columns ----
    print("Same logic via selectExpr:")
    (df.selectExpr(
        "order_id",
        "initcap(trim(name)) AS name",
        "upper(coalesce(country, 'UNK')) AS country",
        "coalesce(amount, 0.0) AS amount",
        "coalesce(tier, 'bronze') AS tier",
    ).show(truncate=False))

    # ---- Logical ops — use & | ~ ----
    print("Filter: (country=US OR amount > 1000) AND tier IS NOT NULL")
    (cleaned
        .filter(((F.col("country") == "US") | (F.col("amount") > 1000)) & F.col("tier").isNotNull())
        .show(truncate=False))

    print("Press Enter to stop...")
    input()
    spark.stop()


if __name__ == "__main__":
    main()
