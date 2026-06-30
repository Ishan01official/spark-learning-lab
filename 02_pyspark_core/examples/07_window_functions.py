"""
Example 07 — Window functions: top-N, lag/lead, running totals, sessionization.

Run:
    python 07_window_functions.py
"""

from pyspark.sql import SparkSession, Window, functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
              .appName("02-core-07-windows")
              .master("local[*]")
              .config("spark.sql.shuffle.partitions", "8")
              .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Sales data: order_id, country, customer, amount, ts
    df = spark.createDataFrame(
        [
            (1,  "US", "alice", 100.0, "2024-01-01 09:00:00"),
            (2,  "US", "alice", 250.0, "2024-01-01 09:15:00"),     # same session
            (3,  "US", "alice",  50.0, "2024-01-02 11:00:00"),     # new session
            (4,  "US", "bob",   180.0, "2024-01-01 10:00:00"),
            (5,  "US", "bob",   300.0, "2024-01-01 14:00:00"),     # > 30 min gap → new session
            (6,  "IN", "cara",   60.0, "2024-01-01 12:00:00"),
            (7,  "IN", "cara",   90.0, "2024-01-01 12:10:00"),
            (8,  "IN", "dan",   200.0, "2024-01-01 18:00:00"),
            (9,  "DE", "evan",  500.0, "2024-01-01 08:00:00"),
            (10, "DE", "evan",  450.0, "2024-01-02 08:00:00"),
        ],
        schema="order_id LONG, country STRING, customer STRING, amount DOUBLE, ts STRING",
    ).withColumn("ts", F.to_timestamp("ts"))

    # ---- 1) Top-N per group: top 2 orders by amount per country ----
    print("\n[1] Top-2 orders per country by amount:")
    w_rank = Window.partitionBy("country").orderBy(F.col("amount").desc())
    (df.withColumn("rn", F.row_number().over(w_rank))
       .filter(F.col("rn") <= 2)
       .orderBy("country", "rn")
       .show(truncate=False))

    # ---- 2) Lag / lead — previous order amount per customer ----
    print("\n[2] Lag — each customer's previous order amount:")
    w_cust = Window.partitionBy("customer").orderBy("ts")
    (df.withColumn("prev_amount", F.lag("amount").over(w_cust))
       .withColumn("delta_vs_prev", F.col("amount") - F.col("prev_amount"))
       .orderBy("customer", "ts")
       .show(truncate=False))

    # ---- 3) Running total of revenue per customer ----
    print("\n[3] Running total per customer:")
    w_run = (Window.partitionBy("customer")
                   .orderBy("ts")
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    (df.withColumn("running_total", F.sum("amount").over(w_run))
       .orderBy("customer", "ts")
       .show(truncate=False))

    # ---- 4) Sessionization: new session if gap > 30 min ----
    print("\n[4] Sessionization (gap > 30 minutes):")
    gap_sec = F.unix_timestamp("ts") - F.unix_timestamp(F.lag("ts").over(w_cust))
    is_new = F.when(gap_sec.isNull() | (gap_sec > 30 * 60), 1).otherwise(0)

    w_acc = (Window.partitionBy("customer")
                   .orderBy("ts")
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    (df.withColumn("is_new_session", is_new)
       .withColumn("session_idx", F.sum("is_new_session").over(w_acc))
       .withColumn("session_id", F.concat_ws("_", "customer", "session_idx"))
       .select("order_id", "customer", "ts", "amount", "session_id")
       .orderBy("customer", "ts")
       .show(truncate=False))

    # ---- 5) The same window spec reused for multiple columns (no extra shuffle) ----
    print("\n[5] Reusing one window spec for multiple columns:")
    (df.select(
        "*",
        F.row_number().over(w_rank).alias("rn"),
        F.dense_rank().over(w_rank).alias("dr"),
        F.percent_rank().over(w_rank).alias("pr"),
    ).orderBy("country", "rn").show(truncate=False))

    print("Press Enter to stop...")
    input()
    spark.stop()


if __name__ == "__main__":
    main()
