"""
Dedup at scale — three strategies compared on a skewed input.

Run:
    python run_dedup.py
"""

import time
from pyspark.sql import SparkSession, functions as F, Window


def get_spark() -> SparkSession:
    return (SparkSession.builder
        .appName("dedup-at-scale")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 8)
        .config("spark.sql.adaptive.enabled", True)
        .config("spark.sql.adaptive.skewJoin.enabled", True)
        .getOrCreate())


N_EVENTS = 10_000_000
N_KEYS = 100_000
HOT_KEY = 42
HOT_FRACTION = 0.30  # 30% of all events
DUP_FRACTION = 0.05  # 5% exact duplicates
N_SALTS = 16


def build_input(spark):
    """Build a 10M-row event stream with skew + dups."""
    rng = (spark.range(N_EVENTS)
        .withColumn(
            "event_id",
            F.when(F.rand() < HOT_FRACTION, F.lit(HOT_KEY))
             .otherwise((F.col("id") % N_KEYS).cast("long")))
        .withColumn("event_time", F.col("id"))
        .withColumn("payload", F.concat(F.lit("v"), F.col("id").cast("string"))))

    # Inject 5% exact duplicates by unioning some rows with themselves
    dups = rng.sample(False, DUP_FRACTION, seed=42)
    return rng.unionByName(dups)


def time_strategy(label, fn, df):
    t0 = time.time()
    n = fn(df).count()
    t1 = time.time()
    print(f"  [{label}] result rows = {n:,}, time = {t1-t0:.2f}s")


def strategy_drop(df):
    return df.dropDuplicates(["event_id"])


def strategy_row_number(df):
    w = Window.partitionBy("event_id").orderBy(F.desc("event_time"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def strategy_salted(df):
    # Stage 1: salt + per-(event_id, salt) latest
    w1 = Window.partitionBy("event_id", "salt").orderBy(F.desc("event_time"))
    salted = (df
        .withColumn("salt", (F.rand() * N_SALTS).cast("int"))
        .withColumn("_rn", F.row_number().over(w1))
        .filter("_rn = 1")
        .drop("_rn", "salt"))
    # Stage 2: final dedup, much smaller
    w2 = Window.partitionBy("event_id").orderBy(F.desc("event_time"))
    return (salted
        .withColumn("_rn", F.row_number().over(w2))
        .filter("_rn = 1")
        .drop("_rn"))


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Building skewed input...")
    df = build_input(spark).cache()
    print(f"  input rows = {df.count():,}")
    print(f"  hot key c={df.filter(F.col('event_id') == HOT_KEY).count():,}")

    print("\nComparing strategies:")
    time_strategy("dropDuplicates", strategy_drop,       df)
    time_strategy("row_number",     strategy_row_number, df)
    time_strategy("salted-2-pass",  strategy_salted,     df)

    print("\nOpen the Spark UI at http://localhost:4040 to see task-skew in row_number vs salted.")
    input("Press Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
