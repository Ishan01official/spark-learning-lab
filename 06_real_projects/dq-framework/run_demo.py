"""
Demo the DQ framework on a synthetic orders dataset.

Run:
    python run_demo.py
"""

from pyspark.sql import SparkSession, functions as F

from dq import DQRule, DQRunner, DQException, split_good_bad


VALID_STATUSES = ["NEW", "PAID", "SHIPPED", "REFUNDED"]
KNOWN_REGIONS = ["US", "EU", "APAC", "LATAM"]
EMAIL_RE = r"^[^@]+@[^@]+\.[^@]+$"


def get_spark() -> SparkSession:
    return (SparkSession.builder
        .appName("dq-framework-demo")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame([
        (1, "alice@x.com",  "NEW",       100.00, "US"),
        (2, "bob@x.com",    "PAID",       50.00, "EU"),
        (3, "carol@x.com",  "SHIPPED",   200.00, "APAC"),
        (4, "no-email-com", "NEW",       -10.00, "US"),     # bad amount + bad email
        (5, "dave@x.com",   "UNKNOWN",    25.00, "MARS"),   # bad status + unknown region
        (6, "eve@x.com",    "NEW",        80.00, None),     # null region (warn only)
        (7, "frank@x.com",  "PAID",       60.00, "EU"),
        (8, None,           "NEW",        30.00, "US"),     # null email
    ], ["order_id", "email", "status", "amount", "region"])

    rules = [
        DQRule("amount_positive",
               lambda d: F.col("amount") > 0,
               severity="error",
               description="Amounts must be positive"),
        DQRule("status_valid",
               lambda d: F.col("status").isin(VALID_STATUSES),
               severity="error",
               description=f"Status must be one of {VALID_STATUSES}"),
        DQRule("email_format",
               lambda d: F.col("email").rlike(EMAIL_RE),
               severity="warn",
               threshold=0.10,
               description="Email format check (warn only, allow 10%)"),
        DQRule("region_known",
               lambda d: F.col("region").isin(KNOWN_REGIONS),
               severity="warn",
               threshold=0.20,
               description="Region must be a known value (warn only)"),
    ]

    # ----- Run as a report (no row routing) -----
    report = DQRunner(rules).run(df)
    print("DQ REPORT")
    print(report.as_json())

    # ----- Split good/bad based on errors -----
    good, bad = split_good_bad(df, rules)
    print("\nGOOD ROWS:")
    good.show(truncate=False)
    print("\nBAD ROWS (with reasons):")
    bad.show(truncate=False)

    # ----- Fail-fast pattern (for production) -----
    if report.has_errors():
        print(f"\n[would raise DQException in production] errors found")
        # raise DQException(report.as_json())

    spark.stop()


if __name__ == "__main__":
    main()
