"""
Example 09 — UDFs: row-at-a-time vs Pandas UDF vs built-in.

Run:
    python 09_pandas_udf.py

Demonstrates:
- A regular @udf (slow row-at-a-time).
- A @pandas_udf (Arrow-vectorized, fast).
- The same logic with built-in functions (almost always the fastest).
- mapInPandas for arbitrary per-batch processing.
"""

import time
import re
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType


def get_spark() -> SparkSession:
    return (SparkSession.builder
              .appName("02-core-09-udfs")
              .master("local[*]")
              .config("spark.sql.execution.arrow.pyspark.enabled", "true")
              .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Build a sample column of "messy" phone numbers
    n = 100_000
    df = (spark.range(n)
            .withColumn("phone", F.format_string(
                "(%03d) %03d-%04d!@",
                (F.col("id") % 1000),
                (F.col("id") % 1000),
                (F.col("id") % 10000))))
    df.cache().count()      # materialize

    # ---- 1) Row-at-a-time UDF ----
    @udf(returnType=StringType())
    def normalize_row(p):
        if p is None:
            return None
        return re.sub(r"\D", "", p)

    t0 = time.time()
    df.withColumn("digits", normalize_row("phone")).count()
    print(f"[1] Row-at-a-time UDF: {time.time()-t0:.2f}s")

    # ---- 2) Pandas UDF (vectorized) ----
    @pandas_udf(StringType())
    def normalize_pandas(p: pd.Series) -> pd.Series:
        return p.str.replace(r"\D", "", regex=True)

    t0 = time.time()
    df.withColumn("digits", normalize_pandas("phone")).count()
    print(f"[2] Pandas UDF:          {time.time()-t0:.2f}s")

    # ---- 3) Built-in regexp_replace ----
    t0 = time.time()
    df.withColumn("digits", F.regexp_replace("phone", r"\D", "")).count()
    print(f"[3] Built-in regexp:     {time.time()-t0:.2f}s")

    # ---- 4) mapInPandas — full-batch arbitrary transform ----
    def add_len(iterator):
        for pdf in iterator:
            pdf["digits"] = pdf["phone"].str.replace(r"\D", "", regex=True)
            pdf["digit_count"] = pdf["digits"].str.len()
            yield pdf

    out_schema = "id LONG, phone STRING, digits STRING, digit_count LONG"
    t0 = time.time()
    df.mapInPandas(add_len, schema=out_schema).count()
    print(f"[4] mapInPandas:         {time.time()-t0:.2f}s")

    print("\nTakeaway: built-ins win, Pandas UDF closes most of the gap,")
    print("row-at-a-time UDF is the slowest by a wide margin.\n")

    print("Press Enter to stop...")
    input()
    spark.stop()


if __name__ == "__main__":
    main()
