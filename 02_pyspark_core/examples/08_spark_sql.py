"""
Example 08 — Spark SQL: temp views, CTEs, mixing API and SQL.

Run:
    python 08_spark_sql.py
"""

from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    return (SparkSession.builder
              .appName("02-core-08-sql")
              .master("local[*]")
              .getOrCreate())


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    orders = spark.createDataFrame(
        [
            (1, "US", 2024, "alice",  100.0),
            (2, "US", 2024, "alice",  200.0),
            (3, "US", 2024, "bob",     50.0),
            (4, "US", 2025, "alice",  300.0),
            (5, "IN", 2024, "cara",    80.0),
            (6, "IN", 2025, "cara",    90.0),
            (7, "IN", 2025, "dan",     10.0),
            (8, "DE", 2024, "evan",   500.0),
        ],
        schema="order_id LONG, country STRING, year INT, customer STRING, amount DOUBLE",
    )

    countries = spark.createDataFrame(
        [("US", "United States"), ("IN", "India"), ("DE", "Germany")],
        schema="country STRING, country_full STRING",
    )

    # ---- Register temp views ----
    orders.createOrReplaceTempView("orders")
    countries.createOrReplaceTempView("countries")

    # ---- Pure SQL with a CTE ----
    print("\n[1] CTE + JOIN + aggregation in SQL:")
    spark.sql("""
        WITH revenue_by_country_year AS (
            SELECT country, year, SUM(amount) AS revenue, COUNT(DISTINCT customer) AS buyers
            FROM orders
            GROUP BY country, year
        )
        SELECT c.country_full, r.year, r.revenue, r.buyers
        FROM revenue_by_country_year r
        JOIN countries c USING (country)
        ORDER BY r.year, r.revenue DESC
    """).show()

    # ---- Mixed: DataFrame → temp view → SQL → DataFrame ----
    print("\n[2] DataFrame → temp view → SQL → DataFrame:")
    big = orders.filter(F.col("amount") > 50)
    big.createOrReplaceTempView("orders_big")
    top_per_country = spark.sql("""
        SELECT country, year, customer, amount,
               ROW_NUMBER() OVER (PARTITION BY country ORDER BY amount DESC) AS rn
        FROM orders_big
    """).filter(F.col("rn") == 1)
    top_per_country.show()

    # ---- Catalog inspection ----
    print("\n[3] Catalog:")
    print("Current DB:", spark.catalog.currentDatabase())
    print("Temp views:", [v.name for v in spark.catalog.listTables() if v.isTemporary])
    print("Columns in orders:", [c.name for c in spark.catalog.listColumns("orders")])

    # ---- GROUPING SETS (custom subtotals) ----
    print("\n[4] GROUPING SETS:")
    spark.sql("""
        SELECT country, year, SUM(amount) AS revenue, GROUPING_ID() AS gid
        FROM orders
        GROUP BY GROUPING SETS ((country), (year), (country, year), ())
        ORDER BY gid, country, year
    """).show()

    print("Press Enter to stop...")
    input()
    spark.stop()


if __name__ == "__main__":
    main()
