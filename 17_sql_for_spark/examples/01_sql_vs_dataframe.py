from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("sql-vs-dataframe")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    spark = get_spark()

    orders = spark.createDataFrame(
        [
            (1, "book", "online", 25.0),
            (2, "book", "store", 30.0),
            (3, "laptop", "online", 900.0),
            (4, "laptop", "store", 1100.0),
            (5, "book", "online", 10.0),
        ],
        ["order_id", "category", "channel", "amount"],
    )

    orders.createOrReplaceTempView("orders")

    sql_result = spark.sql(
        """
        SELECT
            category,
            channel,
            COUNT(*) AS order_count,
            SUM(amount) AS revenue
        FROM orders
        GROUP BY category, channel
        ORDER BY category, channel
        """
    )

    dataframe_result = (
        orders.groupBy("category", "channel")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("revenue"),
        )
        .orderBy("category", "channel")
    )

    print("SQL result")
    sql_result.show()

    print("DataFrame result")
    dataframe_result.show()

    print("Physical plan")
    sql_result.explain()

    spark.stop()


if __name__ == "__main__":
    main()
