from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "tmp" / "case_studies" / "small_files"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("case-study-small-files")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )


def make_orders(spark: SparkSession, rows: int = 50_000) -> DataFrame:
    return (
        spark.range(rows)
        .withColumn("order_date", F.date_add(F.lit("2026-01-01").cast("date"), (F.col("id") % 30).cast("int")))
        .withColumn("region", F.concat(F.lit("region_"), (F.col("id") % 8).cast("string")))
        .withColumn("amount", (F.rand(seed=9) * 200).cast("double"))
    )


def count_data_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.rglob("*.parquet") if item.is_file())


def reset_output() -> None:
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    reset_output()

    orders = make_orders(spark)

    many_files_path = OUTPUT_DIR / "many_files"
    compacted_path = OUTPUT_DIR / "compacted"

    (
        orders.repartition(48)
        .write.mode("overwrite")
        .partitionBy("order_date")
        .parquet(str(many_files_path))
    )

    (
        orders.repartition(8, "order_date")
        .write.mode("overwrite")
        .partitionBy("order_date")
        .parquet(str(compacted_path))
    )

    many_files = count_data_files(many_files_path)
    compacted_files = count_data_files(compacted_path)

    print("\nSmall-files case-study result:")
    print(f"many_files_path={many_files_path}")
    print(f"many_files_count={many_files}")
    print(f"compacted_path={compacted_path}")
    print(f"compacted_files_count={compacted_files}")
    print("\nDiscussion:")
    print("- More files means more listing, scheduling, and open-file overhead.")
    print("- Fewer, larger files are usually better for scan-heavy analytical tables.")
    print("- The correct target file count depends on data volume, table format, and workload.")

    spark.stop()


if __name__ == "__main__":
    main()
