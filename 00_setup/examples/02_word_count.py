"""Word count — distributed computing's 'hello world'.

This script demonstrates:
  - SparkSession creation
  - createDataFrame from in-memory data
  - split + explode to tokenize
  - groupBy + count (forces a shuffle)
  - orderBy (forces another shuffle / global sort)
  - show() as the action that triggers execution

Run:
    python 00_setup/examples/02_word_count.py
Open http://localhost:4040 BEFORE pressing Enter to see the DAG.

See 00_setup/05-first-pyspark-program.md for the line-by-line writeup.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("word_count")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = get_spark()

    # Tiny in-memory corpus so the script runs anywhere.
    text = [
        "spark is fast",
        "spark is distributed",
        "spark is fun",
        "fast distributed engines are fun",
    ]
    df = spark.createDataFrame([(line,) for line in text], schema=["line"])

    # Tokenize: split each line by whitespace, then explode the array of words
    # into one row per word.
    words = df.select(F.explode(F.split("line", r"\s+")).alias("word"))

    counts = (
        words
        .groupBy("word")        # SHUFFLE: rows for the same word converge on one executor
        .count()
        .orderBy(F.desc("count"))  # second SHUFFLE: global sort
    )

    # show() is an action -> Spark plans, optimizes, executes.
    counts.show()

    # explain() prints the physical plan; useful to confirm where shuffles happen.
    print("=" * 60)
    print("Physical plan:")
    counts.explain(mode="formatted")

    input("Spark UI is at http://localhost:4040 — press Enter to stop.")
    spark.stop()


if __name__ == "__main__":
    main()
