"""
Reusable SCD Type 2 merge helper.
"""

from typing import List
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F


def attr_hash(df: DataFrame, attr_cols: List[str], col_name: str = "attr_hash") -> DataFrame:
    """Add a deterministic hash over the SCD2 attributes."""
    return df.withColumn(
        col_name,
        F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in attr_cols]), 256)
    )


def apply_scd2_merge(
    spark,
    target: DeltaTable,
    source: DataFrame,
    *,
    natural_key: str,
    attr_cols: List[str],
    event_time_col: str = "source_updated_at",
) -> None:
    """
    Apply an SCD2 merge from `source` into `target`.

    `source` must have:
      - natural_key column
      - all `attr_cols`
      - event_time_col

    `target` must already exist with the SCD2 columns:
      natural_key, *attr_cols, event_time_col, valid_from, valid_to, is_current, attr_hash
    """
    source = attr_hash(source, attr_cols)
    target_current = (target.toDF()
                            .alias("t")
                            .where("is_current = true"))

    # Identify rows in source whose attr_hash differs from current target
    s = source.alias("s")
    joined = s.join(
        target_current,
        F.col(f"s.{natural_key}") == F.col(f"t.{natural_key}"),
        "left"
    )
    changes = joined.where(
        "t.{nk} IS NULL OR s.attr_hash != t.attr_hash".format(nk=natural_key)
    ).select(s["*"])

    # Build the staged DataFrame: two rows per existing-customer change.
    target_ids = (target_current.select(natural_key)
                  .rdd.flatMap(lambda r: r).collect())
    changes_cached = changes.persist()

    close_rows = (changes_cached
        .where(F.col(natural_key).isin(target_ids))
        .withColumn("merge_key", F.col(natural_key)))

    new_rows = (changes_cached
        .withColumn("merge_key", F.lit(None).cast(changes_cached.schema[natural_key].dataType)))

    staged = close_rows.unionByName(new_rows)

    # Build the update/insert clauses
    insert_values = {col: f"s.{col}" for col in [natural_key] + attr_cols + [event_time_col, "attr_hash"]}
    insert_values["valid_from"] = f"s.{event_time_col}"
    insert_values["valid_to"]   = "null"
    insert_values["is_current"] = "true"
    insert_values["inserted_at"] = "current_timestamp()"

    (target.alias("t")
        .merge(
            staged.alias("s"),
            f"t.{natural_key} = s.merge_key AND t.is_current = true"
        )
        .whenMatchedUpdate(set={
            "valid_to":   f"s.{event_time_col}",
            "is_current": "false",
        })
        .whenNotMatchedInsert(values=insert_values)
        .execute())

    changes_cached.unpersist()
