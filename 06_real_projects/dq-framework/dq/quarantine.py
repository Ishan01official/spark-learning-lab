"""
Quarantine helpers — split a DataFrame into good / bad with a reason column.
"""

from functools import reduce
from typing import List

from pyspark.sql import DataFrame, functions as F

from .rules import DQRule


def split_good_bad(df: DataFrame, rules: List[DQRule]) -> tuple[DataFrame, DataFrame]:
    """
    Split `df` into (good, bad) DataFrames.
      good: all rows that pass every `error`-severity rule.
      bad:  rows that violate at least one `error` rule, with a `_dq_reasons`
            array column listing the violated rule names.
    Warnings don't move rows to `bad`; they're informational only.
    """
    error_rules = [r for r in rules if r.severity == "error"]
    if not error_rules:
        return df, df.limit(0)

    # Add a column per rule with the rule name if it FAILED
    flagged = df
    fail_cols = []
    for r in error_rules:
        col = f"_dq_fail__{r.name}"
        flagged = flagged.withColumn(
            col,
            F.when(~r.predicate(df), F.lit(r.name)).otherwise(F.lit(None))
        )
        fail_cols.append(col)

    flagged = flagged.withColumn(
        "_dq_reasons",
        F.array_compact(F.array(*[F.col(c) for c in fail_cols]))
    )
    flagged = flagged.drop(*fail_cols)

    good = flagged.filter(F.size("_dq_reasons") == 0).drop("_dq_reasons")
    bad  = flagged.filter(F.size("_dq_reasons") > 0)

    return good, bad
