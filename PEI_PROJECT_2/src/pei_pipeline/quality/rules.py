"""
==========================================================
Data Quality Rules
==========================================================

Reusable Data Quality rule implementations.

Each function receives a DataFrame and returns a filtered
DataFrame containing rows that violate the rule.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_not_null(
    df: DataFrame,
    column_name: str
) -> DataFrame:
    """
    Return rows where the column is NULL.
    """

    return df.filter(col(column_name).isNull())


def check_unique(
    df: DataFrame,
    column_name: str
) -> DataFrame:
    """
    Return duplicate rows.
    """

    duplicate_keys = (

        df.groupBy(column_name)
          .count()
          .filter("count > 1")
          .select(column_name)

    )

    return df.join(
        duplicate_keys,
        column_name,
        "inner"
    )


def check_positive(
    df: DataFrame,
    column_name: str
) -> DataFrame:
    """
    Return rows where value <= 0.
    """

    return df.filter(col(column_name) <= 0)