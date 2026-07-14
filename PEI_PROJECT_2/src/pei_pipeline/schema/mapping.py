"""
==========================================================
Schema Mapping
==========================================================

Functions for standardizing and mapping DataFrame columns.

Responsibilities
----------------
- Standardize column names
- Rename columns using metadata
- Reorder columns

No datatype casting.
No validation.
"""

import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Convert column names to a standard format.

    Example:
        Customer ID  -> customer_id
        Order-Date   -> order_date
        Product(Name) -> product_name
    """

    renamed_columns = []

    for column in df.columns:
        new_name = column.strip().lower()
        new_name = re.sub(r"[ /-]+", "_", new_name)
        new_name = re.sub(r"[()]", "", new_name)
        new_name = re.sub(r"[^a-zA-Z0-9_]", "", new_name)
        new_name = re.sub(r"_+", "_", new_name)
        renamed_columns.append(new_name)
    return df.toDF(*renamed_columns)


def apply_column_mapping(
    df: DataFrame,
    mapping_df: DataFrame
) -> DataFrame:
    """
    Rename columns using metadata.

    mapping_df should contain:

        source_column
        target_column
    """

    mappings = {
        row["source_column"]: row["target_column"]
        for row in mapping_df.collect()
    }

    for source_column, target_column in mappings.items():
        if source_column in df.columns:
            df = df.withColumnRenamed(
                source_column,
                target_column
            )

    return df


def reorder_columns(
    df: DataFrame,
    expected_columns: list[str]
) -> DataFrame:
    """
    Reorder DataFrame columns.

    Extra columns remain at the end.
    """

    existing = [
        c
        for c in expected_columns
        if c in df.columns
    ]

    remaining = [
        c
        for c in df.columns
        if c not in existing
    ]

    return df.select(
        *existing,
        *remaining
    )