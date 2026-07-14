"""
==========================================================
Data Quality Engine
==========================================================

Executes metadata-driven data quality rules.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit
)

from pei_pipeline.quality.rules import (
    check_not_null,
    check_unique,
    check_positive
)


RULES = {
    "NOT_NULL": check_not_null,
    "UNIQUE": check_unique,
    "POSITIVE": check_positive
}


def run_quality_checks(
    df,
    rules_df,
    run_id,
    table_name
):
    """
    Execute all active DQ rules.
    """

    rejected_df = None
    valid_df = df

    for rule in rules_df.collect():
        rule_type = rule["rule_type"]
        column_name = rule["column_name"]
        
        if rule_type not in RULES:
            continue

        failed = RULES[rule_type](
            valid_df,
            column_name
        )

        if rejected_df is None:
            rejected_df = failed
        else:
            rejected_df = rejected_df.unionByName(failed)

        valid_df = valid_df.subtract(failed)

    metrics = {
        "total_records": df.count(),
        "valid_records": valid_df.count(),
        "rejected_records": 0 if rejected_df is None else rejected_df.count()
    }

    return {
        "valid_df": valid_df,
        "rejected_df": rejected_df,
        "metrics": metrics
    }




from functools import reduce


def split_valid_invalid(
    df: DataFrame,
    required_schema: DataFrame
):
    """
    Split DataFrame into valid and invalid records
    based on required (NOT NULL) columns.

    Parameters
    ----------
    df
        DataFrame after datatype casting.

    required_schema
        Metadata table containing:
            - standard_column
            - data_type
            - nullable

    Returns
    -------
    {
        "valid_df": DataFrame,
        "invalid_df": DataFrame,
        "metrics": {
            "total_records": int,
            "valid_records": int,
            "rejected_records": int
        }
    }
    """

    metadata_columns = {

        "pipeline_run_id",
        "source_system",
        "source_file_name",
        "ingestion_timestamp",
        "ingestion_date"

    }

    schema_rows = required_schema.collect()

    valid_condition = None

    invalid_frames = []

    # -------------------------------------------------------
    # Build validation conditions
    # -------------------------------------------------------

    for row in schema_rows:

        column_name = row["standard_column"]
        expected_type = row["data_type"]
        nullable = row["nullable"]

        if column_name in metadata_columns:
            continue

        if nullable:
            continue

        condition = col(column_name).isNotNull()

        if valid_condition is None:

            valid_condition = condition

        else:

            valid_condition = valid_condition & condition

        # ------------------------------------------
        # Invalid records for this required column
        # ------------------------------------------

        failed = (
            df
            .filter(col(column_name).isNull())
            .withColumn(
                "failed_column",
                lit(column_name)
            )

            .withColumn(
                "expected_data_type",
                lit(expected_type)
            )

            .withColumn(
                "actual_value",
                col(column_name).cast("string")
            )

            .withColumn(
                "rejection_reason",
                lit("REQUIRED_COLUMN_NULL")
            )

        )

        invalid_frames.append(failed)

    # -------------------------------------------------------
    # Valid dataframe
    # -------------------------------------------------------

    if valid_condition is None:

        valid_df = df

    else:

        valid_df = df.filter(valid_condition)

    # -------------------------------------------------------
    # Invalid dataframe
    # -------------------------------------------------------

    if invalid_frames:

        invalid_df = reduce(

            lambda x, y: x.unionByName(y),

            invalid_frames

        )

    else:

        invalid_df = None

    # -------------------------------------------------------
    # Metrics
    # -------------------------------------------------------

    valid_count = valid_df.count()

    rejected_count = (

        invalid_df.count()

        if invalid_df is not None

        else 0

    )

    return {

        "valid_df": valid_df,

        "invalid_df": invalid_df,

        "metrics": {

            "total_records": valid_count + rejected_count,

            "valid_records": valid_count,

            "rejected_records": rejected_count

        }

    }

