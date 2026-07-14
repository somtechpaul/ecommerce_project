"""
==========================================================
Schema Casting
==========================================================

Safely cast DataFrame columns using metadata.

Responsibilities
----------------
- Cast columns according to required_schema
- Use tolerant casting
- Detect datatype conversion failures
- Return valid casted rows and datatype-invalid rows
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    expr,
    lit
)


# Temporary internal column used only during casting
DATATYPE_VALID_FLAG = "__datatype_valid"


def _quote_column(column_name: str) -> str:
    """
    Safely quote a Spark column name for SQL expressions.
    """

    escaped_name = column_name.replace("`", "``")

    return f"`{escaped_name}`"


def _build_safe_cast_expression(
    column_name: str,
    data_type: str,
    date_format: str = None
):
    """
    Create a tolerant Spark casting expression.

    Invalid values become NULL instead of stopping
    the pipeline.
    """

    quoted_column = _quote_column(column_name)

    normalized_type = data_type.strip().lower()

    # --------------------------------------------------
    # DATE
    # --------------------------------------------------

    if normalized_type == "date":

        if date_format:

            escaped_format = date_format.replace("'", "''")

            return expr(
                f"""
                CAST(
                    try_to_timestamp(
                        {quoted_column},
                        '{escaped_format}'
                    )
                    AS DATE
                )
                """
            )

        return expr(
            f"try_cast({quoted_column} AS DATE)"
        )

    # --------------------------------------------------
    # TIMESTAMP
    # --------------------------------------------------

    if normalized_type == "timestamp":

        if date_format:

            escaped_format = date_format.replace("'", "''")

            return expr(
                f"""
                try_to_timestamp(
                    {quoted_column},
                    '{escaped_format}'
                )
                """
            )

        return expr(
            f"try_cast({quoted_column} AS TIMESTAMP)"
        )

    # --------------------------------------------------
    # ALL OTHER TYPES
    # --------------------------------------------------

    return expr(
        f"try_cast({quoted_column} AS {data_type})"
    )


def cast_columns(
    df: DataFrame,
    schema_df: DataFrame
):
    """
    Safely cast columns according to required_schema.

    Parameters
    ----------
    df
        Bronze DataFrame containing source values.

    schema_df
        Metadata DataFrame containing at least:

        - standard_column
        - data_type
        - date_format

    Returns
    -------
    {
        "casted_df": DataFrame,
        "datatype_invalid_df": DataFrame or None,
        "metrics": {
            "datatype_valid_records": int,
            "datatype_rejected_records": int
        }
    }

    Datatype failure definition
    ---------------------------
    Original value is NOT NULL
    AND
    Safely casted value becomes NULL.

    Example
    -------
    Original price_per_product = "New York"
    Expected datatype           = DOUBLE
    Casted value                = NULL

    This row is rejected as DATATYPE_CAST_FAILED.
    """

    schema_rows = schema_df.collect()

    # Keep the original DataFrame unchanged.
    # Rejected rows are created from this DataFrame so
    # actual source values remain available.
    original_df = df

    # This DataFrame will be transformed column by column.
    casted_df = df.withColumn(
        DATATYPE_VALID_FLAG,
        lit(True)
    )

    invalid_frames = []

    for row in schema_rows:

        row_metadata = row.asDict(recursive=True)

        column_name = row_metadata["standard_column"]
        data_type = row_metadata["data_type"]

        date_format = row_metadata.get("date_format")

        if column_name not in df.columns:
            continue

        safe_cast_expression = _build_safe_cast_expression(
            column_name=column_name,
            data_type=data_type,
            date_format=date_format
        )

        # --------------------------------------------------
        # Datatype failure condition on original data
        # --------------------------------------------------

        datatype_failure_condition = (
            col(column_name).isNotNull()
            & safe_cast_expression.isNull()
        )

        # --------------------------------------------------
        # Capture malformed records
        # --------------------------------------------------

        failed_df = (
            original_df
            .filter(datatype_failure_condition)
            .withColumn(
                "failed_column",
                lit(column_name)
            )
            .withColumn(
                "expected_data_type",
                lit(data_type)
            )
            .withColumn(
                "actual_value",
                col(column_name).cast("string")
            )
            .withColumn(
                "rejection_reason",
                lit("DATATYPE_CAST_FAILED")
            )
        )

        invalid_frames.append(failed_df)

        # --------------------------------------------------
        # Add temporary safely casted column
        # --------------------------------------------------

        temporary_cast_column = (
            f"__casted_{column_name}"
        )

        casted_df = casted_df.withColumn(
            temporary_cast_column,
            safe_cast_expression
        )

        # --------------------------------------------------
        # Mark malformed rows as datatype invalid
        #
        # Source NULL is not considered a datatype error.
        # It will be handled later by required-field checks.
        # --------------------------------------------------

        cast_failure_on_working_df = (
            col(column_name).isNotNull()
            & col(temporary_cast_column).isNull()
        )

        casted_df = casted_df.withColumn(
            DATATYPE_VALID_FLAG,
            col(DATATYPE_VALID_FLAG)
            & (~cast_failure_on_working_df)
        )

        # Replace source column with safely casted column
        casted_df = (
            casted_df
            .drop(column_name)
            .withColumnRenamed(
                temporary_cast_column,
                column_name
            )
        )

    # --------------------------------------------------
    # Keep only rows that passed all datatype conversions
    # --------------------------------------------------

    valid_casted_df = (
        casted_df
        .filter(col(DATATYPE_VALID_FLAG))
        .drop(DATATYPE_VALID_FLAG)
    )

    # --------------------------------------------------
    # Combine all datatype-invalid records
    # --------------------------------------------------

    if invalid_frames:
        datatype_invalid_df = reduce(
            lambda left, right:
                left.unionByName(
                    right,
                    allowMissingColumns=True
                ),
            invalid_frames
        )

        if datatype_invalid_df.count() == 0:
            datatype_invalid_df = None

    else:

        datatype_invalid_df = None

    # --------------------------------------------------
    # Metrics
    # --------------------------------------------------

    datatype_valid_count = valid_casted_df.count()

    datatype_rejected_count = (
        datatype_invalid_df.count()
        if datatype_invalid_df is not None
        else 0
    )

    return {
        "casted_df": valid_casted_df,
        "datatype_invalid_df": datatype_invalid_df,
        "metrics": {
            "datatype_valid_records": datatype_valid_count,
            "datatype_rejected_records": datatype_rejected_count
        }
    }