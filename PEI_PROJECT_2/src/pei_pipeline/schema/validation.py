"""
==========================================================
Schema Validation
==========================================================

Validate DataFrame structure against expected metadata schema.

Responsibilities
----------------
- Check missing columns
- Check extra columns
- Check datatype mismatches

No data quality validation.
No logging.
No table writes.
"""

from pyspark.sql import DataFrame


def validate_schema(
    df: DataFrame,
    schema_df: DataFrame
) -> dict:
    """
    Compare DataFrame schema with expected metadata schema.

    Returns
    -------
    {
        "valid": bool,
        "missing_columns": [],
        "extra_columns": [],
        "datatype_mismatches": []
    }
    """

    expected_schema = {

        row["standard_column"]: row["data_type"].lower()

        for row in schema_df.collect()

    }

    actual_schema = {

        field.name: field.dataType.simpleString().lower()

        for field in df.schema.fields

    }

    missing_columns = [

        column

        for column in expected_schema

        if column not in actual_schema

    ]

    extra_columns = [

        column

        for column in actual_schema

        if column not in expected_schema

    ]

    datatype_mismatches = []

    for column_name in expected_schema:

        if column_name in actual_schema:

            expected = expected_schema[column_name]

            actual = actual_schema[column_name]

            if expected != actual:

                datatype_mismatches.append({

                    "column_name": column_name,

                    "expected_type": expected,

                    "actual_type": actual

                })

    return {

        "valid": (
            len(missing_columns) == 0
            and len(datatype_mismatches) == 0
        ),

        "missing_columns": missing_columns,

        "extra_columns": extra_columns,

        "datatype_mismatches": datatype_mismatches

    }