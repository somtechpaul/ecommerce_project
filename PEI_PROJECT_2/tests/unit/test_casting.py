"""
==========================================================
Unit Tests
Schema Casting
==========================================================

Tests metadata-driven datatype casting.
"""

from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    DateType,
    StringType,
    StructType,
    StructField
)

from pei_pipeline.schema.casting import (
    cast_columns
)


# ==========================================================
# Reusable Metadata Schema
# ==========================================================

SCHEMA_METADATA = StructType([

    StructField(
        "standard_column",
        StringType(),
        False
    ),

    StructField(
        "data_type",
        StringType(),
        False
    ),

    StructField(
        "date_format",
        StringType(),
        True
    )

])


# ==========================================================
# Integer Casting
# ==========================================================

def test_cast_integer(spark):

    input_df = spark.createDataFrame(

        [

            ("100",)

        ],

        ["quantity"]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "quantity",

                "integer",

                None

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    casted_df = result["casted_df"]

    metrics = result["metrics"]

    assert isinstance(

        casted_df.schema["quantity"].dataType,

        IntegerType

    )

    assert casted_df.count() == 1

    assert metrics["datatype_valid_records"] == 1

    assert metrics["datatype_rejected_records"] == 0


# ==========================================================
# Double Casting
# ==========================================================

def test_cast_double(spark):

    input_df = spark.createDataFrame(

        [

            ("10.55",)

        ],

        ["price"]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "price",

                "double",

                None

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    casted_df = result["casted_df"]

    metrics = result["metrics"]

    assert isinstance(

        casted_df.schema["price"].dataType,

        DoubleType

    )

    row = casted_df.collect()[0]

    assert row.price == 10.55

    assert metrics["datatype_valid_records"] == 1

    assert metrics["datatype_rejected_records"] == 0


# ==========================================================
# Date Casting
# ==========================================================

def test_cast_date(spark):

    input_df = spark.createDataFrame(

        [

            ("21/8/2016",)

        ],

        ["order_date"]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "order_date",

                "date",

                "d/M/yyyy"

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    casted_df = result["casted_df"]

    metrics = result["metrics"]

    assert isinstance(

        casted_df.schema["order_date"].dataType,

        DateType

    )

    row = casted_df.collect()[0]

    assert str(row.order_date) == "2016-08-21"

    assert metrics["datatype_valid_records"] == 1

    assert metrics["datatype_rejected_records"] == 0


# ==========================================================
# String Casting
# ==========================================================

def test_cast_string(spark):

    input_df = spark.createDataFrame(

        [

            (100,)

        ],

        ["customer_id"]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "customer_id",

                "string",

                None

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    casted_df = result["casted_df"]

    metrics = result["metrics"]

    assert isinstance(

        casted_df.schema["customer_id"].dataType,

        StringType

    )

    row = casted_df.collect()[0]

    assert row.customer_id == "100"

    assert metrics["datatype_valid_records"] == 1

    assert metrics["datatype_rejected_records"] == 0


# ==========================================================
# Ignore Missing Column
# ==========================================================

def test_ignore_missing_column(spark):

    input_df = spark.createDataFrame(

        [

            ("100",)

        ],

        ["quantity"]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "price",

                "double",

                None

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    casted_df = result["casted_df"]

    metrics = result["metrics"]

    # ------------------------------------------------------
    # Original column should still exist
    # ------------------------------------------------------

    assert "quantity" in casted_df.columns

    # ------------------------------------------------------
    # Missing metadata column should not be added
    # ------------------------------------------------------

    assert "price" not in casted_df.columns

    # ------------------------------------------------------
    # Row count should remain unchanged
    # ------------------------------------------------------

    assert casted_df.count() == 1

    # ------------------------------------------------------
    # Metrics
    # ------------------------------------------------------

    assert metrics["datatype_valid_records"] == 1

    assert metrics["datatype_rejected_records"] == 0

    # ------------------------------------------------------
    # No rejected dataframe should be produced
    # ------------------------------------------------------

    assert result["datatype_invalid_df"] is None



# ==========================================================
# Invalid Numeric Cast
# ==========================================================

def test_invalid_double_cast(spark):

    input_df = spark.createDataFrame(

        [

            ("ABC",)

        ],

        ["price"]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "price",

                "double",

                None

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    casted_df = result["casted_df"]

    rejected_df = result["datatype_invalid_df"]

    metrics = result["metrics"]

    # ------------------------------------------------------
    # No valid rows should remain
    # ------------------------------------------------------

    assert casted_df.count() == 0

    # ------------------------------------------------------
    # One rejected row expected
    # ------------------------------------------------------

    assert rejected_df is not None

    assert rejected_df.count() == 1

    row = rejected_df.collect()[0]

    assert row.failed_column == "price"

    assert row.expected_data_type == "double"

    assert row.actual_value == "ABC"

    assert row.rejection_reason == "DATATYPE_CAST_FAILED"

    # ------------------------------------------------------
    # Metrics
    # ------------------------------------------------------

    assert metrics["datatype_valid_records"] == 0

    assert metrics["datatype_rejected_records"] == 1


# ==========================================================
# Invalid Date Cast
# ==========================================================

def test_invalid_date_cast(spark):

    input_df = spark.createDataFrame(

        [

            ("XYZ",)

        ],

        ["order_date"]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "order_date",

                "date",

                "d/M/yyyy"

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    casted_df = result["casted_df"]

    rejected_df = result["datatype_invalid_df"]

    metrics = result["metrics"]

    # ------------------------------------------------------
    # No valid rows should remain
    # ------------------------------------------------------

    assert casted_df.count() == 0

    # ------------------------------------------------------
    # One rejected row expected
    # ------------------------------------------------------

    assert rejected_df is not None

    assert rejected_df.count() == 1

    row = rejected_df.collect()[0]

    assert row.failed_column == "order_date"

    assert row.expected_data_type == "date"

    assert row.actual_value == "XYZ"

    assert row.rejection_reason == "DATATYPE_CAST_FAILED"

    # ------------------------------------------------------
    # Metrics
    # ------------------------------------------------------

    assert metrics["datatype_valid_records"] == 0

    assert metrics["datatype_rejected_records"] == 1



# ==========================================================
# Multiple Column Casting
# ==========================================================

def test_cast_multiple_columns(spark):

    input_df = spark.createDataFrame(

        [

            ("100", "10.55", "21/8/2016")

        ],

        [

            "quantity",
            "price",
            "order_date"

        ]

    )

    schema_df = spark.createDataFrame(

        [

            ("quantity", "integer", None),

            ("price", "double", None),

            ("order_date", "date", "d/M/yyyy")

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    casted_df = result["casted_df"]

    row = casted_df.collect()[0]

    assert isinstance(
        casted_df.schema["quantity"].dataType,
        IntegerType
    )

    assert isinstance(
        casted_df.schema["price"].dataType,
        DoubleType
    )

    assert isinstance(
        casted_df.schema["order_date"].dataType,
        DateType
    )

    assert row.quantity == 100

    assert row.price == 10.55

    assert str(row.order_date) == "2016-08-21"

    assert result["metrics"]["datatype_valid_records"] == 1

    assert result["metrics"]["datatype_rejected_records"] == 0


# ==========================================================
# Mixed Valid and Invalid Rows
# ==========================================================

def test_mixed_valid_invalid_rows(spark):

    input_df = spark.createDataFrame(

        [

            ("10.5",),

            ("ABC",),

            ("99.99",)

        ],

        ["price"]

    )

    schema_df = spark.createDataFrame(

        [

            ("price", "double", None)

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    assert result["casted_df"].count() == 2

    assert result["datatype_invalid_df"].count() == 1

    assert result["metrics"]["datatype_valid_records"] == 2

    assert result["metrics"]["datatype_rejected_records"] == 1


# ==========================================================
# NULL Values Should Not Be Rejected
# ==========================================================

def test_null_value_not_rejected(spark):

    input_df = spark.createDataFrame(

        [

            (None,),

            ("10.5",)

        ],

        ["price"]

    )

    schema_df = spark.createDataFrame(

        [

            ("price", "double", None)

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    assert result["casted_df"].count() == 2

    assert result["datatype_invalid_df"] is None

    assert result["metrics"]["datatype_valid_records"] == 2

    assert result["metrics"]["datatype_rejected_records"] == 0


# ==========================================================
# Empty DataFrame
# ==========================================================

def test_empty_dataframe(spark):

    input_df = spark.createDataFrame(

        [],

        "price STRING"

    )

    schema_df = spark.createDataFrame(

        [

            ("price", "double", None)

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    assert result["casted_df"].count() == 0

    assert result["datatype_invalid_df"] is None

    assert result["metrics"]["datatype_valid_records"] == 0

    assert result["metrics"]["datatype_rejected_records"] == 0


# ==========================================================
# Metadata Column Missing in Source
# ==========================================================

def test_metadata_column_not_present(spark):

    input_df = spark.createDataFrame(

        [

            ("John",)

        ],

        ["customer_name"]

    )

    schema_df = spark.createDataFrame(

        [

            ("customer_id", "string", None)

        ],

        schema=SCHEMA_METADATA

    )

    result = cast_columns(

        input_df,

        schema_df

    )

    casted_df = result["casted_df"]

    assert "customer_name" in casted_df.columns

    assert "customer_id" not in casted_df.columns

    assert result["metrics"]["datatype_valid_records"] == 1

    assert result["metrics"]["datatype_rejected_records"] == 0