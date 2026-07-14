"""
==========================================================
Unit Tests
Schema Validation
==========================================================

Tests schema/validation.py
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from pei_pipeline.schema.validation import (
    validate_schema
)


# ==========================================================
# Metadata Schema
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
    )

])


# ==========================================================
# Valid Schema
# ==========================================================

def test_valid_schema(spark):

    df = spark.createDataFrame(

        [

            (

                "C001",

                "John",

                100.5

            )

        ],

        [

            "customer_id",

            "customer_name",

            "sales"

        ]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "customer_id",

                "string"

            ),

            (

                "customer_name",

                "string"

            ),

            (

                "sales",

                "double"

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = validate_schema(

        df,

        schema_df

    )

    assert result["valid"] is True

    assert result["missing_columns"] == []

    assert result["extra_columns"] == []

    assert result["datatype_mismatches"] == []


# ==========================================================
# Missing Column
# ==========================================================

def test_missing_column(spark):

    df = spark.createDataFrame(

        [

            (

                "C001",

                "John"

            )

        ],

        [

            "customer_id",

            "customer_name"

        ]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "customer_id",

                "string"

            ),

            (

                "customer_name",

                "string"

            ),

            (

                "sales",

                "double"

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = validate_schema(

        df,

        schema_df

    )

    assert result["valid"] is False

    assert result["missing_columns"] == [

        "sales"

    ]



# ==========================================================
# Extra Column
# ==========================================================

def test_extra_column(spark):

    df = spark.createDataFrame(

        [

            (

                "C001",

                "John",

                100.0,

                "ABC"

            )

        ],

        [

            "customer_id",

            "customer_name",

            "sales",

            "dummy"

        ]

    )

    schema_df = spark.createDataFrame(

        [

            (

                "customer_id",

                "string"

            ),

            (

                "customer_name",

                "string"

            ),

            (

                "sales",

                "double"

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = validate_schema(

        df,

        schema_df

    )

    assert result["extra_columns"] == [

        "dummy"

    ]

    assert result["valid"] is True



# ==========================================================
# Datatype Mismatch
# ==========================================================

def test_datatype_mismatch(spark):

    schema = StructType([

        StructField(

            "customer_id",

            IntegerType(),

            True

        )

    ])

    df = spark.createDataFrame(

        [

            (

                100,

            )

        ],

        schema

    )

    schema_df = spark.createDataFrame(

        [

            (

                "customer_id",

                "string"

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = validate_schema(

        df,

        schema_df

    )

    assert result["valid"] is False

    assert len(

        result["datatype_mismatches"]

    ) == 1

    mismatch = result[

        "datatype_mismatches"

    ][0]

    assert mismatch["column_name"] == "customer_id"

    assert mismatch["expected_type"] == "string"

    assert mismatch["actual_type"] == "int"



# ==========================================================
# Multiple Validation Errors
# ==========================================================

def test_multiple_errors(spark):

    schema = StructType([

        StructField(

            "customer_id",

            IntegerType(),

            True

        )

    ])

    df = spark.createDataFrame(

        [

            (

                100,

            )

        ],

        schema

    )

    schema_df = spark.createDataFrame(

        [

            (

                "customer_id",

                "string"

            ),

            (

                "customer_name",

                "string"

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = validate_schema(

        df,

        schema_df

    )

    assert result["valid"] is False

    assert result["missing_columns"] == [

        "customer_name"

    ]

    assert len(

        result["datatype_mismatches"]

    ) == 1




# ==========================================================
# Empty Metadata
# ==========================================================

def test_empty_metadata(spark):

    df = spark.createDataFrame(

        [

            (

                "A",

            )

        ],

        [

            "id"

        ]

    )

    schema_df = spark.createDataFrame(

        [],

        schema=SCHEMA_METADATA

    )

    result = validate_schema(

        df,

        schema_df

    )

    assert result["valid"] is True

    assert result["missing_columns"] == []

    assert result["extra_columns"] == [

        "id"

    ]





# ==========================================================
# Empty DataFrame Schema
# ==========================================================

def test_empty_dataframe_schema(spark):

    schema = StructType([])

    df = spark.createDataFrame(

        [],

        schema

    )

    schema_df = spark.createDataFrame(

        [

            (

                "customer_id",

                "string"

            )

        ],

        schema=SCHEMA_METADATA

    )

    result = validate_schema(

        df,

        schema_df

    )

    assert result["valid"] is False

    assert result["missing_columns"] == [

        "customer_id"

    ]




