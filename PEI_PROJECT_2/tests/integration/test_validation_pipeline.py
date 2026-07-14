"""
==========================================================
Integration Tests
Schema Validation Pipeline
==========================================================

Tests complete Schema Validation pipeline.
"""

from datetime import datetime

from conftest import SCHEMA_METADATA

from pei_pipeline.pipelines.schema_validation import (
    run_schema_validation_pipeline
)


def test_schema_validation_pipeline(

    spark,

    monkeypatch

):

    # =====================================================
    # Mock Ingestion Configuration
    # =====================================================

    config_df = spark.createDataFrame(

        [

            (

                "customer",

                "bronze.customer",

                "silver.customer_valid",

                "FULL"

            )

        ],

        [

            "source_name",

            "bronze_table",

            "validated_table",

            "load_type"

        ]

    )

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.get_ingestion_config",

        lambda spark: config_df

    )

    # =====================================================
    # Mock Required Schema
    # =====================================================

    schema_df = spark.createDataFrame(

        [

            (

                "customer",

                1,

                "customer_id",

                "string",

                None,

                False

            ),

            (

                "customer",

                2,

                "customer_name",

                "string",

                None,

                False

            ),

            (

                "customer",

                3,

                "email",

                "string",

                None,

                True

            )

        ],

        schema=SCHEMA_METADATA

    )

    # =====================================================
    # Mock Bronze Table
    # =====================================================

    bronze_df = spark.createDataFrame(

        [

            (

                "C001",

                "John",

                "john@test.com",

                "customer.csv"

            ),

            (

                "C002",

                "Alice",

                "alice@test.com",

                "customer.csv"

            )

        ],

        [

            "customer_id",

            "customer_name",

            "email",

            "source_file_name"

        ]

    )

    # =====================================================
    # Mock get_table_schema()
    # =====================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.get_table_schema",

        lambda spark, table_name: schema_df

    )

    # =====================================================
    # Mock Bronze Reader
    # =====================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.read_table",

        lambda **kwargs: bronze_df

    )

    # =====================================================
    # Mock Casting
    # =====================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.cast_columns",

        lambda *args, **kwargs: {

            "casted_df": bronze_df,

            "datatype_invalid_df": None,

            "metrics": {

                "datatype_valid_records": 2,

                "datatype_rejected_records": 0

            }

        }

    )

    # =====================================================
    # Mock Required Column Validation
    # =====================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.split_valid_invalid",

        lambda *args, **kwargs: {

            "valid_df": bronze_df,

            "invalid_df": None,

            "metrics": {

                "total_records": 2,

                "valid_records": 2,

                "rejected_records": 0

            }

        }

    )

    # =====================================================
    # Mock Schema Validation
    # =====================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.validate_schema",

        lambda *args, **kwargs: {

            "valid": True,

            "missing_columns": [],

            "datatype_mismatches": []

        }

    )

    # =====================================================
    # Mock Schema Validation Logging
    # =====================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.log_schema_validation",

        lambda *args, **kwargs: None

    )

    # =====================================================
    # Mock Rejected Records Logging
    # =====================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.log_rejected_records",

        lambda *args, **kwargs: None

    )

    # =====================================================
    # Capture Validated Table Writes
    # =====================================================

    written_tables = {}

    def mock_write_table(

        df,

        table_name,

        **kwargs

    ):

        written_tables[table_name] = df

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.write_table",

        mock_write_table

    )

    # =====================================================
    # Disable Pipeline Audit
    # =====================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.schema_validation.log_pipeline_run",

        lambda **kwargs: None

    )

    # =====================================================
    # Execute Pipeline
    # =====================================================

    result = run_schema_validation_pipeline(

        spark=spark,

        run_id="TEST_RUN",

        start_time=datetime.now()

    )

    # =====================================================
    # Basic Pipeline Assertions
    # =====================================================

    assert result["stage"] == "schema_validation"

    assert result["status"] == "SUCCESS"

    assert result["processed_tables"] == 1

    assert result["failed_tables"] == 0

    assert result["rows_read"] == 2

    assert result["rows_written"] == 2

    # =====================================================
    # Validate Table Written
    # =====================================================

    assert "silver.customer_valid" in written_tables

    validated_df = written_tables[

        "silver.customer_valid"

    ]

        # =====================================================
    # Validate Data Written
    # =====================================================

    assert validated_df.count() == 2

    # =====================================================
    # Validate Columns
    # =====================================================

    assert "customer_id" in validated_df.columns

    assert "customer_name" in validated_df.columns

    assert "email" in validated_df.columns

    assert "source_file_name" in validated_df.columns

    # =====================================================
    # Validate Values
    # =====================================================

    rows = validated_df.collect()

    assert rows[0]["customer_id"] == "C001"

    assert rows[0]["customer_name"] == "John"

    assert rows[0]["email"] == "john@test.com"

    assert rows[1]["customer_id"] == "C002"

    assert rows[1]["customer_name"] == "Alice"

    assert rows[1]["email"] == "alice@test.com"

    # =====================================================
    # Validate Schema
    # =====================================================

    schema = {

        field.name: field.dataType.simpleString()

        for field in validated_df.schema.fields

    }

    assert schema["customer_id"] == "string"

    assert schema["customer_name"] == "string"

    assert schema["email"] == "string"

    # =====================================================
    # Validate Output Table Count
    # =====================================================

    assert len(written_tables) == 1

    # =====================================================
    # Validate No Duplicate Writes
    # =====================================================

    assert list(written_tables.keys()) == [

        "silver.customer_valid"

    ]










