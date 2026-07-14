"""
==========================================================
Integration Tests
Bronze Ingestion Pipeline
==========================================================
"""

from datetime import datetime
from types import SimpleNamespace

from pyspark.sql.functions import (
    lit,
    current_timestamp,
    current_date
)

from pei_pipeline.pipelines.ingestion import (
    run_ingestion_pipeline
)


def test_ingestion_pipeline(

    spark,

    monkeypatch

):

    # ======================================================
    # Metadata Configuration
    # ======================================================

    config_df = spark.createDataFrame(

        [

            (

                "customer",

                "/landing/customer",

                "*.csv",

                "csv",

                "bronze.customer",

                "/archive/customer",

                True

            )

        ],

        [

            "source_name",

            "landing_path",

            "file_pattern",

            "file_type",

            "bronze_table",

            "archive_path",

            "active_flag"

        ]

    )

    monkeypatch.setattr(

        "pei_pipeline.pipelines.ingestion.get_ingestion_config",

        lambda spark: config_df

    )

    # ======================================================
    # Mock Landing Files
    # ======================================================

    file = SimpleNamespace(

        name="customer.csv",

        path="/landing/customer/customer.csv"

    )

    monkeypatch.setattr(

        "pei_pipeline.pipelines.ingestion.get_matching_files",

        lambda **kwargs: [file]

    )

    # ======================================================
    # Mock File Reader
    # ======================================================

    sample_df = spark.createDataFrame(

        [

            ("C001", "John"),

            ("C002", "Alice"),

            ("C003", "Bob")

        ],

        [

            "customer_id",

            "customer_name"

        ]

    )

    monkeypatch.setattr(

        "pei_pipeline.pipelines.ingestion.read_file",

        lambda **kwargs: sample_df

    )

    # ======================================================
    # Mock Standardization
    # ======================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.ingestion.standardize_column_names",

        lambda df: df

    )

    # ======================================================
    # Mock Metadata Columns
    # ======================================================

    def mock_add_metadata_columns(

        df,

        run_id,

        source_file,

        source_system

    ):

        return (

            df

            .withColumn(

                "pipeline_run_id",

                lit(run_id)

            )

            .withColumn(

                "source_file_name",

                lit(source_file)

            )

            .withColumn(

                "source_system",

                lit(source_system)

            )

            .withColumn(

                "ingestion_timestamp",

                current_timestamp()

            )

            .withColumn(

                "ingestion_date",

                current_date()

            )

        )

    monkeypatch.setattr(

        "pei_pipeline.pipelines.ingestion.add_metadata_columns",

        mock_add_metadata_columns

    )

    # ======================================================
    # Mock Business Casting
    # ======================================================

    monkeypatch.setattr(

        "pei_pipeline.pipelines.ingestion.cast_business_columns_to_string",

        lambda df: df

    )

    # ======================================================
    # Capture Bronze Writes
    # ======================================================

    written_tables = {}

    def mock_write_table(

        df,

        table_name,

        **kwargs

    ):

        written_tables[table_name] = df

    monkeypatch.setattr(

        "pei_pipeline.pipelines.ingestion.write_table",

        mock_write_table

    )

    # ======================================================
    # Capture Archive
    # ======================================================

    archive_calls = []

    monkeypatch.setattr(

        "pei_pipeline.pipelines.ingestion.move_file",

        lambda **kwargs: archive_calls.append(kwargs)

    )

    # ======================================================
    # Capture Audit
    # ======================================================

    audit_calls = []

    monkeypatch.setattr(

        "pei_pipeline.pipelines.ingestion.log_pipeline_run",

        lambda **kwargs: audit_calls.append(kwargs)

    )

    # ======================================================
    # Dummy dbutils
    # ======================================================

    dbutils = SimpleNamespace()

    # ======================================================
    # Execute Pipeline
    # ======================================================

    result = run_ingestion_pipeline(

        spark=spark,

        dbutils=dbutils,

        run_id="TEST_RUN",

        start_time=datetime.now()

    )

    # ======================================================
    # Assertions
    # ======================================================

    assert result["status"] == "SUCCESS"

    assert result["processed_files"] == 1

    assert result["failed_files"] == 0

    assert "bronze.customer" in written_tables

    bronze_df = written_tables["bronze.customer"]

    assert bronze_df.count() == 3

    assert "pipeline_run_id" in bronze_df.columns

    assert "source_file_name" in bronze_df.columns

    assert "source_system" in bronze_df.columns

    assert "ingestion_timestamp" in bronze_df.columns

    assert "ingestion_date" in bronze_df.columns

    assert len(archive_calls) == 1

    assert len(audit_calls) == 1