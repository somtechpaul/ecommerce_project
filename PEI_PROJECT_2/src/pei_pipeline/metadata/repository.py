"""
==========================================================
Metadata Repository
==========================================================

Provides reusable functions for reading metadata tables.

Responsibilities
----------------
- Read ingestion configuration
- Read expected table schema
- Read column mappings
- Read data quality rules

No business logic.
"""

from pyspark.sql import DataFrame

from pei_pipeline.config.settings import (
    INGESTION_CONFIG_TABLE,
    REQUIRED_SCHEMA_TABLE,
    DATA_QUALITY_RULE_TABLE
)


def get_ingestion_config(spark) -> DataFrame:
    """
    Return all active ingestion configurations.
    """

    return (

        spark.table(INGESTION_CONFIG_TABLE)

        .filter("active_flag = true")

    )


def get_table_schema(
    spark,
    table_name: str
) -> DataFrame:
    """
    Return expected schema for a table.
    """

    return (

        spark.table(REQUIRED_SCHEMA_TABLE)

        .filter(f"table_name = '{table_name}'")

        .orderBy("column_order")

    )


def get_column_mapping(
    spark,
    table_name: str
) -> DataFrame:
    """
    Return source-to-target column mappings.
    """

    return (

        spark.table(REQUIRED_SCHEMA_TABLE)

        .filter(f"table_name = '{table_name}'")

    )


def get_data_quality_rules(
    spark,
    table_name: str
) -> DataFrame:
    """
    Return active DQ rules for a table.
    """

    return (

        spark.table(DATA_QUALITY_RULE_TABLE)

        .filter(f"table_name = '{table_name}'")

        .filter("active_flag = true")

    )