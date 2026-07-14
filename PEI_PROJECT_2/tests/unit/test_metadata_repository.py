"""
==========================================================
Unit Tests
Metadata Repository
==========================================================

Tests metadata repository functions.
"""

from pei_pipeline.metadata.repository import (

    get_ingestion_config,

    get_table_schema,

    get_column_mapping,

    get_data_quality_rules

)


# ==========================================================
# Ingestion Config
# ==========================================================

def test_get_ingestion_config(spark):

    result = get_ingestion_config(

        spark

    )

    assert result.count() > 0

    assert "source_name" in result.columns

    assert "bronze_table" in result.columns

    assert "active_flag" in result.columns


# ==========================================================
# Customer Schema
# ==========================================================

def test_get_customer_schema(spark):

    result = get_table_schema(

        spark,

        "customer"

    )

    assert result.count() > 0

    assert "standard_column" in result.columns

    assert "data_type" in result.columns

    assert result.filter(

        "standard_column='customer_id'"

    ).count() == 1


# ==========================================================
# Products Schema
# ==========================================================

def test_get_products_schema(spark):

    result = get_table_schema(

        spark,

        "products"

    )

    assert result.count() > 0

    assert result.filter(

        "standard_column='product_id'"

    ).count() == 1


# ==========================================================
# Orders Schema
# ==========================================================

def test_get_orders_schema(spark):

    result = get_table_schema(

        spark,

        "orders"

    )

    assert result.count() > 0

    assert result.filter(

        "standard_column='order_id'"

    ).count() == 1


# ==========================================================
# Column Mapping
# ==========================================================

def test_get_column_mapping(spark):

    result = get_column_mapping(

        spark,

        "customer"

    )

    assert result.count() > 0

    assert "source_column" in result.columns

    assert "standard_column" in result.columns


# ==========================================================
# Customer DQ Rules
# ==========================================================

def test_customer_dq_rules(spark):

    result = get_data_quality_rules(

        spark,

        "customer"

    )

    assert result.count() > 0

    assert "rule_type" in result.columns

    assert result.filter(

        "rule_type='NOT_NULL'"

    ).count() == 1


# ==========================================================
# Products DQ Rules
# ==========================================================

def test_products_dq_rules(spark):

    result = get_data_quality_rules(

        spark,

        "products"

    )

    assert result.count() > 0

    assert result.filter(

        "rule_type='UNIQUE'"

    ).count() == 1


# ==========================================================
# Orders DQ Rules
# ==========================================================

def test_orders_dq_rules(spark):

    result = get_data_quality_rules(

        spark,

        "orders"

    )

    assert result.count() > 0

    assert result.filter(

        "rule_type='NOT_NULL'"

    ).count() == 1


# ==========================================================
# Invalid Table Schema
# ==========================================================

def test_invalid_table_schema(spark):

    result = get_table_schema(

        spark,

        "dummy"

    )

    assert result.count() == 0


# ==========================================================
# Invalid Column Mapping
# ==========================================================

def test_invalid_column_mapping(spark):

    result = get_column_mapping(

        spark,

        "dummy"

    )

    assert result.count() == 0


# ==========================================================
# Invalid DQ Rules
# ==========================================================

def test_invalid_dq_rules(spark):

    result = get_data_quality_rules(

        spark,

        "dummy"

    )

    assert result.count() == 0