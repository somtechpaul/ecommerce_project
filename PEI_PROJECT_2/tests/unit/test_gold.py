"""
==========================================================
Unit Tests
Gold Transformations
==========================================================

Tests Gold layer business transformations.
"""

import pytest

from pei_pipeline.transformations.gold import (
    build_gold_tables
)


# ==========================================================
# Build Gold Tables
# ==========================================================

def test_build_gold_tables(

    spark,

    sample_configs,

    customer_enriched_df,

    product_enriched_df,

    orders_valid_df,

    monkeypatch

):

    # ------------------------------------------------------
    # Mock spark.table()
    # ------------------------------------------------------

    table_lookup = {

        "ecommerce_dev.silver.customer_enriched":
            customer_enriched_df,

        "ecommerce_dev.silver.products_enriched":
            product_enriched_df,

        "ecommerce_dev.silver.orders_valid":
            orders_valid_df

    }

    def mock_table(table_name):

        return table_lookup[table_name]

    monkeypatch.setattr(

        spark,

        "table",

        mock_table

    )

    # ------------------------------------------------------
    # Execute
    # ------------------------------------------------------

    gold_tables = build_gold_tables(

        spark=spark,

        configs=sample_configs

    )

    # ------------------------------------------------------
    # Validate Gold Tables Created
    # ------------------------------------------------------

    assert len(gold_tables) == 5

    assert "ecommerce_dev.gold.sales_master" in gold_tables

    assert "ecommerce_dev.gold.customer_summary" in gold_tables

    assert "ecommerce_dev.gold.product_summary" in gold_tables

    assert "ecommerce_dev.gold.daily_sales" in gold_tables

    assert "ecommerce_dev.gold.profit_summary" in gold_tables


# ==========================================================
# Sales Master Join
# ==========================================================

def test_sales_master(

    spark,

    sample_configs,

    customer_enriched_df,

    product_enriched_df,

    orders_valid_df,

    monkeypatch

):

    table_lookup = {

        "ecommerce_dev.silver.customer_enriched":
            customer_enriched_df,

        "ecommerce_dev.silver.products_enriched":
            product_enriched_df,

        "ecommerce_dev.silver.orders_valid":
            orders_valid_df

    }

    monkeypatch.setattr(

        spark,

        "table",

        lambda table: table_lookup[table]

    )

    gold = build_gold_tables(

        spark,

        sample_configs

    )

    sales_master = gold[

        "ecommerce_dev.gold.sales_master"

    ]

    assert sales_master.count() > 0
    assert "customer_name" in sales_master.columns
    assert "product_name" in sales_master.columns
    assert "price" in sales_master.columns
    assert "profit" in sales_master.columns
    assert "product_category" in sales_master.columns
    assert "product_sub_category" in sales_master.columns
    assert "customer_name" in sales_master.columns
    assert "country" in sales_master.columns


# ==========================================================
# Customer Summary
# ==========================================================

def test_customer_summary(

    spark,

    sample_configs,

    customer_enriched_df,

    product_enriched_df,

    orders_valid_df,

    monkeypatch

):

    table_lookup = {

        "ecommerce_dev.silver.customer_enriched":
            customer_enriched_df,

        "ecommerce_dev.silver.products_enriched":
            product_enriched_df,

        "ecommerce_dev.silver.orders_valid":
            orders_valid_df

    }

    monkeypatch.setattr(

        spark,

        "table",

        lambda table: table_lookup[table]

    )

    gold = build_gold_tables(

        spark,

        sample_configs

    )

    df = gold[

        "ecommerce_dev.gold.customer_summary"

    ]

    assert df.count() > 0

    assert "customer_id" in df.columns

    assert "total_sales" in df.columns

    assert "total_profit" in df.columns


# ==========================================================
# Product Summary
# ==========================================================

def test_product_summary(

    spark,

    sample_configs,

    customer_enriched_df,

    product_enriched_df,

    orders_valid_df,

    monkeypatch

):

    table_lookup = {

        "ecommerce_dev.silver.customer_enriched":
            customer_enriched_df,

        "ecommerce_dev.silver.products_enriched":
            product_enriched_df,

        "ecommerce_dev.silver.orders_valid":
            orders_valid_df

    }

    monkeypatch.setattr(

        spark,

        "table",

        lambda table: table_lookup[table]

    )

    gold = build_gold_tables(

        spark,

        sample_configs

    )

    df = gold[

        "ecommerce_dev.gold.product_summary"

    ]

    assert df.count() > 0

    assert "product_id" in df.columns

    assert "total_quantity" in df.columns

    assert "total_sales" in df.columns


# ==========================================================
# Daily Sales
# ==========================================================

def test_daily_sales(

    spark,

    sample_configs,

    customer_enriched_df,

    product_enriched_df,

    orders_valid_df,

    monkeypatch

):

    table_lookup = {

        "ecommerce_dev.silver.customer_enriched":
            customer_enriched_df,

        "ecommerce_dev.silver.products_enriched":
            product_enriched_df,

        "ecommerce_dev.silver.orders_valid":
            orders_valid_df

    }

    monkeypatch.setattr(

        spark,

        "table",

        lambda table: table_lookup[table]

    )

    gold = build_gold_tables(

        spark,

        sample_configs

    )

    df = gold[

        "ecommerce_dev.gold.daily_sales"

    ]

    assert df.count() > 0

    assert "order_date" in df.columns

    assert "total_sales" in df.columns

    assert "total_profit" in df.columns



# ==========================================================
# Profit Summary
# ==========================================================

def test_profit_summary(
    spark,
    sample_configs,
    customer_enriched_df,
    product_enriched_df,
    orders_valid_df,
    monkeypatch
):

    table_lookup = {
        "ecommerce_dev.silver.customer_enriched":
            customer_enriched_df,

        "ecommerce_dev.silver.products_enriched":
            product_enriched_df,

        "ecommerce_dev.silver.orders_valid":
            orders_valid_df
    }

    monkeypatch.setattr(
        spark,
        "table",
        lambda table: table_lookup[table]
    )

    gold_tables = build_gold_tables(
        spark=spark,
        configs=sample_configs
    )

    profit_summary = gold_tables[
        "ecommerce_dev.gold.profit_summary"
    ]

    expected_columns = {
        "order_year",
        "product_category",
        "product_sub_category",
        "customer_id",
        "customer_name",
        "total_profit"
    }

    assert expected_columns.issubset(
        set(profit_summary.columns)
    )

    assert profit_summary.count() > 0


