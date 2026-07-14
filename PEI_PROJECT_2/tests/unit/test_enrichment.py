"""
==========================================================
Unit Tests
Enrichment
==========================================================

Tests business enrichment logic.
"""

import pytest

from pei_pipeline.transformations.enrichment import (
    enrich_dataframe
)


# ==========================================================
# Customer Enrichment
# ==========================================================

def test_customer_enrichment(
    spark
):

    df = spark.createDataFrame(

        [

            (

                "C001",
                "John",
                "Street 1",
                "Bangalore",
                "Karnataka",
                "India",
                "south"

            )

        ],

        [

            "customer_id",
            "customer_name",
            "address",
            "city",
            "state",
            "country",
            "region"

        ]

    )

    result = enrich_dataframe(

        spark=spark,

        df=df,

        source_name="customer"

    )

    row = result.collect()[0]

    assert row.customer_full_address == \
        "Street 1, Bangalore, Karnataka, India"

    assert row.customer_region == "SOUTH"


# ==========================================================
# Product Enrichment
# ==========================================================

def test_product_enrichment(
    spark
):

    df = spark.createDataFrame(

        [

            (

                "P001",

                "office supplies",

                "paper"

            )

        ],

        [

            "product_id",

            "category",

            "sub_category"

        ]

    )

    result = enrich_dataframe(

        spark=spark,

        df=df,

        source_name="products"

    )

    row = result.collect()[0]

    assert row.category_name == "Office Supplies"

    assert row.sub_category_name == "Paper"


# ==========================================================
# Orders
# ==========================================================

def test_orders_no_enrichment(
    spark
):

    df = spark.createDataFrame(

        [

            (

                "O001",

                "C001"

            )

        ],

        [

            "order_id",

            "customer_id"

        ]

    )

    result = enrich_dataframe(

        spark=spark,

        df=df,

        source_name="orders"

    )

    assert result.columns == df.columns


# ==========================================================
# Invalid Source
# ==========================================================

def test_unknown_source(
    spark
):

    df = spark.createDataFrame(

        [

            ("1",)

        ],

        [

            "id"

        ]

    )

    result = enrich_dataframe(

        spark=spark,

        df=df,

        source_name="dummy"

    )

    assert result.columns == df.columns


# ==========================================================
# Customer Region Uppercase
# ==========================================================

def test_customer_region_uppercase(
    spark
):

    df = spark.createDataFrame(

        [

            (

                "C001",

                "John",

                "Road",

                "Delhi",

                "Delhi",

                "India",

                "north"

            )

        ],

        [

            "customer_id",

            "customer_name",

            "address",

            "city",

            "state",

            "country",

            "region"

        ]

    )

    result = enrich_dataframe(

        spark=spark,

        df=df,

        source_name="customer"

    )

    row = result.collect()[0]

    assert row.customer_region == "NORTH"


# ==========================================================
# Product Title Case
# ==========================================================

def test_product_title_case(
    spark
):

    df = spark.createDataFrame(

        [

            (

                "P001",

                "technology",

                "accessories"

            )

        ],

        [

            "product_id",

            "category",

            "sub_category"

        ]

    )

    result = enrich_dataframe(

        spark=spark,

        df=df,

        source_name="products"

    )

    row = result.collect()[0]

    assert row.category_name == "Technology"

    assert row.sub_category_name == "Accessories"