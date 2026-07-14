"""
==========================================================
Unit Tests
Table Reader
==========================================================

Tests io/table_reader.py
"""

import pytest
from types import SimpleNamespace

from pei_pipeline.config.settings import CATALOG
from pei_pipeline.io.table_reader import read_table


# ==========================================================
# Helper
# ==========================================================

class MockSpark:

    def __init__(self):

        self.last_table = None

        self.return_df = SimpleNamespace(name="MockDataFrame")

    def table(self, table_name):

        self.last_table = table_name

        return self.return_df


# ==========================================================
# Read Bronze Table
# ==========================================================

def test_read_bronze_table():

    spark = MockSpark()

    df = read_table(

        spark=spark,

        table_name="bronze.orders"

    )

    assert spark.last_table == f"{CATALOG}.bronze.orders"

    assert df == spark.return_df


# ==========================================================
# Read Silver Table
# ==========================================================

def test_read_silver_table():

    spark = MockSpark()

    read_table(

        spark=spark,

        table_name="silver.customer"

    )

    assert spark.last_table == f"{CATALOG}.silver.customer"


# ==========================================================
# Read Gold Table
# ==========================================================

def test_read_gold_table():

    spark = MockSpark()

    read_table(

        spark=spark,

        table_name="gold.sales_master"

    )

    assert spark.last_table == f"{CATALOG}.gold.sales_master"


# ==========================================================
# Fully Qualified Table Name
# ==========================================================

def test_read_fully_qualified_table():

    spark = MockSpark()

    read_table(

        spark=spark,

        table_name="ecommerce_dev.bronze.orders"

    )

    assert spark.last_table == "ecommerce_dev.bronze.orders"


# ==========================================================
# Invalid Table Name
# ==========================================================

def test_invalid_table_name():

    spark = MockSpark()

    with pytest.raises(

        ValueError,

        match="Invalid table name"

    ):

        read_table(

            spark=spark,

            table_name="orders"

        )


# ==========================================================
# Too Many Parts
# ==========================================================

def test_invalid_table_name_four_parts():

    spark = MockSpark()

    with pytest.raises(

        ValueError,

        match="Invalid table name"

    ):

        read_table(

            spark=spark,

            table_name="a.b.c.d"

        )


# ==========================================================
# Spark.table() Called Once
# ==========================================================

def test_table_called_once():

    spark = MockSpark()

    read_table(

        spark=spark,

        table_name="bronze.customer"

    )

    assert spark.last_table == f"{CATALOG}.bronze.customer"


# ==========================================================
# Returned Object
# ==========================================================

def test_return_dataframe():

    spark = MockSpark()

    result = read_table(

        spark=spark,

        table_name="silver.orders"

    )

    assert result is spark.return_df