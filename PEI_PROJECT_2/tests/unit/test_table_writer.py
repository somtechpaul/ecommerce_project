"""
==========================================================
Unit Tests
Table Writer
==========================================================

Tests io/table_writer.py
"""

import pytest

from pei_pipeline.config.settings import CATALOG
from pei_pipeline.io.table_writer import (
    write_table
)


# ==========================================================
# Mock DataFrame Writer
# ==========================================================

class MockWriter:

    def __init__(self):

        self.format_name = None
        self.mode_name = None
        self.options = {}
        self.partition_columns = None
        self.saved_table = None

    def format(self, value):

        self.format_name = value

        return self

    def mode(self, value):

        self.mode_name = value

        return self

    def option(self, key, value):

        self.options[key] = value

        return self

    def partitionBy(self, *cols):

        self.partition_columns = cols

        return self

    def saveAsTable(self, table):

        self.saved_table = table


# ==========================================================
# Mock DataFrame
# ==========================================================

class MockDataFrame:

    def __init__(self):

        self.columns = [

            "id",

            "name",

            "ingestion_date"

        ]

        self.write = MockWriter()

        self.sparkSession = "spark"

    def alias(self, value):

        return self
    


# ==========================================================
# Append
# ==========================================================

def test_append_write():

    df = MockDataFrame()

    result = write_table(

        df=df,

        table_name="bronze.customer",

        mode="append"

    )

    assert result["status"] == "SUCCESS"

    assert df.write.mode_name == "append"

    assert df.write.saved_table == f"{CATALOG}.bronze.customer"




# ==========================================================
# Overwrite
# ==========================================================

def test_overwrite_write():

    df = MockDataFrame()

    result = write_table(

        df=df,

        table_name="silver.customer",

        mode="overwrite"

    )

    assert result["mode"] == "overwrite"

    assert df.write.mode_name == "overwrite"



# ==========================================================
# Merge Schema
# ==========================================================

def test_merge_schema():

    df = MockDataFrame()

    write_table(

        df=df,

        table_name="bronze.customer",

        merge_schema=True

    )

    assert df.write.options["mergeSchema"] == "true"



# ==========================================================
# Partition By
# ==========================================================

def test_partition_by():

    df = MockDataFrame()

    write_table(

        df=df,

        table_name="bronze.customer",

        partition_by=[

            "ingestion_date"

        ]

    )

    assert df.write.partition_columns == (

        "ingestion_date",

    )


# ==========================================================
# Invalid Partition Column
# ==========================================================

def test_invalid_partition_column():

    df = MockDataFrame()

    with pytest.raises(

        ValueError,

        match="Partition columns"

    ):

        write_table(

            df=df,

            table_name="bronze.customer",

            partition_by=[

                "dummy"

            ]

        )


# ==========================================================
# Invalid Mode
# ==========================================================

def test_invalid_mode():

    df = MockDataFrame()

    with pytest.raises(

        ValueError,

        match="Unsupported mode"

    ):

        write_table(

            df=df,

            table_name="bronze.customer",

            mode="delete"

        )




# ==========================================================
# Invalid Table Name
# ==========================================================

def test_invalid_table_name():

    df = MockDataFrame()

    with pytest.raises(

        ValueError,

        match="Invalid table name"

    ):

        write_table(

            df=df,

            table_name="customer"

        )

# ==========================================================
# Fully Qualified Table
# ==========================================================

def test_fully_qualified_table():

    df = MockDataFrame()

    write_table(

        df=df,

        table_name="ecommerce_dev.bronze.customer"

    )

    assert (

        df.write.saved_table

        == "ecommerce_dev.bronze.customer"

    )



# ==========================================================
# Merge Without Keys
# ==========================================================

def test_merge_without_keys():

    df = MockDataFrame()

    with pytest.raises(

        ValueError,

        match="merge_keys"

    ):

        write_table(

            df=df,

            table_name="bronze.customer",

            mode="merge"

        )



# ==========================================================
# Merge Success
# ==========================================================

def test_merge_success(monkeypatch):

    df = MockDataFrame()

    class MockMerge:

        def whenMatchedUpdateAll(self):

            return self

        def whenNotMatchedInsertAll(self):

            return self

        def execute(self):

            return None

    class MockDelta:

        def alias(self, value):

            return self

        def merge(self, df, condition):

            self.condition = condition

            return MockMerge()

    monkeypatch.setattr(

        "pei_pipeline.io.table_writer.DeltaTable.forName",

        lambda spark, table: MockDelta()

    )

    result = write_table(

        df=df,

        table_name="bronze.customer",

        mode="merge",

        merge_keys=["id"]

    )

    assert result["status"] == "SUCCESS"

    assert result["mode"] == "merge"



