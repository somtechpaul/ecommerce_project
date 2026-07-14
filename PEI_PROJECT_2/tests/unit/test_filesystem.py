"""
==========================================================
Unit Tests
Filesystem Utilities
==========================================================

Tests filesystem.py
"""

from types import SimpleNamespace

from pei_pipeline.io.filesystem import (
    get_matching_files,
    move_file
)


# ==========================================================
# Helper
# ==========================================================

def create_mock_dbutils(files=None):

    if files is None:
        files = []

    fs = SimpleNamespace()

    fs.ls = lambda path: files

    fs.mv_called = None

    def mv(source, destination):

        fs.mv_called = (
            source,
            destination
        )

    fs.mv = mv

    dbutils = SimpleNamespace()

    dbutils.fs = fs

    return dbutils


# ==========================================================
# Match CSV Files
# ==========================================================

def test_get_matching_csv_files():

    files = [

        SimpleNamespace(

            name="customer.csv",

            path="/landing/customer.csv",

            modificationTime=20

        ),

        SimpleNamespace(

            name="orders.json",

            path="/landing/orders.json",

            modificationTime=10

        ),

        SimpleNamespace(

            name="products.csv",

            path="/landing/products.csv",

            modificationTime=30

        )

    ]

    dbutils = create_mock_dbutils(files)

    result = get_matching_files(

        dbutils=dbutils,

        landing_path="/landing",

        file_pattern="*.csv"

    )

    assert len(result) == 2

    assert result[0].name == "customer.csv"

    assert result[1].name == "products.csv"