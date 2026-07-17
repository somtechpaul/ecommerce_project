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
    move_file,
    generate_source_file_id
)

def test_generate_source_file_id_is_deterministic():

    first_id = generate_source_file_id(
        source_name="orders",
        file_path="/landing/orders/Orders.json",
        file_size=3045599,
        modification_time=1784123400000
    )

    second_id = generate_source_file_id(
        source_name="orders",
        file_path="/landing/orders/Orders.json",
        file_size=3045599,
        modification_time=1784123400000
    )

    assert first_id == second_id
    assert len(first_id) == 64


def test_generate_new_id_when_file_changes():

    original_id = generate_source_file_id(
        source_name="orders",
        file_path="/landing/orders/Orders.json",
        file_size=3045599,
        modification_time=1784123400000
    )

    updated_id = generate_source_file_id(
        source_name="orders",
        file_path="/landing/orders/Orders.json",
        file_size=3046000,
        modification_time=1784209800000
    )

    assert original_id != updated_id


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