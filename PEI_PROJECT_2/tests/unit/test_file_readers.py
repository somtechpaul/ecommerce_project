"""
==========================================================
Unit Tests
File Readers
==========================================================

Tests reusable file readers.
"""

import pytest

from pei_pipeline.io.file_readers import (

    read_csv,

    read_json,

    read_file

)


# ==========================================================
# CSV Reader
# ==========================================================

def test_read_csv(

    spark,

    sample_csv_file

):

    df = read_csv(

        spark=spark,

        file_path=sample_csv_file

    )

    assert df.count() == 3

    assert "customer_id" in df.columns

    assert "customer_name" in df.columns


# ==========================================================
# CSV Reader with Custom Options
# ==========================================================

def test_read_csv_custom_options(

    spark,

    sample_csv_file

):

    df = read_csv(

        spark=spark,

        file_path=sample_csv_file,

        options={

            "header": "true",

            "delimiter": ","

        }

    )

    assert df.count() == 3

    assert "customer_id" in df.columns


# ==========================================================
# JSON Reader
# ==========================================================

def test_read_json(

    spark,

    sample_json_file

):

    df = read_json(

        spark=spark,

        file_path=sample_json_file

    )

    assert df.count() == 3

    assert "order_id" in df.columns


# ==========================================================
# Generic Reader - CSV
# ==========================================================

def test_read_file_csv(

    spark,

    sample_csv_file

):

    df = read_file(

        spark=spark,

        file_path=sample_csv_file,

        file_type="csv"

    )

    assert df.count() == 3

    assert "customer_id" in df.columns


# ==========================================================
# Generic Reader - JSON
# ==========================================================

def test_read_file_json(

    spark,

    sample_json_file

):

    df = read_file(

        spark=spark,

        file_path=sample_json_file,

        file_type="json"

    )

    assert df.count() == 3

    assert "order_id" in df.columns




# ==========================================================
# Parquet Reader
# ==========================================================

from pei_pipeline.io.file_readers import (
    read_excel
)


# ==========================================================
# Excel Reader
# ==========================================================

def test_read_excel(

    spark,

    sample_excel_file

):

    df = read_excel(

        spark=spark,

        file_path=sample_excel_file

    )

    assert df.count() == 3

    assert "customer_id" in df.columns


# ==========================================================
# Generic Reader - Excel
# ==========================================================

def test_read_file_excel(

    spark,

    sample_excel_file

):

    df = read_file(

        spark=spark,

        file_path=sample_excel_file,

        file_type="excel"

    )

    assert df.count() == 3

    assert "customer_id" in df.columns


# ==========================================================
# Unsupported File Type
# ==========================================================

def test_invalid_file_type(

    spark,

    sample_csv_file

):

    with pytest.raises(ValueError):

        read_file(

            spark=spark,

            file_path=sample_csv_file,

            file_type="xml"

        )


# ==========================================================
# Non-existent File
# ==========================================================

def test_file_not_found(spark):

    with pytest.raises(Exception):

        read_file(

            spark=spark,

            file_path="/tmp/file_does_not_exist.csv",

            file_type="csv"

        ).count()
