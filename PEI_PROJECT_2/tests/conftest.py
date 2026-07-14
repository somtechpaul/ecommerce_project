import pytest

from pathlib import Path
from pyspark.sql import SparkSession

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)

SCHEMA_METADATA = StructType([
    StructField("table_name", StringType(), False),
    StructField("column_order", IntegerType(), False),
    StructField("standard_column", StringType(), False),
    StructField("data_type", StringType(), False),
    StructField("date_format", StringType(), True),
    StructField("nullable", BooleanType(), False)
])




# ==========================================================
# Fixtures Folder
# ==========================================================

FIXTURE_DIR = (
    Path(__file__).parent / "fixtures"
)


# ==========================================================
# Customer CSV
# ==========================================================

@pytest.fixture
def sample_csv_file():

    return str(
        FIXTURE_DIR / "customer.csv"
    )


# ==========================================================
# Product CSV
# ==========================================================

@pytest.fixture
def sample_products_csv():

    return str(
        FIXTURE_DIR / "products.csv"
    )


# ==========================================================
# Orders JSON
# ==========================================================

@pytest.fixture
def sample_json_file():

    return str(
        FIXTURE_DIR / "orders.json"
    )


# ==========================================================
# Parquet
# ==========================================================

@pytest.fixture
def sample_parquet_file():

    return str(
        FIXTURE_DIR / "sample.parquet"
    )


# ==========================================================
# Excel
# ==========================================================

@pytest.fixture
def sample_excel_file():

    return str(
        FIXTURE_DIR / "customer.xlsx"
    )



@pytest.fixture(scope="session")
def spark():

    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    return spark