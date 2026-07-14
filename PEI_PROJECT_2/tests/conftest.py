import pytest

from pathlib import Path
from pyspark.sql import SparkSession
from datetime import date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
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



# ==========================================================
# Gold Pipeline Configurations
# ==========================================================

@pytest.fixture
def sample_configs(spark):

    schema = StructType([

        StructField(
            "source_name",
            StringType(),
            False
        ),

        StructField(
            "enrich_table",
            StringType(),
            True
        ),

        StructField(
            "dq_pass_table",
            StringType(),
            True
        )

    ])

    data = [

        (

            "customer",

            "silver.customer_enriched",

            None

        ),

        (

            "products",

            "silver.products_enriched",

            None

        ),

        (

            "orders",

            None,

            "silver.orders_valid"

        )

    ]

    return spark.createDataFrame(

        data,

        schema

    )


# ==========================================================
# Customer Enriched
# ==========================================================

@pytest.fixture
def customer_enriched_df(spark):

    schema = StructType([

        StructField("customer_id", StringType(), False),

        StructField("customer_name", StringType(), True),

        StructField("segment", StringType(), True),

        StructField("country", StringType(), True),

        StructField("city", StringType(), True),

        StructField("state", StringType(), True),

        StructField("postal_code", StringType(), True),

        StructField("region", StringType(), True),

        StructField("customer_full_address", StringType(), True),

        StructField("customer_region", StringType(), True)

    ])

    data = [

        (

            "C001",

            "John",

            "Consumer",

            "India",

            "Bangalore",

            "Karnataka",

            "560001",

            "South",

            "Street 1, Bangalore, Karnataka, India",

            "SOUTH"

        )

    ]

    return spark.createDataFrame(

        data,

        schema

    )


# ==========================================================
# Product Enriched
# ==========================================================

@pytest.fixture
def product_enriched_df(spark):

    schema = StructType([

        StructField("product_id", StringType(), False),

        StructField("product_name", StringType(), True),

        StructField("category", StringType(), True),

        StructField("sub_category", StringType(), True),

        StructField("category_name", StringType(), True),

        StructField("sub_category_name", StringType(), True),

        StructField("price_per_product", DoubleType(), True)

    ])

    data = [

        (

            "P001",

            "Laptop",

            "Technology",

            "Computers",

            "Technology",

            "Computers",

            50000.0

        )

    ]

    return spark.createDataFrame(

        data,

        schema

    )


# ==========================================================
# Orders Valid
# ==========================================================

@pytest.fixture
def orders_valid_df(spark):

    schema = StructType([

        StructField("row_id", IntegerType(), False),

        StructField("order_id", StringType(), False),

        StructField("order_date", DateType(), True),

        StructField("ship_date", DateType(), True),

        StructField("ship_mode", StringType(), True),

        StructField("customer_id", StringType(), True),

        StructField("product_id", StringType(), True),

        StructField("quantity", IntegerType(), True),

        StructField("price", DoubleType(), True),

        StructField("discount", DoubleType(), True),

        StructField("profit", DoubleType(), True),

        StructField("pipeline_run_id", StringType(), True),

        StructField("source_system", StringType(), True),

        StructField("source_file_name", StringType(), True),

        StructField("ingestion_timestamp", TimestampType(), True),

        StructField("ingestion_date", DateType(), True)

    ])

    data = [

        (

            1,

            "O001",

            date(2024, 1, 1),

            date(2024, 1, 3),

            "First Class",

            "C001",

            "P001",

            2,

            50000.0,

            0.10,

            1200.567,

            "RUN001",

            "Orders",

            "orders.csv",

            None,

            date(2024, 1, 1)

        )

    ]

    return spark.createDataFrame(

        data,

        schema

    )