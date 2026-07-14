"""
==========================================================
Table Reader
==========================================================

Reusable Unity Catalog table reader.

Responsibilities
----------------
- Read Delta tables
- Resolve catalog names
- Return Spark DataFrame

No business logic.
No validations.
"""

from pyspark.sql import DataFrame

from pei_pipeline.config.settings import CATALOG


def read_table(
    spark,
    table_name: str
) -> DataFrame:
    """
    Read a Unity Catalog table.

    Supports

    bronze.orders
    silver.customer
    gold.fact_sales

    OR

    ecommerce_dev.bronze.orders
    """

    # ------------------------------------------------------
    # Build Fully Qualified Table Name
    # ------------------------------------------------------

    if table_name.count(".") == 1:

        full_table_name = f"{CATALOG}.{table_name}"

    elif table_name.count(".") == 2:

        full_table_name = table_name

    else:

        raise ValueError(

            f"Invalid table name : {table_name}"

        )

    print(f"Reading Table : {full_table_name}")

    df = spark.table(

        full_table_name

    )

    print("Table read completed.")

    return df