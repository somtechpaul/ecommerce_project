"""
==========================================================
Gold Transformations
==========================================================

Reusable business transformations for Gold layer.

Responsibilities
----------------
- Build Dimensions
- Build Facts
- Build Aggregates

No file IO.
No audit logging.
No table writes.
"""

from pyspark.sql.functions import col

from pei_pipeline.config.settings import (
    CATALOG,
    GOLD_SCHEMA
)
from pyspark.sql.functions import (
    col,
    sum,
    avg,
    countDistinct,
    round as spark_round,
    year
)

def build_gold_tables(
    spark,
    configs
):
    """
    Build all Gold DataFrames.

    Parameters
    ----------
    spark
        Spark Session

    configs
        Ingestion configuration DataFrame

    Returns
    -------
    dict
        Dictionary of

        {
            target_table : dataframe
        }
    """

    print("=" * 70)
    print("BUILDING GOLD TABLES")
    print("=" * 70)

    gold_tables = {}

    # ======================================================
    # Convert Metadata to Dictionary
    # ======================================================

    config_map = {

        row["source_name"]: row.asDict()

        for row in configs.collect()

    }

    # ======================================================
    # Read Metadata Driven Tables
    # ======================================================

    customer_table = (
        f"{CATALOG}."
        f"{config_map['customer']['enrich_table']}"
    )

    product_table = (
        f"{CATALOG}."
        f"{config_map['products']['enrich_table']}"
    )

    orders_table = (
        f"{CATALOG}."
        f"{config_map['orders']['dq_pass_table']}"
    )

    print()
    print("Reading Gold Source Tables")
    print("-" * 70)

    print(f"Customer Table : {customer_table}")

    customer_df = spark.table(
        customer_table
    )

    print(
        f"Customer Rows : {customer_df.count()}"
    )

    print()

    print(f"Product Table : {product_table}")

    product_df = spark.table(
        product_table
    )

    print(
        f"Product Rows : {product_df.count()}"
    )

    print()

    print(f"Orders Table : {orders_table}")

    orders_df = spark.table(
        orders_table
    )

    print(
        f"Orders Rows : {orders_df.count()}"
    )

    print("-" * 70)

    # ======================================================
    # Part 2 starts here
    # Build Sales Master
    # ======================================================
        # ======================================================
    # Sales Master
    # ======================================================

    print()
    print("=" * 70)
    print("Building Sales Master")
    print("=" * 70)

    sales_master = (

        orders_df.alias("o")

        .join(

            customer_df.alias("c"),

            col("o.customer_id")
            ==

            col("c.customer_id"),

            "left"

        )

        .join(

            product_df.alias("p"),

            col("o.product_id")
            ==

            col("p.product_id"),

            "left"

        )

        .select(

            # ==================================================
            # Orders
            # ==================================================

            col("o.row_id"),

            col("o.order_id"),

            col("o.order_date"),

            col("o.ship_date"),

            col("o.ship_mode"),

            col("o.customer_id"),

            col("o.product_id"),

            col("o.quantity"),

            col("o.price"),

            col("o.discount"),

            spark_round(
                col("o.profit"),
                2
            ).alias("profit"),

            col("o.pipeline_run_id"),

            col("o.source_system"),

            col("o.source_file_name"),

            col("o.ingestion_timestamp"),

            col("o.load_date"),

            # ==================================================
            # Customer
            # ==================================================

            col("c.customer_name"),

            col("c.segment"),

            col("c.country"),

            col("c.city"),

            col("c.state"),

            col("c.postal_code"),

            col("c.region"),

            col("c.customer_full_address"),

            col("c.customer_region"),

            # ==================================================
            # Product
            # ==================================================

            col("p.category").alias(
                "product_category"
            ),

            col("p.sub_category").alias(
                "product_sub_category"
            ),

            col("p.product_name"),

            col("p.category_name"),

            col("p.sub_category_name"),

            col("p.price_per_product")

        )

    )

    print()

    print("Sales Master Built Successfully.")

    print()

    print("Sales Master Schema")

    sales_master.printSchema()

    print()

    print(
        f"Sales Master Rows : {sales_master.count()}"
    )

    # ======================================================
    # Add Sales Master to Gold Dictionary
    # ======================================================

    gold_tables[
        f"{CATALOG}.{GOLD_SCHEMA}.sales_master"
    ] = sales_master

    # ==========================================================
    # Profit Summary
    # ==========================================================

    profit_summary = (

        sales_master

        .withColumn(

            "order_year",

            year(col("order_date"))

        )

        .groupBy(

            "order_year",

            "product_category",

            "product_sub_category",

            "customer_id",

            "customer_name"

        )

        .agg(

            spark_round(

                sum("profit"),

                2

            ).alias(

                "total_profit"

            )

        )

        .orderBy(

            "order_year",

            "product_category",

            "product_sub_category",

            "customer_name"

        )

    )

    # ======================================================
    # Add Profit Summary to Gold Dictionary
    # ======================================================

    gold_tables[
        f"{CATALOG}.{GOLD_SCHEMA}.profit_summary"
    ] = profit_summary



    # ======================================================
    # Customer Sales Summary
    # ======================================================

    print()
    print("=" * 70)
    print("Building Customer Sales Summary")
    print("=" * 70)

    customer_summary = (

        sales_master

        .groupBy(

            "customer_id",
            "customer_name",
            "segment",
            "country",
            "state",
            "city",
            "customer_region"

        )

        .agg(

            sum("quantity").alias("total_quantity"),

            sum("price").alias("total_sales"),

            sum("profit").alias("total_profit"),

            avg("discount").alias("average_discount"),

            countDistinct("order_id").alias("total_orders")

        )

    )

    print("Customer Summary Built Successfully.")

    print()

    customer_summary.printSchema()

    print()

    print(
        f"Customer Summary Rows : {customer_summary.count()}"
    )

    gold_tables[

        f"{CATALOG}.{GOLD_SCHEMA}.customer_summary"

    ] = customer_summary


    # ======================================================
    # Product Sales Summary
    # ======================================================

    print()
    print("=" * 70)
    print("Building Product Sales Summary")
    print("=" * 70)

    product_summary = (

        sales_master

        .groupBy(

            "product_id",
            "product_name",
            "product_category",
            "product_sub_category"

        )

        .agg(

            sum("quantity").alias("total_quantity"),

            sum("price").alias("total_sales"),

            sum("profit").alias("total_profit"),

            avg("discount").alias("average_discount"),

            countDistinct("order_id").alias("total_orders")

        )

    )

    print("Product Summary Built Successfully.")

    print()

    product_summary.printSchema()

    print()

    print(
        f"Product Summary Rows : {product_summary.count()}"
    )

    gold_tables[

        f"{CATALOG}.{GOLD_SCHEMA}.product_summary"

    ] = product_summary


    # ======================================================
    # Daily Sales Summary
    # ======================================================

    print()
    print("=" * 70)
    print("Building Daily Sales Summary")
    print("=" * 70)

    daily_sales = (

        sales_master

        .groupBy(

            "order_date"

        )

        .agg(

            sum("quantity").alias("total_quantity"),

            sum("price").alias("total_sales"),

            sum("profit").alias("total_profit"),

            avg("discount").alias("average_discount"),

            countDistinct("order_id").alias("total_orders")

        )

        .orderBy(

            "order_date"

        )

    )

    print("Daily Sales Summary Built Successfully.")

    print()

    daily_sales.printSchema()

    print()

    print(
        f"Daily Sales Rows : {daily_sales.count()}"
    )

    gold_tables[

        f"{CATALOG}.{GOLD_SCHEMA}.daily_sales"

    ] = daily_sales


        # ======================================================
    # Gold Build Summary
    # ======================================================

    print()
    print("=" * 70)
    print("GOLD TABLE BUILD COMPLETED")
    print("=" * 70)

    print(
        f"Total Gold Tables Built : {len(gold_tables)}"
    )

    print()

    for table_name in gold_tables.keys():

        print(
            f"Gold Table Ready : {table_name}"
        )

    print("=" * 70)

    # ======================================================
    # Return Gold DataFrames
    # ======================================================

    return gold_tables















