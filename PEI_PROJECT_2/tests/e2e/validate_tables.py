"""
==========================================================
PEI Table Validation
==========================================================

Reusable validation functions for PEI pipeline.

Responsibilities
----------------
- Validate Bronze tables
- Validate Silver tables
- Validate Enriched tables
- Validate Gold tables
- Validate Audit tables

No pipeline execution.
"""

# ==========================================================
# Helper
# ==========================================================

def validate_table(

    spark,

    table_name,

    minimum_rows=1

):
    """
    Validate table existence and row count.
    """

    print()

    print(f"Checking {table_name}")

    df = spark.table(table_name)

    row_count = df.count()

    print(f"Rows : {row_count}")

    if row_count < minimum_rows:

        raise RuntimeError(

            f"{table_name} contains only "

            f"{row_count} rows."

        )

    return df


# ==========================================================
# Validate Columns
# ==========================================================

def validate_columns(

    dataframe,

    required_columns

):

    missing = [

        column

        for column in required_columns

        if column not in dataframe.columns

    ]

    if missing:

        raise RuntimeError(

            f"Missing columns : {missing}"

        )



# ==========================================================
# Bronze
# ==========================================================

def validate_bronze(

    spark

):

    print()

    print("=" * 70)

    print("BRONZE VALIDATION")

    print("=" * 70)

    customer = validate_table(

        spark,

        "ecommerce_dev.bronze.customer"

    )

    validate_columns(

        customer,

        [

            "customer_id",

            "pipeline_run_id",

            "source_file_name"

        ]

    )

    products = validate_table(

        spark,

        "ecommerce_dev.bronze.products"

    )

    orders = validate_table(

        spark,

        "ecommerce_dev.bronze.orders"

    )

    return {

        "customer": customer,

        "products": products,

        "orders": orders

    }



# ==========================================================
# Silver
# ==========================================================

def validate_silver(

    spark

):

    print()

    print("=" * 70)

    print("SILVER VALIDATION")

    print("=" * 70)

    customer = validate_table(

        spark,

        "ecommerce_dev.silver.customer_valid"

    )

    validate_columns(

        customer,

        [

            "customer_id",

            "customer_name",

            "email"

        ]

    )

    products = validate_table(

        spark,

        "ecommerce_dev.silver.products_valid"

    )

    orders = validate_table(

        spark,

        "ecommerce_dev.silver.orders_valid"

    )

    return {

        "customer": customer,

        "products": products,

        "orders": orders

    }




# ==========================================================
# Enriched
# ==========================================================

def validate_enriched(

    spark

):

    print()

    print("=" * 70)

    print("ENRICHMENT VALIDATION")

    print("=" * 70)

    customer = validate_table(

        spark,

        "ecommerce_dev.silver.customer_enriched"

    )

    validate_columns(

        customer,

        [

            "customer_full_address",

            "customer_region"

        ]

    )

    products = validate_table(

        spark,

        "ecommerce_dev.silver.product_enriched"

    )

    validate_columns(

        products,

        [

            "category_name",

            "sub_category_name"

        ]

    )

    return {

        "customer": customer,

        "products": products

    }



# ==========================================================
# Gold
# ==========================================================

def validate_gold(

    spark

):

    print()

    print("=" * 70)

    print("GOLD VALIDATION")

    print("=" * 70)

    sales_master = validate_table(

        spark,

        "ecommerce_dev.gold.sales_master"

    )

    validate_columns(

        sales_master,

        [

            "customer_name",

            "product_name"

        ]

    )

    customer_summary = validate_table(

        spark,

        "ecommerce_dev.gold.customer_summary"

    )

    validate_columns(

        customer_summary,

        [

            "total_sales",

            "total_profit"

        ]

    )

    product_summary = validate_table(

        spark,

        "ecommerce_dev.gold.product_summary"

    )

    validate_columns(

        product_summary,

        [

            "total_quantity"

        ]

    )

    daily_sales = validate_table(

        spark,

        "ecommerce_dev.gold.daily_sales"

    )

    validate_columns(

        daily_sales,

        [

            "order_date"

        ]

    )



# ==========================================================
# Audit
# ==========================================================

def validate_audit(

    spark

):

    print()

    print("=" * 70)

    print("AUDIT VALIDATION")

    print("=" * 70)

    audit = validate_table(

        spark,

        "ecommerce_dev.audit.pipeline_run_log"

    )

    validate_columns(

        audit,

        [

            "run_id",

            "pipeline_stage",

            "status"

        ]

    )




# ==========================================================
# Execute All Validations
# ==========================================================

def validate_pipeline(

    spark

):

    validate_bronze(

        spark

    )

    validate_silver(

        spark

    )

    validate_enriched(

        spark

    )

    validate_gold(

        spark

    )

    validate_audit(

        spark

    )

    print()

    print("=" * 70)

    print("ALL TABLE VALIDATIONS PASSED")

    print("=" * 70)



profit_summary = validate_table(
    spark,
    "ecommerce_dev.gold.profit_summary"
)

validate_columns(
    profit_summary,
    [
        "order_year",
        "product_category",
        "product_sub_category",
        "customer_id",
        "customer_name",
        "total_profit"
    ]
)




