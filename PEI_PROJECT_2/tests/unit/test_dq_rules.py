"""
==========================================================
Unit Tests
Data Quality Engine
==========================================================

Tests metadata-driven Data Quality Engine.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType
)

from pei_pipeline.quality.engine import (
    run_quality_checks
)


# ==========================================================
# Reusable Rules Schema
# ==========================================================

RULE_SCHEMA = StructType([

    StructField("table_name", StringType(), False),

    StructField("column_name", StringType(), False),

    StructField("rule_type", StringType(), False),

    StructField("rule_value", StringType(), True),

    StructField("active_flag", BooleanType(), False)

])


# ==========================================================
# NOT NULL Rule
# ==========================================================

def test_not_null_rule(spark):

    df = spark.createDataFrame(

        [

            ("C001",),

            (None,)

        ],

        ["customer_id"]

    )

    rules_df = spark.createDataFrame(

        [

            (

                "customer",

                "customer_id",

                "NOT_NULL",

                None,

                True

            )

        ],

        schema=RULE_SCHEMA

    )

    result = run_quality_checks(

        df=df,

        rules_df=rules_df,

        run_id="TEST",

        table_name="customer"

    )

    assert result["metrics"]["total_records"] == 2

    assert result["metrics"]["valid_records"] == 1

    assert result["metrics"]["rejected_records"] == 1


# ==========================================================
# UNIQUE Rule
# ==========================================================

def test_unique_rule(spark):

    df = spark.createDataFrame(

        [

            ("P001",),

            ("P001",),

            ("P002",)

        ],

        ["product_id"]

    )

    rules_df = spark.createDataFrame(

        [

            (

                "products",

                "product_id",

                "UNIQUE",

                None,

                True

            )

        ],

        schema=RULE_SCHEMA

    )

    result = run_quality_checks(

        df=df,

        rules_df=rules_df,

        run_id="TEST",

        table_name="products"

    )

    assert result["metrics"]["total_records"] == 3

    assert result["metrics"]["valid_records"] == 1

    assert result["metrics"]["rejected_records"] == 2


# ==========================================================
# POSITIVE Rule
# ==========================================================

def test_positive_rule(spark):

    df = spark.createDataFrame(

        [

            (100.0,),

            (-25.0,),

            (50.0,)

        ],

        ["price"]

    )

    rules_df = spark.createDataFrame(

        [

            (

                "products",

                "price",

                "POSITIVE",

                None,

                True

            )

        ],

        schema=RULE_SCHEMA

    )

    result = run_quality_checks(

        df=df,

        rules_df=rules_df,

        run_id="TEST",

        table_name="products"

    )

    assert result["metrics"]["total_records"] == 3

    assert result["metrics"]["valid_records"] == 2

    assert result["metrics"]["rejected_records"] == 1


# ==========================================================
# Multiple Rules
# ==========================================================

def test_multiple_rules(spark):

    df = spark.createDataFrame(

        [

            ("P001", 10),

            ("P001", -5),

            (None, 20)

        ],

        [

            "product_id",

            "quantity"

        ]

    )

    rules_df = spark.createDataFrame(

        [

            (

                "products",

                "product_id",

                "NOT_NULL",

                None,

                True

            ),

            (

                "products",

                "product_id",

                "UNIQUE",

                None,

                True

            ),

            (

                "products",

                "quantity",

                "POSITIVE",

                None,

                True

            )

        ],

        schema=RULE_SCHEMA

    )

    result = run_quality_checks(

        df=df,

        rules_df=rules_df,

        run_id="TEST",

        table_name="products"

    )

    assert result["metrics"]["total_records"] == 3

    assert result["metrics"]["valid_records"] == 0

    assert result["metrics"]["rejected_records"] >= 3


# ==========================================================
# Unsupported Rule
# ==========================================================

def test_unknown_rule(spark):

    df = spark.createDataFrame(

        [

            ("ABC",)

        ],

        ["code"]

    )

    rules_df = spark.createDataFrame(

        [

            (

                "dummy",

                "code",

                "REGEX",

                None,

                True

            )

        ],

        schema=RULE_SCHEMA

    )

    result = run_quality_checks(

        df=df,

        rules_df=rules_df,

        run_id="TEST",

        table_name="dummy"

    )

    assert result["metrics"]["total_records"] == 1

    assert result["metrics"]["valid_records"] == 1

    assert result["metrics"]["rejected_records"] == 0


# ==========================================================
# Empty Rules
# ==========================================================

def test_empty_rules(spark):

    df = spark.createDataFrame(

        [

            ("A",)

        ],

        ["id"]

    )

    rules_df = spark.createDataFrame(

        [],

        schema=RULE_SCHEMA

    )

    result = run_quality_checks(

        df=df,

        rules_df=rules_df,

        run_id="TEST",

        table_name="dummy"

    )

    assert result["metrics"]["total_records"] == 1

    assert result["metrics"]["valid_records"] == 1

    assert result["metrics"]["rejected_records"] == 0


# ==========================================================
# All Rows Valid
# ==========================================================

def test_all_rows_valid(spark):

    df = spark.createDataFrame(

        [

            (10,),

            (20,),

            (30,)

        ],

        ["quantity"]

    )

    rules_df = spark.createDataFrame(

        [

            (

                "orders",

                "quantity",

                "POSITIVE",

                None,

                True

            )

        ],

        schema=RULE_SCHEMA

    )

    result = run_quality_checks(

        df=df,

        rules_df=rules_df,

        run_id="TEST",

        table_name="orders"

    )

    assert result["metrics"]["valid_records"] == 3

    assert result["metrics"]["rejected_records"] == 0


# ==========================================================
# All Rows Invalid
# ==========================================================

def test_all_rows_invalid(spark):

    df = spark.createDataFrame(

        [

            (-1,),

            (-5,),

            (-100,)

        ],

        ["quantity"]

    )

    rules_df = spark.createDataFrame(

        [

            (

                "orders",

                "quantity",

                "POSITIVE",

                None,

                True

            )

        ],

        schema=RULE_SCHEMA

    )

    result = run_quality_checks(

        df=df,

        rules_df=rules_df,

        run_id="TEST",

        table_name="orders"

    )

    assert result["metrics"]["valid_records"] == 0

    assert result["metrics"]["rejected_records"] == 3

    assert result["rejected_df"].count() == 3