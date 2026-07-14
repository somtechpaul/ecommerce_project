"""
==========================================================
Data Enrichment
==========================================================

Reusable DataFrame enrichment functions.

Responsibilities
----------------
- Add metadata columns
- Add derived columns
- Business enrichment

No file reading.
No table writing.
No logging.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp, current_date,
    lit
)
from pyspark.sql.functions import col


def add_metadata_columns(
    df: DataFrame,
    run_id: str,
    source_file: str,
    source_system: str
) -> DataFrame:
    """
    Add metadata columns required for Bronze ingestion.
    """

    return (

        df

        .withColumn(
            "pipeline_run_id",
            lit(run_id)
        )

        .withColumn(
            "source_file_name",
            lit(source_file)
        )

        .withColumn(
            "source_system",
            lit(source_system)
        )

        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "ingestion_date",
            current_date()
        )

    )


def cast_business_columns_to_string(df):

    metadata_columns = [
        "pipeline_run_id",
        "source_file_name",
        "source_system",
        "ingestion_timestamp",
        "ingestion_date"
    ]

    for column in df.columns:

        if column not in metadata_columns:

            df = df.withColumn(
                column,
                col(column).cast("string")
            )

    return df



from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    concat_ws,
    upper,
    col
)

def enrich_customer(
    spark,
    df: DataFrame
) -> DataFrame:
    """
    Customer enrichment.
    """

    return (

        df

        .withColumn(

            "customer_full_address",

            concat_ws(

                ", ",

                col("address"),
                col("city"),
                col("state"),
                col("country")

            )

        )

        .withColumn(

            "customer_region",

            upper(

                col("region")

            )

        )

    )




from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    initcap,
    col
)

def enrich_products(
    spark,
    df: DataFrame
) -> DataFrame:
    """
    Product enrichment.
    """

    return (

        df

        .withColumn(

            "category_name",

            initcap(

                col("category")

            )

        )

        .withColumn(

            "sub_category_name",

            initcap(

                col("sub_category")

            )

        )

    )


def enrich_orders(
    spark,
    df: DataFrame
) -> DataFrame:

    return df




ENRICHMENT_FUNCTIONS = {

    "customer": enrich_customer,

    "products": enrich_products,

    "orders": enrich_orders

}

def enrich_dataframe(
    spark,
    df: DataFrame,
    source_name: str
) -> DataFrame:
    """
    Apply business enrichment for a source.

    Parameters
    ----------
    spark
        Spark Session

    df
        Input DataFrame

    source_name
        Source name from metadata.

    Returns
    -------
    DataFrame
        Enriched DataFrame.
    """

    print("=" * 60)
    print("Business Enrichment")
    print("=" * 60)
    print(f"Source : {source_name}")

    enrichment_function = ENRICHMENT_FUNCTIONS.get(
        source_name.lower()
    )

    if enrichment_function is None:

        print(
            f"No enrichment configured for {source_name}."
        )

        print("=" * 60)

        return df

    print(
        f"Applying enrichment : {enrichment_function.__name__}"
    )

    enriched_df = enrichment_function(
        spark=spark,
        df=df
    )

    print("Business Enrichment Completed.")
    print("=" * 60)

    return enriched_df
