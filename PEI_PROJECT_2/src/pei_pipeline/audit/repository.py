"""
==========================================================
Audit Repository
==========================================================

Provides reusable functions for writing audit information.

Responsibilities
----------------
- Pipeline run logging
- Schema validation logging
- Rejected record logging

No business logic.
"""

from pyspark.sql import Row

from pei_pipeline.config.settings import (
    PIPELINE_RUN_LOG_TABLE,
    SCHEMA_VALIDATION_LOG_TABLE,
    REJECTED_RECORDS_TABLE
)
from uuid import uuid4

from pyspark.sql.functions import (
    lit,
    current_timestamp,
    to_json,
    struct,
    expr
)

def log_pipeline_run(
    spark,
    run_id: str,
    attempt_id: str,
    attempt_number: int,
    pipeline_name: str,
    pipeline_stage: str,
    source_name: str,
    source_file_name: str,
    archived_file_name: str,
    source_table: str,
    target_table: str,
    status: str,
    records_read: int,
    records_written: int,
    rejected_records: int,
    start_time,
    end_time,
    error_message: str = ""
):
    """
    Write pipeline execution log.
    """

    data = [

        Row(
            run_id=run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            pipeline_name=pipeline_name,
            pipeline_stage=pipeline_stage,
            source_name=source_name,
            source_file_name=source_file_name,
            archived_file_name=archived_file_name,
            source_table=source_table,
            target_table=target_table,
            start_time=start_time,
            end_time=end_time,
            status=status,
            records_read=records_read,
            records_written=records_written,
            rejected_records=rejected_records,
            error_message=error_message
        )

    ]

    spark.createDataFrame(data) \
         .write \
         .mode("append") \
         .saveAsTable(PIPELINE_RUN_LOG_TABLE)


def log_schema_validation(
    spark,
    validation_results: list[dict]
):
    """
    Write schema validation results.
    """

    if not validation_results:
        return

    spark.createDataFrame(validation_results) \
         .write \
         .mode("append") \
         .saveAsTable(SCHEMA_VALIDATION_LOG_TABLE)





def log_rejected_records(
    invalid_df,
    run_id,
    pipeline_stage,
    table_name,
    source_name,
    source_file_name
):
    """
    Write rejected records into audit.rejected_records.

    Parameters
    ----------
    invalid_df
        DataFrame containing rejected records.

    run_id
        Pipeline Run ID.

    pipeline_stage
        Schema Validation / Data Quality / Enrichment.

    table_name
        Target table.

    source_name
        Source dataset.

    source_file_name
        Original source file.
    """

    if invalid_df is None:
        return

    if invalid_df.limit(1).count() == 0:
        return

    rejected_df = (

        invalid_df

        .withColumn(
            "rejection_id",
            expr("uuid()")
        )

        .withColumn(
            "run_id",
            lit(run_id)
        )

        .withColumn(
            "pipeline_stage",
            lit(pipeline_stage)
        )

        .withColumn(
            "table_name",
            lit(table_name)
        )

        .withColumn(
            "source_name",
            lit(source_name)
        )

        .withColumn(
            "source_file_name",
            lit(source_file_name)
        )

        .withColumn(
            "raw_record",
            to_json(
                struct(*invalid_df.columns)
            )
        )

        .withColumn(
            "rejected_timestamp",
            current_timestamp()
        )

    )

    rejected_df.select(

        "rejection_id",

        "run_id",

        "pipeline_stage",

        "table_name",

        "source_name",

        "source_file_name",

        "failed_column",

        "expected_data_type",

        "actual_value",

        "rejection_reason",

        "raw_record",

        "rejected_timestamp"

    ).write.mode("append").saveAsTable(

        REJECTED_RECORDS_TABLE

    )