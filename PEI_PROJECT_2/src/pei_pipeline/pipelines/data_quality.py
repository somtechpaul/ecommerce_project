"""
==========================================================
Data Quality Pipeline
==========================================================

Executes metadata-driven Data Quality rules against
schema-validated tables.

Pipeline Flow
-------------
Validated Table
        |
        v
Read Data Quality Rules
        |
        v
Execute Data Quality Checks
        |
        +----------------------+
        |                      |
        v                      v
DQ Passed Records       DQ Failed Records
        |                      |
        v                      v
<table>_valid           <table>_invalid
        |
        v
Pipeline Audit
"""

import traceback

from datetime import datetime
from pyspark.sql.functions import (
    col,
    lit
)
from pei_pipeline.metadata.repository import (
    get_ingestion_config,
    get_data_quality_rules
)

from pei_pipeline.quality.engine import (
    run_quality_checks
)

from pei_pipeline.io.table_writer import (
    write_table
)

from pei_pipeline.io.table_reader import (
    read_table
)

from pei_pipeline.audit.repository import log_pipeline_run


def run_data_quality_pipeline(
    spark,
    run_id,
    attempt_id,
    attempt_number,
    start_time
):
    """
    Execute the metadata-driven Data Quality pipeline.

    Parameters
    ----------
    spark
        Active Spark session.

    run_id
        Unique pipeline execution ID.

    start_time
        Pipeline execution start timestamp.

    Returns
    -------
    dict
        Overall Data Quality pipeline execution summary.
    """

    print()
    print("=" * 70)
    print("PEI DATA QUALITY PIPELINE")
    print("=" * 70)
    print(f"Pipeline Run ID : {run_id}")
    print("=" * 70)

    # ==========================================================
    # Read active ingestion configurations
    # ==========================================================

    configs = get_ingestion_config(spark)

    config_rows = configs.collect()

    print(f"Active Sources Found : {len(config_rows)}")

    # ==========================================================
    # Overall pipeline counters
    # ==========================================================

    processed_tables = 0
    failed_tables = 0
    skipped_tables = 0

    total_rows_read = 0
    total_valid_written = 0
    total_invalid_written = 0

    # ==========================================================
    # Process every configured source
    # ==========================================================

    for config in config_rows:

        source_name = config["source_name"]

        # Input to the DQ pipeline
        validated_table = config["validated_table"]

        # Outputs from the DQ pipeline
        dq_pass_table = config["dq_pass_table"]
        dq_failed_table = config["dq_failed_table"]

        load_type = (
            config["load_type"]
            or ""
        ).strip().upper()

        primary_keys_value = (
            config["primary_keys"]
            or ""
        )

        primary_keys = [
            key.strip()
            for key in primary_keys_value.split(",")
            if key.strip()
        ]

        print()
        print("-" * 70)
        print(f"Source           : {source_name}")
        print(f"Validated Table  : {validated_table}")
        print(f"DQ Pass Table    : {dq_pass_table}")
        print(f"DQ Failed Table  : {dq_failed_table}")
        print("-" * 70)

        # ======================================================
        # Per-source execution variables
        # ======================================================

        status = "SUCCESS"
        error_message = ""

        rows_read = 0
        valid_records = 0
        rejected_records = 0

        source_file_id = ""
        source_file_name = ""
        source_file_count = 0
        table_start_time = datetime.now()

        try:

            # ==================================================
            # Validate table configuration
            # ==================================================

            if not validated_table:

                raise ValueError(
                    f"validated_table is not configured for source "
                    f"'{source_name}'"
                )

            if not dq_pass_table:

                raise ValueError(
                    f"dq_pass_table is not configured for source "
                    f"'{source_name}'"
                )

            if not dq_failed_table:

                raise ValueError(
                    f"dq_failed_table is not configured for source "
                    f"'{source_name}'"
                )

            # ==================================================
            # Step 1: Read validated table
            # ==================================================

            print()
            print("Step 1 : Reading Validated Table...")
            print(f"Reading Table : {validated_table}")

            validated_df = read_table(
                spark=spark,
                table_name=validated_table
            )

            required_lineage_columns = {
                "pipeline_run_id",
                "source_file_id",
                "source_file_name"
            }

            missing_lineage_columns = sorted(
                required_lineage_columns
                - set(validated_df.columns)
            )

            if missing_lineage_columns:

                raise ValueError(
                    f"Validated table '{validated_table}' is missing "
                    f"required lineage columns: "
                    f"{missing_lineage_columns}"
                )


            validated_df = validated_df.filter(
                col("pipeline_run_id") == lit(run_id)
            )


            if validated_df.limit(1).count() == 0:

                status = "SKIPPED"
                skipped_tables += 1

                print(
                    f"No current-batch validated records found for "
                    f"{source_name}, pipeline_run_id={run_id}"
                )

                continue


            source_files = (
                validated_df
                .select(
                    "source_file_id",
                    "source_file_name"
                )
                .distinct()
                .collect()
            )

            source_file_count = len(source_files)

            if source_file_count == 1:

                source_file_id = source_files[0]["source_file_id"]
                source_file_name = source_files[0]["source_file_name"]

            else:

                source_file_id = "MULTIPLE"
                source_file_name = "MULTIPLE"
            
            if any(
                row["source_file_id"] is None
                for row in source_files
            ):

                raise ValueError(
                    f"Null source_file_id found in the current "
                    f"validated batch for '{source_name}'."
                )


            rows_read = validated_df.count()

            total_rows_read += rows_read

            print(f"Rows Read : {rows_read}")
            print("Validated Schema :")

            validated_df.printSchema()

            # ==================================================
            # Step 2: Read Data Quality rules
            # ==================================================

            print()
            print("Step 2 : Reading Data Quality Rules...")

            rules_df = get_data_quality_rules(
                spark=spark,
                table_name=source_name
            )

            rules_count = rules_df.count()

            print(f"Rules Found : {rules_count}")

            if rules_count == 0:

                raise ValueError(
                    f"No active Data Quality rules are configured "
                    f"for source '{source_name}'."
                )

            # During development, uncomment this to inspect rules.
            #
            # rules_df.show(
            #     truncate=False
            # )

            # ==================================================
            # Step 3: Execute Data Quality rules
            # ==================================================

            print()
            print("Step 3 : Executing Data Quality Rules...")

            result = run_quality_checks(
                df=validated_df,
                rules_df=rules_df,
                run_id=run_id,
                table_name=source_name
            )

            valid_df = result["valid_df"]
            rejected_df = result["rejected_df"]
            metrics = result["metrics"]

            valid_records = metrics["valid_records"]
            rejected_records = metrics["rejected_records"]

            print("Data Quality Checks Completed.")
            print(f"Valid Records    : {valid_records}")
            print(f"Rejected Records : {rejected_records}")

            
            # ==================================================
            # Step 5: Write DQ-passed records
            # ==================================================

            print()
            print("Step 5 : Writing DQ Passed Records...")
            print(f"Target Table : {dq_pass_table}")
            print(f"Rows to Write : {valid_records}")

            if load_type == "FULL":

                write_table(
                    df=valid_df,
                    table_name=dq_pass_table,
                    mode="overwrite"
                )

            elif load_type == "INCREMENTAL":

                if not primary_keys:

                    raise ValueError(
                        f"primary_keys must be configured for "
                        f"incremental source '{source_name}'."
                    )

                missing_primary_keys = [
                    key
                    for key in primary_keys
                    if key not in valid_df.columns
                ]

                if missing_primary_keys:

                    raise ValueError(
                        f"Primary keys are missing from the "
                        f"DQ-passed DataFrame: "
                        f"{missing_primary_keys}"
                    )

                write_table(
                    df=valid_df,
                    table_name=dq_pass_table,
                    mode="merge",
                    merge_keys=primary_keys
                )

            elif load_type == "CDC":

                raise NotImplementedError(
                    f"CDC is configured for '{source_name}', "
                    "but CDC insert, update and delete handling "
                    "has not been implemented."
                )

            else:

                raise ValueError(
                    f"Unsupported load_type '{load_type}' "
                    f"for source '{source_name}'."
                )

            print(
                f"DQ Passed Records Written Successfully "
                f"to {dq_pass_table}"
            )

            # ==================================================
            # Step 6: Write DQ-failed records
            # ==================================================

            print()

            if rejected_df is not None and rejected_records > 0:
                print()
                print("Step 6 : Writing DQ Failed Records...")
                print(f"Target Table : {dq_failed_table}")
                print(f"Rows to Write : {rejected_records}")

                failed_output_df = (
                    rejected_df
                    if rejected_df is not None
                    else validated_df.limit(0)
                )

                escaped_run_id = run_id.replace(
                    "'",
                    "''"
                )

                write_table(
                    df=failed_output_df,
                    table_name=dq_failed_table,
                    mode="overwrite",
                    replace_where=(
                        "pipeline_run_id = "
                        f"'{escaped_run_id}'"
                    )
                )

                print(
                    f"DQ Failed current-run slice written "
                    f"successfully to {dq_failed_table}"
                )

            else:

                print("Step 6 : No DQ Failed Records to Write.")

            # ==================================================
            # Update successful execution counters
            # ==================================================

            processed_tables += 1

            total_valid_written += valid_records
            total_invalid_written += rejected_records

            print()
            print(
                f"Data Quality Completed Successfully : "
                f"{source_name}"
            )

        except Exception as ex:
            status = "FAILED"
            failed_tables += 1
            error_message = traceback.format_exc()
            print()
            print(f"Data Quality Failed : {source_name}")
            print(f"Error Message : {error_message}")

            print()
            print("Full Traceback")
            print("-" * 70)

            traceback.print_exc()

        finally:

            # ==================================================
            # Pipeline audit
            # ==================================================

            print()
            print("Writing Pipeline Audit...")

            try:

                log_pipeline_run(
                    spark=spark,
                    run_id=run_id,
                    attempt_id=attempt_id,
                    attempt_number=attempt_number,
                    pipeline_name="PEI Pipeline",
                    pipeline_stage="Data Quality",
                    source_name=source_name,
                    source_file_id=source_file_id,
                    source_file_count=source_file_count,
                    source_file_name=source_file_name,
                    archived_file_name="",
                    source_table=validated_table,
                    target_table=(
                        f"{dq_pass_table},"
                        f"{dq_failed_table}"
                    ),
                    status=status,
                    records_read=rows_read,
                    records_written=valid_records,
                    rejected_records=rejected_records,
                    start_time=table_start_time,
                    end_time=datetime.now(),
                    error_message=error_message
                )

                print("Pipeline Audit Written.")

            except Exception as audit_exception:

                print("Pipeline Audit Logging Failed.")
                print(f"Audit Error : {audit_exception}")

                traceback.print_exc()
                raise RuntimeError(
                    f"Data Quality audit logging failed for "
                    f"source '{source_name}'."
                ) from audit_exception

    # ==========================================================
    # Final pipeline status
    # ==========================================================

    if failed_tables == 0:
        pipeline_status = "SUCCESS"

    elif processed_tables == 0:
        pipeline_status = "FAILED"

    else:
        pipeline_status = "PARTIAL_SUCCESS"

    # ==========================================================
    # Final pipeline summary
    # ==========================================================

    print()
    print("=" * 70)
    print("PEI DATA QUALITY PIPELINE COMPLETED")
    print("=" * 70)
    print(f"Status                 : {pipeline_status}")
    print(f"Processed Tables       : {processed_tables}")
    print(f"Failed Tables          : {failed_tables}")
    print(f"Rows Read              : {total_rows_read}")
    print(f"Valid Rows Written     : {total_valid_written}")
    print(f"Rejected Rows Written  : {total_invalid_written}")
    print("=" * 70)

    return {
        "stage": "data_quality",
        "run_id": run_id,
        "status": pipeline_status,
        "processed_tables": processed_tables,
        "failed_tables": failed_tables,
        "skipped_tables": skipped_tables,
        "rows_read": total_rows_read,
        "valid_rows_written": total_valid_written,
        "rejected_rows_written": total_invalid_written
    }