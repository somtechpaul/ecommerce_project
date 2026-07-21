"""
==========================================================
Enrichment Pipeline
==========================================================

Executes current-batch business enrichment transformations.

Write strategy
--------------
FULL
    Overwrite the complete enriched table.

INCREMENTAL
    Merge the current pipeline batch using configured
    business keys.

CDC
    Not supported until explicit insert, update and delete
    operation handling is implemented.
"""

from datetime import datetime
import traceback

from pyspark.sql.functions import (
    col,
    lit
)

from pei_pipeline.metadata.repository import (
    get_ingestion_config
)

from pei_pipeline.transformations.enrichment import (
    enrich_dataframe
)

from pei_pipeline.io.table_writer import (
    write_table
)

from pei_pipeline.io.table_reader import (
    read_table
)

from pei_pipeline.audit.repository import (
    log_pipeline_run
)


def run_enrichment_pipeline(
    spark,
    run_id,
    attempt_id,
    attempt_number,
    start_time
):
    """
    Execute the metadata-driven business enrichment pipeline.
    """

    print()
    print("=" * 70)
    print("PEI ENRICHMENT PIPELINE")
    print("=" * 70)
    print(f"Pipeline Run ID : {run_id}")
    print(f"Attempt ID      : {attempt_id}")
    print(f"Attempt Number  : {attempt_number}")
    print(f"Start Time      : {start_time}")
    print("=" * 70)

    # ==========================================================
    # Read and validate configuration
    # ==========================================================

    configs = get_ingestion_config(
        spark
    )

    required_config_columns = {
        "source_name",
        "dq_pass_table",
        "enrich_table",
        "load_type",
        "primary_keys"
    }

    missing_config_columns = sorted(
        required_config_columns
        - set(configs.columns)
    )

    if missing_config_columns:

        raise ValueError(
            "Ingestion configuration is missing required "
            f"columns: {missing_config_columns}"
        )

    config_rows = configs.collect()

    print(
        f"Active Sources Found : {len(config_rows)}"
    )

    # ==========================================================
    # Pipeline counters
    # ==========================================================

    processed_tables = 0
    failed_tables = 0
    skipped_tables = 0

    total_rows_read = 0
    total_rows_written = 0

    # ==========================================================
    # Process configured sources
    # ==========================================================

    for config in config_rows:

        source_name = (
            config["source_name"]
            or ""
        ).strip()

        source_table = (
            config["dq_pass_table"]
            or ""
        ).strip()

        target_table = (
            config["enrich_table"]
            or ""
        ).strip()

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
        print(f"Source          : {source_name}")
        print(f"Source Table    : {source_table}")
        print(f"Target Table    : {target_table}")
        print(f"Load Type       : {load_type}")
        print(f"Primary Keys    : {primary_keys}")
        print("-" * 70)

        # ======================================================
        # Per-source execution context
        # ======================================================

        status = "SUCCESS"
        error_message = ""

        rows_read = 0
        rows_written = 0

        source_file_id = ""
        source_file_name = ""
        source_file_count = 0

        table_start_time = datetime.now()

        try:

            # ==================================================
            # Validate source configuration
            # ==================================================

            if not source_name:

                raise ValueError(
                    "source_name cannot be empty."
                )

            if not source_table:

                raise ValueError(
                    f"dq_pass_table is not configured for "
                    f"source '{source_name}'."
                )

            # Orders currently does not require a separate
            # enrichment table. It is used from dq_pass_table
            # when the sales master is constructed.
            if not target_table:

                status = "SKIPPED"
                skipped_tables += 1

                print(
                    f"No enrichment target is configured for "
                    f"source '{source_name}'. "
                    "Skipping enrichment."
                )

                continue

            if source_table == target_table:

                raise ValueError(
                    f"Enrichment source and target tables must "
                    f"be different for source '{source_name}'."
                )

            # ==================================================
            # Step 1: Read current DQ-passed batch
            # ==================================================

            print()
            print("Step 1 : Reading Source Table...")
            print(f"Reading Table : {source_table}")

            dq_passed_df = read_table(
                spark=spark,
                table_name=source_table
            )

            required_lineage_columns = {
                "pipeline_run_id",
                "source_file_id",
                "source_file_name"
            }

            missing_lineage_columns = sorted(
                required_lineage_columns
                - set(dq_passed_df.columns)
            )

            if missing_lineage_columns:

                raise ValueError(
                    f"DQ-passed table '{source_table}' is "
                    f"missing required lineage columns: "
                    f"{missing_lineage_columns}"
                )

            dq_passed_df = dq_passed_df.filter(
                col("pipeline_run_id") == lit(run_id)
            )

            if dq_passed_df.limit(1).count() == 0:

                status = "SKIPPED"
                skipped_tables += 1

                print(
                    f"No current-batch DQ-passed records found "
                    f"for {source_name}, "
                    f"pipeline_run_id={run_id}"
                )

                continue

            source_files = (
                dq_passed_df
                .select(
                    "source_file_id",
                    "source_file_name"
                )
                .distinct()
                .collect()
            )

            if any(
                row["source_file_id"] is None
                for row in source_files
            ):

                raise ValueError(
                    f"Null source_file_id found in the current "
                    f"DQ-passed batch for '{source_name}'."
                )

            source_file_count = len(
                source_files
            )

            if source_file_count == 1:

                source_file_id = (
                    source_files[0]["source_file_id"]
                )

                source_file_name = (
                    source_files[0]["source_file_name"]
                )

            else:

                source_file_id = "MULTIPLE"
                source_file_name = "MULTIPLE"

            rows_read = dq_passed_df.count()

            total_rows_read += rows_read

            print("Table read completed.")
            print(f"Rows Read : {rows_read}")
            print("Input Schema:")

            dq_passed_df.printSchema()

            # ==================================================
            # Step 2: Execute business enrichment
            # ==================================================

            print()
            print(
                "Step 2 : Executing Business Enrichment..."
            )

            enriched_df = enrich_dataframe(
                spark=spark,
                df=dq_passed_df,
                source_name=source_name
            )

            rows_to_write = enriched_df.count()

            print(
                "Business Enrichment Completed."
            )

            print(
                f"Rows After Enrichment : {rows_to_write}"
            )

            print("Enriched Schema:")

            enriched_df.printSchema()

            # ==================================================
            # Step 3: Write enriched table
            # ==================================================

            print()
            print(
                "Step 3 : Writing Enriched Table..."
            )

            if load_type == "FULL":

                write_table(
                    df=enriched_df,
                    table_name=target_table,
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
                    if key not in enriched_df.columns
                ]

                if missing_primary_keys:

                    raise ValueError(
                        f"Primary keys are missing from the "
                        f"enriched DataFrame: "
                        f"{missing_primary_keys}"
                    )

                write_table(
                    df=enriched_df,
                    table_name=target_table,
                    mode="merge",
                    merge_keys=primary_keys
                )

            elif load_type == "CDC":

                raise NotImplementedError(
                    f"CDC enrichment is configured for "
                    f"'{source_name}', but CDC insert, "
                    "update and delete handling has not "
                    "been implemented."
                )

            else:

                raise ValueError(
                    f"Unsupported load_type '{load_type}' "
                    f"for source '{source_name}'."
                )

            # Mark rows as written only after the Delta write
            # completes successfully.
            rows_written = rows_to_write

            total_rows_written += rows_written
            processed_tables += 1

            print(
                "Enriched Table Written Successfully."
            )

            print()
            print(
                f"Enrichment Completed Successfully : "
                f"{source_name}"
            )

        except Exception:

            status = "FAILED"
            failed_tables += 1
            error_message = traceback.format_exc()

            print()
            print(
                f"Enrichment Failed : {source_name}"
            )
            print(
                f"Error Message : {error_message}"
            )

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
                    pipeline_stage="Enrichment",
                    source_name=source_name,
                    source_file_id=source_file_id,
                    source_file_count=source_file_count,
                    source_file_name=source_file_name,
                    archived_file_name="",
                    source_table=source_table,
                    target_table=target_table,
                    status=status,
                    records_read=rows_read,
                    records_written=rows_written,
                    rejected_records=0,
                    start_time=table_start_time,
                    end_time=datetime.now(),
                    error_message=error_message
                )

                print(
                    "Pipeline Audit Written."
                )

            except Exception as audit_exception:

                print(
                    "Pipeline Audit Logging Failed."
                )

                print(
                    f"Audit Error : {audit_exception}"
                )

                traceback.print_exc()

                raise RuntimeError(
                    f"Enrichment audit logging failed for "
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
    # Pipeline summary
    # ==========================================================

    print()
    print("=" * 70)
    print("PEI ENRICHMENT PIPELINE COMPLETED")
    print("=" * 70)
    print(f"Status            : {pipeline_status}")
    print(f"Processed Tables  : {processed_tables}")
    print(f"Failed Tables     : {failed_tables}")
    print(f"Skipped Tables    : {skipped_tables}")
    print(f"Rows Read         : {total_rows_read}")
    print(f"Rows Written      : {total_rows_written}")
    print("=" * 70)

    return {
        "stage": "enrichment",
        "run_id": run_id,
        "status": pipeline_status,
        "processed_tables": processed_tables,
        "failed_tables": failed_tables,
        "skipped_tables": skipped_tables,
        "rows_read": total_rows_read,
        "rows_written": total_rows_written
    }