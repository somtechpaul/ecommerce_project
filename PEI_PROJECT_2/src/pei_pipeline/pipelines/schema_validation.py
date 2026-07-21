"""
==========================================================
Schema Validation Pipeline
==========================================================

Purpose
-------
Reads Bronze tables, validates them against the
required metadata schema, casts columns to expected
datatypes and writes validated data to validated layer.

Workflow
--------
1. Read Bronze table
2. Read required schema metadata
3. Cast columns
4. Validate schema
5. Log schema validation
6. Write Validated
7. Audit Pipeline
"""

import traceback

from datetime import datetime
from pyspark.sql.functions import (
    col,
    lit
)
from pei_pipeline.metadata.repository import (
    get_ingestion_config,
    get_table_schema
)

from pei_pipeline.schema.casting import (
    cast_columns
)

from pei_pipeline.schema.validation import (
    validate_schema
)

from pei_pipeline.io.table_writer import (
    write_table
)

from pei_pipeline.io.table_reader import read_table

from pei_pipeline.audit.repository import (
    log_pipeline_run,
    log_schema_validation,
    log_rejected_records
)

from pei_pipeline.quality.engine import (
    split_valid_invalid
)


def run_schema_validation_pipeline(
    spark,
    run_id,
    attempt_id,
    attempt_number,
    start_time
):
    """
    Execute Schema Validation Pipeline.
    """

    print("=" * 70, flush=True)
    print("PEI SCHEMA VALIDATION STARTED", flush=True)
    print(f"Run ID     : {run_id}", flush=True)
    print(f"Start Time : {start_time}", flush=True)
    print("=" * 70, flush=True)

    configs = get_ingestion_config(spark)

    config_rows = configs.collect()

    required_config_columns = {
        "source_name",
        "bronze_table",
        "validated_table"
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

    print(
        f"Source Configurations Found : {len(config_rows)}",
        flush=True
    )

    processed_tables = 0
    failed_tables = 0
    skipped_tables = 0

    total_rows_read = 0
    total_rows_written = 0

    for config in config_rows:

        table_start_time = datetime.now()

        status = "SUCCESS"
        error_message = ""

        rows_read = 0
        rows_written = 0

        source_name = config["source_name"]
        bronze_table = config["bronze_table"]
        validated_table = config["validated_table"]

        print("\n" + "-" * 70, flush=True)

        print(
            f"Source        : {source_name}",
            flush=True
        )

        print(
            f"Bronze Table  : {bronze_table}",
            flush=True
        )

        print(
            f"Validated Table  : {validated_table}",
            flush=True
        )

        print("-" * 70, flush=True)
 
        rejected_records = 0
        datatype_rejected_records = 0
        required_rejected_records = 0

        source_file_id = ""
        source_file_name = ""
        source_file_count = 0

        try:

            # =====================================================
            # Read Bronze
            # =====================================================

            print(
                "Step 1 : Reading Bronze Table...",
                flush=True
            )

            bronze_df = read_table(
                spark=spark,
                table_name=bronze_table
            )

            required_lineage_columns = {
                "pipeline_run_id",
                "source_file_id",
                "source_file_name"
            }

            missing_lineage_columns = sorted(
                required_lineage_columns
                - set(bronze_df.columns)
            )

            if missing_lineage_columns:
                raise ValueError(
                    f"Bronze table '{bronze_table}' is missing "
                    f"required lineage columns: "
                    f"{missing_lineage_columns}"
                )

            bronze_df = bronze_df.filter(
                col("pipeline_run_id") == lit(run_id)
            )

            if bronze_df.limit(1).count() == 0:
                status = "SKIPPED"
                skipped_tables += 1

                print(
                    f"No current-batch Bronze records found for "
                    f"{source_name}, pipeline_run_id={run_id}",
                    flush=True
                )

                continue

            source_files = (
                bronze_df
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
                    f"Null source_file_id found in current Bronze "
                    f"batch for {source_name}."
                )

            source_file_count = len(source_files)

            if source_file_count == 1:
                source_file_id = source_files[0]["source_file_id"]
                source_file_name = source_files[0]["source_file_name"]
            else:
                source_file_id = "MULTIPLE"
                source_file_name = "MULTIPLE"

            rows_read = bronze_df.count()

            total_rows_read += rows_read

            print(
                f"Rows Read : {rows_read}",
                flush=True
            )

            print(
                "Ingested Bronze Schema : ",
                flush=True
            )

            bronze_df.printSchema()

            # =====================================================
            # Read Required Schema
            # =====================================================

            print(
                "Step 2 : Reading required schema from table required_schema ...",
                flush=True
            )

            required_schema = get_table_schema(
                spark,
                source_name
            )

            required_column_count = required_schema.count()

            print(
                f"Required Columns : {required_column_count}",
                flush=True
            )

            if required_column_count == 0:
                raise ValueError(
                    f"No required schema metadata found for "
                    f"source_name='{source_name}'."
                )
            
            required_schema_rows = required_schema.collect()

            expected_columns = {
                row["standard_column"]
                for row in required_schema_rows
            }

            missing_columns = sorted(
                expected_columns
                - set(bronze_df.columns)
            )

            if missing_columns:

                validation_logs = [
                    {
                        "run_id": run_id,
                        "attempt_id": attempt_id,
                        "attempt_number": attempt_number,
                        "table_name": source_name,
                        "source_file_id": source_file_id,
                        "column_name": column_name,
                        "expected_type": "Present",
                        "actual_type": "Missing",
                        "validation_status": "FAILED"
                    }
                    for column_name in missing_columns
                ]

                log_schema_validation(
                    spark,
                    validation_logs
                )

                raise ValueError(
                    f"Required columns missing from "
                    f"'{bronze_table}': {missing_columns}"
                )



            # ==========================================================
            # Step 3: Safe datatype casting
            # ==========================================================

            print(
                "Step 3 : Casting Columns as per "
                "the required schema..."
            )

            casting_output = cast_columns(
                bronze_df,
                required_schema
            )

            casted_df = casting_output["casted_df"]

            datatype_invalid_df = (
                casting_output["datatype_invalid_df"]
            )

            datatype_metrics = casting_output["metrics"]

            datatype_rejected_records = (
                datatype_metrics["datatype_rejected_records"]
            )

            print(
                f"Columns Casting Completed for "
                f"{bronze_table}."
            )

            print(
                "Datatype Invalid Records : "
                f"{datatype_rejected_records}"
            )

            print("Validated Schema")

            casted_df.printSchema()


            if (
                datatype_invalid_df is not None
                and datatype_rejected_records > 0
            ):

                print(
                    "Logging Datatype Invalid Records..."
                )

                log_rejected_records(
                    invalid_df=datatype_invalid_df,
                    run_id=run_id,
                    pipeline_stage="Schema Validation",
                    table_name=validated_table,
                    source_name=source_name,
                    source_file_name=source_file_name
                )

                print(
                    "Datatype Invalid Records Logged."
                )
                                
            validation_output = split_valid_invalid(
                casted_df,
                required_schema
            )
            valid_df = validation_output["valid_df"]

            required_invalid_df = (
                validation_output["invalid_df"]
            )

            required_metrics = validation_output["metrics"]

            required_rejected_records = (
                required_metrics["rejected_records"]
            )

            if (
                required_invalid_df is not None
                and required_rejected_records > 0
            ):

                print(
                    "Logging required-column invalid records...",
                    flush=True
                )

                log_rejected_records(
                    invalid_df=required_invalid_df,
                    run_id=run_id,
                    pipeline_stage="Schema Validation",
                    table_name=validated_table,
                    source_name=source_name,
                    source_file_name=source_file_name
                )

                print(
                    "Required-column invalid records logged.",
                    flush=True
                )
            # =====================================================
            # Validate Schema
            # =====================================================

            print(
                "Step 4 : Validating Schema...",
                flush=True
            )

            validation_result = validate_schema(
                valid_df,
                required_schema
            )

            print("=" * 40, flush=True)

            print(
                "Schema Validation Summary : ",
                flush=True
            )

            print("=" * 40, flush=True)

            print(
                f"Schema Valid        : {validation_result['valid']}",
                flush=True
            )

            print(
                f"Missing Columns     : {len(validation_result['missing_columns'])}",
                flush=True
            )

            print(
                f"Datatype Mismatches : {len(validation_result['datatype_mismatches'])}",
                flush=True
            )

            validation_logs = []

            # =====================================================
            # Datatype Mismatches
            # =====================================================

            for mismatch in validation_result["datatype_mismatches"]:

                print(
                    f"{mismatch['column_name']} : "
                    f"{mismatch['actual_type']} "
                    f"-> "
                    f"{mismatch['expected_type']}",
                    flush=True
                )

                validation_logs.append({
                    "run_id": run_id,
                    "attempt_id": attempt_id,
                    "attempt_number": attempt_number,
                    "table_name": source_name,
                    "source_file_id": source_file_id,
                    "column_name": mismatch["column_name"],
                    "expected_type": mismatch["expected_type"],
                    "actual_type": mismatch["actual_type"],
                    "validation_status": "FAILED"
                })

            # =====================================================
            # Missing Columns
            # =====================================================

            for column in validation_result["missing_columns"]:

                print(
                    f"Missing Column : {column}",
                    flush=True
                )

                validation_logs.append({
                    "run_id": run_id,
                    "attempt_id": attempt_id,
                    "attempt_number": attempt_number,
                    "table_name": source_name,
                    "source_file_id": source_file_id,
                    "column_name": column,
                    "expected_type": "Present",
                    "actual_type": "Missing",
                    "validation_status": "FAILED"
                })

            if validation_logs:
                print(
                    "Writing Schema Validation Logs...",
                    flush=True
                )

                log_schema_validation(
                    spark,
                    validation_logs
                )

                print(
                    f"Validation Log Records : {len(validation_logs)}",
                    flush=True
                )

            # =====================================================
            # Stop Pipeline if Validation Failed
            # =====================================================

            if not validation_result["valid"]:
                raise Exception(
                    "Schema validation failed."
                )

            # =====================================================
            # Write Validated Table
            # =====================================================

            print(
                "Step 5 : Writing to Validated Table...",
                flush=True
            )
            
            # Schema Validation always writes an idempotent
            # current-batch slice. Business load strategy is
            # applied only after Data Quality.

            escaped_run_id = run_id.replace(
                "'",
                "''"
            )

            valid_record_count = (
                required_metrics["valid_records"]
            )

            rejected_records = (
                rows_read
                - valid_record_count
            )

            write_table(
                df=valid_df,
                table_name=validated_table,
                mode="overwrite",
                replace_where=(
                    "pipeline_run_id = "
                    f"'{escaped_run_id}'"
                )
            )

            rows_written = valid_record_count

            total_rows_written += rows_written
            processed_tables += 1

            print(
                f"Rows Written : {rows_written}",
                flush=True
            )

            print(
                "Validated Load Completed.",
                flush=True
            )

        except Exception as ex:

            status = "FAILED"
            failed_tables += 1
            error_message = traceback.format_exc()

            print(
                f"Schema Validation Failed : {source_name}",
                flush=True
            )

            print(
                error_message,
                flush=True
            )
            print("\nFull Traceback\n", flush=True)

            traceback.print_exc()

            print("=" * 70, flush=True)

        finally:

            table_end_time = datetime.now()

            print(
                "Writing Pipeline Audit...",
                flush=True
            )

            log_pipeline_run(
                spark=spark,
                run_id=run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                pipeline_name="PEI Pipeline",
                pipeline_stage="Schema Validation",
                source_name=source_name,
                source_file_id=source_file_id,
                source_file_count=source_file_count,
                source_file_name=source_file_name,
                archived_file_name="",
                source_table=bronze_table,
                target_table=validated_table,
                status=status,
                records_read=rows_read,
                records_written=rows_written,
                rejected_records=rejected_records,
                start_time=table_start_time,
                end_time=table_end_time,
                error_message=error_message
            )

            print(
                "Pipeline Audit Written.",
                flush=True
            )

    # =============================================================
    # Pipeline Summary
    # =============================================================

    if failed_tables == 0:
        final_status = "SUCCESS"

    elif processed_tables == 0:
        final_status = "FAILED"

    else:
        final_status = "PARTIAL_SUCCESS"

    result = {
        "stage": "schema_validation",
        "run_id": run_id,
        "status": final_status,
        "processed_tables": processed_tables,
        "failed_tables": failed_tables,
        "skipped_tables": skipped_tables,
        "rows_read": total_rows_read,
        "rows_written": total_rows_written,
        "start_time": start_time,
        "end_time": datetime.now()
    }

    print("\n" + "=" * 70, flush=True)

    print(
        "PEI SCHEMA VALIDATION COMPLETED",
        flush=True
    )

    print(
        f"Status            : {final_status}",
        flush=True
    )

    print(
        f"Processed Tables  : {processed_tables}",
        flush=True
    )

    print(
        f"Failed Tables     : {failed_tables}",
        flush=True
    )

    print(
        f"Rows Read         : {total_rows_read}",
        flush=True
    )

    print(
        f"Rows Written      : {total_rows_written}",
        flush=True
    )

    print(
        f"Skipped Tables   : {skipped_tables}",
        flush=True
    )

    print("=" * 70, flush=True)

    return result


