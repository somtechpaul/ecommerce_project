"""
==========================================================
Bronze Ingestion Pipeline
==========================================================

Orchestrates metadata-driven ingestion into Bronze.

Workflow
--------
1. Read ingestion metadata
2. Discover files
3. Read files
4. Standardize column names
5. Add metadata columns
6. Write Bronze table
7. Archive processed files
8. Write audit logs
"""

"""
==========================================================
Bronze Ingestion Pipeline
==========================================================
"""

from datetime import datetime
from pathlib import Path

from pei_pipeline.metadata.repository import (
    get_ingestion_config,
    get_processed_file_status,
    register_file_processing,
    mark_bronze_committed,
    mark_bronze_failed,
    mark_archive_succeeded,
    mark_archive_failed
)
from pei_pipeline.io.filesystem import (
    get_matching_files,
    move_file,
    generate_source_file_id
)
from pei_pipeline.io.file_readers import read_file
from pei_pipeline.io.table_writer import write_table
from pei_pipeline.schema.mapping import standardize_column_names
from pei_pipeline.transformations.enrichment import add_metadata_columns, cast_business_columns_to_string
from pei_pipeline.audit.repository import log_pipeline_run


def run_ingestion_pipeline(
    spark,
    dbutils,
    run_id,
    attempt_id,
    attempt_number,
    start_time
):
    """
    Execute Bronze ingestion pipeline.
    """

    print("=" * 70, flush=True)
    print("PEI BRONZE INGESTION STARTED", flush=True)
    print(f"Run ID     : {run_id}", flush=True)
    print(f"Start Time : {start_time}", flush=True)
    print("=" * 70, flush=True)

    configs = get_ingestion_config(spark)

    print("Reading active ingestion configurations...", flush=True)

    config_rows = configs.collect()

    print(
        f"Active source configurations found: {len(config_rows)}",
        flush=True
    )

    processed_files = 0
    failed_files = 0
    total_rows_read = 0
    total_rows_written = 0

    for config in config_rows:

        source_name = config["source_name"]
        landing_path = config["landing_path"]
        file_pattern = config["file_pattern"]
        bronze_table = config["bronze_table"]
        archive_path = config["archive_path"]

        print("\n" + "-" * 70, flush=True)
        print(f"Source       : {source_name}", flush=True)
        print(f"Landing path : {landing_path}", flush=True)
        print(f"File pattern : {file_pattern}", flush=True)
        print(f"Bronze table : {bronze_table}", flush=True)
        print("-" * 70, flush=True)

        print("Searching for matching files...", flush=True)

        files = get_matching_files(
                    dbutils=dbutils,
                    landing_path=landing_path,
                    file_pattern=file_pattern
                )

        print(f"Matching files found: {len(files)}", flush=True)

        if not files:
            print(
                f"No files found for source: {source_name}",
                flush=True
            )
            continue

        for file in files:
            file_start_time = datetime.now()

            status = "SUCCESS"
            error_message = ""

            rows_read = 0
            rows_written = 0

            source_file_id = None
            archive_file_name = ""
            destination_path = ""

            retry_archive_only = False
            current_step = "INITIALISING"

            file_size = getattr(
                file,
                "size",
                0
            )

            file_modification_time = getattr(
                file,
                "modificationTime",
                0
            )

            try:

                # =================================================
                # Step 1: Generate physical file-version identity
                # =================================================

                current_step = "GENERATE_SOURCE_FILE_ID"

                source_file_id = generate_source_file_id(
                    source_name=source_name,
                    file_path=file.path,
                    file_size=file_size,
                    modification_time=file_modification_time
                )

                # Use a deterministic archive filename.
                #
                # A retry of the same physical file version must
                # calculate the same archive destination.
                file_name_without_extension = Path(
                    file.name
                ).stem

                file_extension = Path(
                    file.name
                ).suffix

                archive_file_name = (
                    f"{file_name_without_extension}_"
                    f"{source_file_id[:16]}"
                    f"{file_extension}"
                )

                destination_path = (
                    f"{archive_path.rstrip('/')}/"
                    f"{archive_file_name}"
                )

                print("\nProcessing file:", flush=True)
                print(
                    f"  File Name       : {file.name}",
                    flush=True
                )
                print(
                    f"  File Path       : {file.path}",
                    flush=True
                )
                print(
                    f"  Source File ID  : {source_file_id}",
                    flush=True
                )
                print(
                    f"  File Size       : {file_size}",
                    flush=True
                )
                print(
                    f"  Modified Time   : "
                    f"{file_modification_time}",
                    flush=True
                )

                # =================================================
                # Step 2: Read existing processing state
                # =================================================

                current_step = "GET_PROCESSED_FILE_STATUS"

                processed_state = get_processed_file_status(
                    spark=spark,
                    source_file_id=source_file_id
                )

                # -------------------------------------------------
                # Already fully processed
                # -------------------------------------------------

                if (
                    processed_state
                    and processed_state["archive_status"]
                    == "ARCHIVED"
                ):

                    status = "SKIPPED"

                    print(
                        "  File already committed and archived. "
                        "Skipping Bronze and archive operations.",
                        flush=True
                    )

                    continue

                # -------------------------------------------------
                # Bronze committed but archive incomplete
                # -------------------------------------------------

                retry_archive_only = bool(
                    processed_state
                    and processed_state["bronze_status"]
                    == "COMMITTED"
                    and processed_state["archive_status"]
                    in {
                        "PENDING",
                        "FAILED"
                    }
                )

                if retry_archive_only:

                    print(
                        "  Bronze is already committed. "
                        "Retrying archive operation only.",
                        flush=True
                    )

                else:

                    # =============================================
                    # Step 3: Register/claim file for processing
                    # =============================================

                    current_step = "REGISTER_FILE_PROCESSING"

                    register_file_processing(
                        spark=spark,
                        source_file_id=source_file_id,
                        source_name=source_name,
                        file_name=file.name,
                        file_path=file.path,
                        file_size=file_size,
                        modification_time=(
                            file_modification_time
                        ),
                        bronze_table=bronze_table,

                        # Store the planned full destination,
                        # not only the archive directory.
                        archive_path=destination_path,

                        pipeline_run_id=run_id,
                        attempt_id=attempt_id
                    )

                    # =============================================
                    # Step 4: Read source file
                    # =============================================

                    current_step = "READ_SOURCE_FILE"

                    print(
                        "  Step 1: Reading source file...",
                        flush=True
                    )

                    df = read_file(
                        spark=spark,
                        file_path=file.path,
                        file_type=config["file_type"]
                    )

                    rows_read = df.count()

                    print(
                        f"  Rows read: {rows_read}",
                        flush=True
                    )

                    # =============================================
                    # Step 5: Standardize source columns
                    # =============================================

                    current_step = "STANDARDISE_COLUMNS"

                    df = standardize_column_names(
                        df
                    )

                    # =============================================
                    # Step 6: Add Bronze metadata
                    # =============================================

                    current_step = "ADD_BRONZE_METADATA"

                    df = add_metadata_columns(
                        df=df,
                        run_id=run_id,
                        source_file_id=source_file_id,
                        source_file=file.name,
                        source_system=source_name
                    )

                    df = cast_business_columns_to_string(
                        df
                    )

                    print(
                        "  Bronze DataFrame schema:",
                        flush=True
                    )

                    df.printSchema()

                    # =============================================
                    # Step 7: Idempotent Bronze write
                    # =============================================

                    current_step = "WRITE_BRONZE"

                    print(
                        f"  Writing to {bronze_table}...",
                        flush=True
                    )

                    write_table(
                        df=df,
                        table_name=bronze_table,

                        # This is a selective overwrite.
                        # It does not overwrite the full table.
                        mode="overwrite",

                        replace_where=(
                            f"source_file_id = "
                            f"'{source_file_id}'"
                        ),

                        partition_by=[
                            "ingestion_date"
                        ]
                    )

                    rows_written = rows_read

                    # =============================================
                    # Step 8: Record successful Bronze commit
                    # =============================================

                    current_step = "MARK_BRONZE_COMMITTED"

                    mark_bronze_committed(
                        spark=spark,
                        source_file_id=source_file_id,
                        pipeline_run_id=run_id,
                        attempt_id=attempt_id
                    )

                    print(
                        "  Bronze commit recorded.",
                        flush=True
                    )

                # =================================================
                # Step 9: Archive
                # =================================================

                current_step = "MOVE_TO_ARCHIVE"

                print(
                    f"  Archiving to {destination_path}...",
                    flush=True
                )

                move_file(
                    dbutils=dbutils,
                    source_path=file.path,
                    destination_path=destination_path
                )

                # =================================================
                # Step 10: Record archive success
                # =================================================

                current_step = "MARK_ARCHIVE_SUCCEEDED"

                mark_archive_succeeded(
                    spark=spark,
                    source_file_id=source_file_id,
                    archived_file_path=destination_path,
                    pipeline_run_id=run_id,
                    attempt_id=attempt_id
                )

                processed_files += 1
                total_rows_read += rows_read
                total_rows_written += rows_written

                print(
                    f"  File completed successfully: "
                    f"{file.name}",
                    flush=True
                )

            except Exception as ex:

                status = "FAILED"

                error_message = (
                    f"Step '{current_step}' failed: {ex}"
                )

                failed_files += 1

                print(
                    f"  File failed: {file.name}",
                    flush=True
                )

                print(
                    f"  Error: {error_message}",
                    flush=True
                )

                # =============================================
                # Persist the correct failure state
                # =============================================

                if source_file_id is not None:

                    try:

                        current_state = (
                            get_processed_file_status(
                                spark=spark,
                                source_file_id=(
                                    source_file_id
                                )
                            )
                        )

                        if current_state is not None:

                            bronze_is_committed = (
                                current_state[
                                    "bronze_status"
                                ]
                                == "COMMITTED"
                            )

                            archive_step_failed = (
                                current_step
                                in {
                                    "MOVE_TO_ARCHIVE",
                                    "MARK_ARCHIVE_SUCCEEDED"
                                }
                            )

                            if (
                                bronze_is_committed
                                or retry_archive_only
                                or archive_step_failed
                            ):

                                mark_archive_failed(
                                    spark=spark,
                                    source_file_id=(
                                        source_file_id
                                    ),
                                    pipeline_run_id=run_id,
                                    attempt_id=attempt_id,
                                    error_message=(
                                        error_message
                                    )
                                )

                                print(
                                    "  Archive failure "
                                    "recorded.",
                                    flush=True
                                )

                            else:

                                mark_bronze_failed(
                                    spark=spark,
                                    source_file_id=(
                                        source_file_id
                                    ),
                                    pipeline_run_id=run_id,
                                    attempt_id=attempt_id,
                                    error_message=(
                                        error_message
                                    )
                                )

                                print(
                                    "  Bronze failure "
                                    "recorded.",
                                    flush=True
                                )

                    except Exception as state_exception:

                        print(
                            "  Failed to update "
                            "processed-file state: "
                            f"{state_exception}",
                            flush=True
                        )

            finally:

                end_time = datetime.now()

                print(
                    "  Writing audit record...",
                    flush=True
                )

                try:

                    log_pipeline_run(
                        spark=spark,
                        run_id=run_id,
                        attempt_id=attempt_id,
                        attempt_number=attempt_number,
                        pipeline_name="PEI Pipeline",
                        pipeline_stage="Bronze",
                        source_name=source_name,
                        source_file_name=file.name,
                        archived_file_name=(
                            archive_file_name
                        ),
                        source_table=landing_path,
                        target_table=bronze_table,
                        status=status,
                        records_read=rows_read,
                        records_written=rows_written,
                        rejected_records=0,
                        start_time=file_start_time,
                        end_time=end_time,
                        error_message=error_message
                    )

                    print(
                        "  Audit record written.",
                        flush=True
                    )

                except Exception as audit_exception:

                    print(
                        "  Audit logging failed: "
                        f"{audit_exception}",
                        flush=True
                    )

    if failed_files == 0:
        final_status = "SUCCESS"

    elif processed_files == 0:
        final_status = "FAILED"

    else:
        final_status = "PARTIAL_SUCCESS"

    result = {
        "stage": "ingestion",
        "run_id": run_id,
        "status": final_status,
        "processed_files": processed_files,
        "failed_files": failed_files,
        "rows_read": total_rows_read,
        "rows_written": total_rows_written,
        "start_time": start_time,
        "end_time": datetime.now()
    }

    print("\n" + "=" * 70, flush=True)
    print("PEI BRONZE INGESTION COMPLETED", flush=True)
    print(f"Status          : {final_status}", flush=True)
    print(f"Processed files : {processed_files}", flush=True)
    print(f"Failed files    : {failed_files}", flush=True)
    print(f"Rows read       : {total_rows_read}", flush=True)
    print(f"Rows written    : {total_rows_written}", flush=True)
    print("=" * 70, flush=True)

    return result