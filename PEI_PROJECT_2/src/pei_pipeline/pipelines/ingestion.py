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

from pei_pipeline.metadata.repository import get_ingestion_config
from pei_pipeline.io.filesystem import get_matching_files, move_file
from pei_pipeline.io.file_readers import read_file
from pei_pipeline.io.table_writer import write_table
from pei_pipeline.schema.mapping import standardize_column_names
from pei_pipeline.transformations.enrichment import add_metadata_columns, cast_business_columns_to_string
from pei_pipeline.audit.repository import log_pipeline_run


def run_ingestion_pipeline(
    spark,
    dbutils,
    run_id,
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

            print("\nProcessing file:", flush=True)
            print(f"  Name : {file.name}", flush=True)
            print(f"  Path : {file.path}", flush=True)

            try:

                print("  Step 1: Reading source file...", flush=True)

                df = read_file(
                    spark=spark,
                    file_path=file.path,
                    file_type=config["file_type"]
                )

                print("  Step 2: Counting source records...", flush=True)

                rows_read = df.count()

                print(f"  Rows read: {rows_read}", flush=True)

                print(
                    f"  Original Source columns: {df.columns}",
                    flush=True
                )

                print(
                    "  Step 3: Standardizing column names...",
                    flush=True
                )

                df = standardize_column_names(df)

                print(
                    f"  Standardized columns: {df.columns}",
                    flush=True
                )

                print(
                    "  Step 4: Adding ingestion metadata...",
                    flush=True
                )

                df = add_metadata_columns(
                    df=df,
                    run_id=run_id,
                    source_file=file.name,
                    source_system=source_name
                )

                df = cast_business_columns_to_string(df)

                print("========== DataFrame Schema after ingestion metadata ==========")
                df.printSchema()

                print(
                    f"  Step 5: Writing to {bronze_table}...",
                    flush=True
                )

                write_table(
                    df=df,
                    table_name=bronze_table,
                    mode="append",
                    partition_by=["ingestion_date"]
                )

                rows_written = rows_read

                print(
                    f"  Rows written: {rows_written}",
                    flush=True
                )

                file_name = Path(file.name).stem
                extension = Path(file.name).suffix
                run_id_short = run_id[:8]
                timestamp = start_time.strftime("%Y%m%d_%H%M%S")
                archive_file_name = f"{file_name}_{timestamp}_{run_id_short}{extension}"


                destination_path = (
                    f"{archive_path.rstrip('/')}/{archive_file_name}"
                )

                print(
                    f"  Step 6: Archiving to {destination_path}...",
                    flush=True
                )

                move_file(
                            dbutils=dbutils,
                            source_path=file.path,
                            destination_path=destination_path
                        )

                processed_files += 1
                total_rows_read += rows_read
                total_rows_written += rows_written

                print(
                    f"  File completed successfully: {file.name}",
                    flush=True
                )

            except Exception as ex:

                status = "FAILED"
                error_message = str(ex)
                failed_files += 1

                print(
                    f"  File failed: {file.name}",
                    flush=True
                )
                print(
                    f"  Error: {error_message}",
                    flush=True
                )

            finally:

                end_time = datetime.now()

                print("  Writing audit record...", flush=True)

                log_pipeline_run(
                    spark=spark,
                    run_id=run_id,
                    pipeline_name="PEI Pipeline",
                    pipeline_stage="Bronze",
                    source_name=source_name,
                    source_file_name=file.name,
                    archived_file_name=archive_file_name,
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

                print("  Audit record written.", flush=True)

    final_status = (
        "SUCCESS"
        if failed_files == 0
        else "PARTIAL_SUCCESS"
    )

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