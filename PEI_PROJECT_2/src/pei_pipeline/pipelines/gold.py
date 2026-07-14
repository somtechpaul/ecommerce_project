"""
==========================================================
Gold Pipeline
==========================================================

Creates Gold business tables from curated Silver data.
"""

from datetime import datetime
import traceback

from pei_pipeline.metadata.repository import (
    get_ingestion_config
)

from pei_pipeline.transformations.gold import (
    build_gold_tables
)

from pei_pipeline.io.table_writer import (
    write_table
)

from pei_pipeline.audit.repository import (
    log_pipeline_run
)


def run_gold_pipeline(
    spark,
    run_id,
    start_time
):
    """
    Execute Gold pipeline.
    """

    print()
    print("=" * 70)
    print("PEI GOLD PIPELINE")
    print("=" * 70)
    print(f"Pipeline Run ID : {run_id}")
    print("=" * 70)

    status = "SUCCESS"

    processed_tables = 0
    failed_tables = 0

    total_rows_written = 0

    try:

        # --------------------------------------------------
        # Step 1
        # --------------------------------------------------

        print("Step 1 : Reading Metadata...")

        configs = get_ingestion_config(
            spark
        )

        print("Metadata Loaded.")

        # --------------------------------------------------
        # Step 2
        # --------------------------------------------------

        print()
        print("Step 2 : Building Gold DataFrames...")

        gold_tables = build_gold_tables(

            spark=spark,

            configs=configs

        )

        print(
            f"Gold Tables Built : {len(gold_tables)}"
        )

        # --------------------------------------------------
        # Step 3
        # --------------------------------------------------

        print()
        print("Step 3 : Writing Gold Tables...")

        for table_name, df in gold_tables.items():

            print()
            print("-" * 70)

            print(f"Target Table : {table_name}")

            rows_written = df.count()

            print(f"Rows Written : {rows_written}")

            print("Schema")

            df.printSchema()

            write_table(

                df=df,

                table_name=table_name,

                mode="overwrite"

            )

            print(
                f"Gold Table : {table_name} : Written Successfully."
            )

            total_rows_written += rows_written

            processed_tables += 1

            print("Writing Pipeline Audit...")

            log_pipeline_run(

                spark=spark,

                run_id=run_id,

                pipeline_name="PEI Pipeline",

                pipeline_stage="Gold",

                source_name="Multiple",

                source_file_name="",

                archived_file_name="",

                source_table="Curated",

                target_table=table_name,

                status="SUCCESS",

                records_read=0,

                records_written=rows_written,

                rejected_records=0,

                start_time=start_time,

                end_time=datetime.now(),

                error_message=""

            )

            print("Pipeline Audit Written.")

        pipeline_status = "SUCCESS"

    except Exception as ex:

        pipeline_status = "FAILED"

        failed_tables += 1

        print()

        print("Gold Pipeline Failed")

        print()

        traceback.print_exc()

        print()

        error_message = str(ex)

        print("Writing Failure Audit...")

        log_pipeline_run(

            spark=spark,

            run_id=run_id,

            pipeline_name="PEI Pipeline",

            pipeline_stage="Gold",

            source_name="Multiple",

            source_file_name="",

            archived_file_name="",

            source_table="Curated",

            target_table="Gold",

            status="FAILED",

            records_read=0,

            records_written=0,

            rejected_records=0,

            start_time=start_time,

            end_time=datetime.now(),

            error_message=error_message

        )

        print("Failure Audit Written.")

    # --------------------------------------------------
    # Final Summary
    # --------------------------------------------------

    print()
    print("=" * 70)
    print("PEI GOLD PIPELINE COMPLETED")
    print("=" * 70)
    print(f"Status            : {pipeline_status}")
    print(f"Processed Tables  : {processed_tables}")
    print(f"Failed Tables     : {failed_tables}")
    print(f"Rows Written      : {total_rows_written}")
    print("=" * 70)

    return {

        "stage": "gold",

        "run_id": run_id,

        "status": pipeline_status,

        "processed_tables": processed_tables,

        "failed_tables": failed_tables,

        "rows_written": total_rows_written

    }