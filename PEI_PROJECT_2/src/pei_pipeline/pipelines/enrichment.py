"""
==========================================================
Enrichment Pipeline
==========================================================

Executes business enrichment transformations.
"""

from datetime import datetime
import traceback

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
    Execute business enrichment pipeline.
    """

    print()
    print("=" * 70)
    print("PEI ENRICHMENT PIPELINE")
    print("=" * 70)
    print(f"Pipeline Run ID : {run_id}")
    print("=" * 70)

    configs = get_ingestion_config(spark)

    processed_tables = 0
    failed_tables = 0

    total_rows_read = 0
    total_rows_written = 0

    for config in configs.collect():

        source_name = config["source_name"]

        source_table = config["dq_pass_table"]

        target_table = config["enrich_table"]

        print()
        print("-" * 70)
        print(f"Source          : {source_name}")
        print(f"Source Table    : {source_table}")
        print(f"Target Table    : {target_table}")
        print("-" * 70)

        status = "SUCCESS"
        error_message = ""

        rows_read = 0
        rows_written = 0

        try:

            # --------------------------------------------------
            # Step 1
            # --------------------------------------------------

            print("Step 1 : Reading Source Table...")
            print(f"Reading Table : {source_table}")

            dq_passed_df = read_table(
                spark=spark,
                table_name=source_table
            )

            print("Table read completed.")

            rows_read = dq_passed_df.count()

            total_rows_read += rows_read

            print(f"Rows Read : {rows_read}")

            print("Input Schema")

            dq_passed_df.printSchema()

            # --------------------------------------------------
            # Step 2
            # --------------------------------------------------

            print()
            print("Step 2 : Executing Business Enrichment...")

            enriched_df = enrich_dataframe(
                spark=spark,
                df=dq_passed_df,
                source_name=source_name
            )

            rows_written = enriched_df.count()

            print("Business Enrichment Completed.")

            print(f"Rows After Enrichment : {rows_written}")

            print("Enriched Schema")

            enriched_df.printSchema()

            # --------------------------------------------------
            # Step 3
            # --------------------------------------------------

            if target_table:

                print()
                print("Step 3 : Writing Enriched Table...")

                write_table(
                    df=enriched_df,
                    table_name=target_table,
                    mode="overwrite"
                )

                print("Enriched Table Written Successfully.")

                total_rows_written += rows_written

            else:

                target_table = source_table

                print("Step 3 : No Enrichment Target Configured.")

            processed_tables += 1

            print()
            print(
                f"Enrichment Completed Successfully : {source_name}"
            )

        except Exception as ex:

            status = "FAILED"

            failed_tables += 1

            error_message = str(ex)

            print()

            print(
                f"Enrichment Failed : {source_name}"
            )

            print()

            traceback.print_exc()

        finally:

            print()

            print("Writing Pipeline Audit...")

            log_pipeline_run(
                spark=spark,
                run_id=run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                pipeline_name="PEI Pipeline",
                pipeline_stage="Enrichment",
                source_name=source_name,
                source_file_name="",
                archived_file_name="",
                source_table=source_table,
                target_table=target_table,
                status=status,
                records_read=rows_read,
                records_written=rows_written,
                rejected_records=0,
                start_time=start_time,
                end_time=datetime.now(),
                error_message=error_message
            )

            print("Pipeline Audit Written.")

    pipeline_status = (
        "SUCCESS"
        if failed_tables == 0
        else (
            "FAILED"
            if processed_tables == 0
            else "PARTIAL_SUCCESS"
        )
    )

    print()
    print("=" * 70)
    print("PEI ENRICHMENT PIPELINE COMPLETED")
    print("=" * 70)
    print(f"Status            : {pipeline_status}")
    print(f"Processed Tables  : {processed_tables}")
    print(f"Failed Tables     : {failed_tables}")
    print(f"Rows Read         : {total_rows_read}")
    print(f"Rows Written      : {total_rows_written}")
    print("=" * 70)

    return {

        "stage": "enrichment",

        "run_id": run_id,

        "status": pipeline_status,

        "processed_tables": processed_tables,

        "failed_tables": failed_tables,

        "rows_read": total_rows_read,

        "rows_written": total_rows_written

    }