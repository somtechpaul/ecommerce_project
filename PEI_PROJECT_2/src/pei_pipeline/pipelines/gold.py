"""
==========================================================
Gold Pipeline
==========================================================

Creates Gold business tables from the complete curated
Silver current state.

Gold write strategy
-------------------
Gold tables are fully rebuilt and overwritten because:

- Sales Master must reflect customer and product changes.
- Aggregates contain SUM, AVG and COUNT DISTINCT results.
- Corrected or deleted orders must be removed.
- Retrying the same run must produce the same final result.

This is idempotent full recomputation, not an incremental
append.
"""

from datetime import datetime
import traceback

from pyspark.sql.functions import (
    col
)

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
    attempt_id,
    attempt_number,
    start_time
):
    """
    Execute the Gold pipeline.
    """

    print()
    print("=" * 70)
    print("PEI GOLD PIPELINE")
    print("=" * 70)
    print(f"Pipeline Run ID : {run_id}")
    print(f"Attempt ID      : {attempt_id}")
    print(f"Attempt Number  : {attempt_number}")
    print(f"Start Time      : {start_time}")
    print("=" * 70)

    # ==========================================================
    # Execution counters and audit defaults
    # ==========================================================

    processed_tables = 0
    failed_tables = 0

    total_rows_written = 0
    source_records_read = 0

    source_file_id = ""
    source_file_name = ""
    source_file_count = 0

    source_table_description = ""
    current_target_table = "MULTIPLE"
    current_table_start_time = start_time

    error_message = ""

    try:

        # ======================================================
        # Step 1: Read and validate metadata
        # ======================================================

        print()
        print("Step 1 : Reading Metadata...")

        configs = get_ingestion_config(
            spark
        )

        required_config_columns = {
            "source_name",
            "dq_pass_table",
            "enrich_table"
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

        config_map = {
            row["source_name"]: row.asDict()
            for row in config_rows
        }

        required_sources = {
            "customer",
            "products",
            "orders"
        }

        missing_sources = sorted(
            required_sources
            - set(config_map)
        )

        if missing_sources:

            raise ValueError(
                "Gold pipeline is missing ingestion "
                f"configuration for sources: {missing_sources}"
            )

        customer_source_table = (
            config_map["customer"]["enrich_table"]
            or ""
        ).strip()

        product_source_table = (
            config_map["products"]["enrich_table"]
            or ""
        ).strip()

        orders_source_table = (
            config_map["orders"]["dq_pass_table"]
            or ""
        ).strip()

        if not customer_source_table:

            raise ValueError(
                "Customer enrichment table is not configured."
            )

        if not product_source_table:

            raise ValueError(
                "Products enrichment table is not configured."
            )

        if not orders_source_table:

            raise ValueError(
                "Orders DQ-passed table is not configured."
            )

        source_table_description = ",".join([
            customer_source_table,
            product_source_table,
            orders_source_table
        ])

        print("Metadata Loaded.")
        print(
            f"Customer Source : {customer_source_table}"
        )
        print(
            f"Product Source  : {product_source_table}"
        )
        print(
            f"Orders Source   : {orders_source_table}"
        )

        # ======================================================
        # Step 2: Build complete Gold DataFrames
        # ======================================================

        print()
        print("Step 2 : Building Gold DataFrames...")

        gold_tables = build_gold_tables(
            spark=spark,
            configs=configs
        )

        if not gold_tables:

            raise ValueError(
                "build_gold_tables() returned no Gold tables."
            )

        print(
            f"Gold Tables Built : {len(gold_tables)}"
        )

        # ======================================================
        # Resolve Sales Master and source lineage
        # ======================================================

        sales_master_table = next(
            (
                table_name
                for table_name in gold_tables
                if table_name.endswith(".sales_master")
            ),
            None
        )

        if sales_master_table is None:

            raise ValueError(
                "Gold transformation did not return "
                "the sales_master table."
            )

        sales_master_df = gold_tables[
            sales_master_table
        ]

        required_lineage_columns = {
            "source_file_id",
            "source_file_name"
        }

        missing_lineage_columns = sorted(
            required_lineage_columns
            - set(sales_master_df.columns)
        )

        if missing_lineage_columns:

            raise ValueError(
                "Gold sales_master is missing required "
                f"lineage columns: {missing_lineage_columns}"
            )

        source_files = (
            sales_master_df
            .filter(
                col("source_file_id").isNotNull()
            )
            .select(
                "source_file_id",
                "source_file_name"
            )
            .distinct()
            .collect()
        )

        source_file_count = len(
            source_files
        )

        if source_file_count == 0:

            raise ValueError(
                "No source-file lineage was found in "
                "the Gold sales_master DataFrame."
            )

        if source_file_count == 1:

            source_file_id = (
                source_files[0]["source_file_id"]
            )

            source_file_name = (
                source_files[0]["source_file_name"]
                or ""
            )

        else:

            source_file_id = "MULTIPLE"
            source_file_name = "MULTIPLE"

        source_records_read = (
            sales_master_df.count()
        )

        print(
            f"Sales Master Source Rows : "
            f"{source_records_read}"
        )

        print(
            f"Contributing Source Files : "
            f"{source_file_count}"
        )

        # ======================================================
        # Step 3: Write complete Gold tables
        # ======================================================

        print()
        print("Step 3 : Writing Gold Tables...")

        for table_name, gold_df in gold_tables.items():

            current_target_table = table_name
            current_table_start_time = datetime.now()

            print()
            print("-" * 70)
            print(
                f"Target Table : {table_name}"
            )

            rows_to_write = gold_df.count()

            print(
                f"Rows to Write : {rows_to_write}"
            )

            print("Schema:")

            gold_df.printSchema()

            # Gold is rebuilt from the complete current-state
            # Silver inputs. Full overwrite removes stale rows
            # and makes retries idempotent.
            write_table(
                df=gold_df,
                table_name=table_name,
                mode="overwrite"
            )

            rows_written = rows_to_write

            total_rows_written += rows_written
            processed_tables += 1

            print(
                f"Gold Table '{table_name}' "
                "Written Successfully."
            )

            # --------------------------------------------------
            # Successful table audit
            # --------------------------------------------------

            print(
                "Writing Pipeline Audit..."
            )

            log_pipeline_run(
                spark=spark,
                run_id=run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                pipeline_name="PEI Pipeline",
                pipeline_stage="Gold",
                source_name="MULTIPLE",
                source_file_id=source_file_id,
                source_file_count=source_file_count,
                source_file_name=source_file_name,
                archived_file_name="",
                source_table=source_table_description,
                target_table=table_name,
                status="SUCCESS",
                records_read=source_records_read,
                records_written=rows_written,
                rejected_records=0,
                start_time=current_table_start_time,
                end_time=datetime.now(),
                error_message=""
            )

            print(
                "Pipeline Audit Written."
            )

        pipeline_status = "SUCCESS"

    except Exception:

        failed_tables += 1

        pipeline_status = (
            "PARTIAL_SUCCESS"
            if processed_tables > 0
            else "FAILED"
        )

        error_message = traceback.format_exc()

        print()
        print("Gold Pipeline Failed")
        print(
            f"Error Message : {error_message}"
        )

        print()
        print("Full Traceback")
        print("-" * 70)

        traceback.print_exc()

        # ======================================================
        # Failure audit
        # ======================================================

        print()
        print("Writing Failure Audit...")

        try:

            log_pipeline_run(
                spark=spark,
                run_id=run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                pipeline_name="PEI Pipeline",
                pipeline_stage="Gold",
                source_name="MULTIPLE",
                source_file_id=source_file_id,
                source_file_count=source_file_count,
                source_file_name=source_file_name,
                archived_file_name="",
                source_table=source_table_description,
                target_table=current_target_table,
                status="FAILED",
                records_read=source_records_read,
                records_written=0,
                rejected_records=0,
                start_time=current_table_start_time,
                end_time=datetime.now(),
                error_message=error_message
            )

            print(
                "Failure Audit Written."
            )

        except Exception as audit_exception:

            print(
                "Gold failure audit logging failed."
            )

            print(
                f"Audit Error : {audit_exception}"
            )

            traceback.print_exc()

            raise RuntimeError(
                "Gold pipeline and its failure audit "
                "both failed."
            ) from audit_exception

    # ==========================================================
    # Final summary
    # ==========================================================

    print()
    print("=" * 70)
    print("PEI GOLD PIPELINE COMPLETED")
    print("=" * 70)
    print(f"Status            : {pipeline_status}")
    print(f"Processed Tables  : {processed_tables}")
    print(f"Failed Tables     : {failed_tables}")
    print(f"Source Rows       : {source_records_read}")
    print(f"Rows Written      : {total_rows_written}")
    print("=" * 70)

    return {
        "stage": "gold",
        "run_id": run_id,
        "status": pipeline_status,
        "processed_tables": processed_tables,
        "failed_tables": failed_tables,
        "source_rows": source_records_read,
        "rows_written": total_rows_written
    }