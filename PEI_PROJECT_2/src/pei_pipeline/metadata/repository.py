"""
==========================================================
Metadata Repository
==========================================================

Provides reusable functions for reading metadata tables.

Responsibilities
----------------
- Read ingestion configuration
- Read expected table schema
- Read column mappings
- Read data quality rules

No business logic.
"""

from pyspark.sql import DataFrame

from delta.tables import DeltaTable

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit
)

from pei_pipeline.config.settings import (
    INGESTION_CONFIG_TABLE,
    REQUIRED_SCHEMA_TABLE,
    DATA_QUALITY_RULE_TABLE,
    PROCESSED_FILES_TABLE
)


def get_ingestion_config(spark) -> DataFrame:
    """
    Return all active ingestion configurations.
    """

    return (

        spark.table(INGESTION_CONFIG_TABLE)

        .filter("active_flag = true")

    )


def get_table_schema(
    spark,
    table_name: str
) -> DataFrame:
    """
    Return expected schema for a table.
    """

    return (

        spark.table(REQUIRED_SCHEMA_TABLE)

        .filter(f"table_name = '{table_name}'")

        .orderBy("column_order")

    )


def get_column_mapping(
    spark,
    table_name: str
) -> DataFrame:
    """
    Return source-to-target column mappings.
    """

    return (

        spark.table(REQUIRED_SCHEMA_TABLE)

        .filter(f"table_name = '{table_name}'")

    )


def get_data_quality_rules(
    spark,
    table_name: str
) -> DataFrame:
    """
    Return active DQ rules for a table.
    """

    return (

        spark.table(DATA_QUALITY_RULE_TABLE)

        .filter(f"table_name = '{table_name}'")

        .filter("active_flag = true")

    )


# ==========================================================
# Processed File Repository
# ==========================================================

def get_processed_file_status(
    spark,
    source_file_id: str
):
    """
    Return the processing state for one physical source-file
    version.

    Returns
    -------
    dict | None

    None is returned when the source_file_id has never been
    registered.
    """

    _validate_required_text(
        value=source_file_id,
        field_name="source_file_id"
    )

    rows = (
        spark.table(PROCESSED_FILES_TABLE)
        .filter(
            col("source_file_id") == lit(source_file_id)
        )
        .limit(2)
        .collect()
    )

    if not rows:
        return None

    if len(rows) > 1:

        raise RuntimeError(
            "metadata.processed_files contains multiple "
            f"records for source_file_id '{source_file_id}'."
        )

    return rows[0].asDict(
        recursive=True
    )


# ==========================================================
# Register File Processing
# ==========================================================

def register_file_processing(
    spark,
    source_file_id: str,
    source_name: str,
    file_name: str,
    file_path: str,
    file_size: int,
    modification_time: int,
    bronze_table: str,
    archive_path: str,
    pipeline_run_id: str,
    attempt_id: str
):
    """
    Register a physical source-file version before processing.

    New file
    --------
    Inserts a new record with:

        bronze_status  = PROCESSING
        archive_status = PENDING

    Existing file
    -------------
    Updates retry information only when Bronze has not already
    committed and the file has not already been archived.

    A COMMITTED or ARCHIVED file is never reset to PROCESSING.
    """

    required_values = {
        "source_file_id": source_file_id,
        "source_name": source_name,
        "file_name": file_name,
        "file_path": file_path,
        "bronze_table": bronze_table,
        "archive_path": archive_path,
        "pipeline_run_id": pipeline_run_id,
        "attempt_id": attempt_id
    }

    for field_name, value in required_values.items():

        _validate_required_text(
            value=value,
            field_name=field_name
        )

    source_df = (
        spark.range(1)
        .select(
            lit(source_file_id)
            .cast("string")
            .alias("source_file_id"),

            lit(source_name)
            .cast("string")
            .alias("source_name"),

            lit(file_name)
            .cast("string")
            .alias("file_name"),

            lit(file_path)
            .cast("string")
            .alias("file_path"),

            lit(file_size)
            .cast("long")
            .alias("file_size"),

            lit(modification_time)
            .cast("long")
            .alias("modification_time"),

            lit(bronze_table)
            .cast("string")
            .alias("bronze_table"),

            lit(archive_path)
            .cast("string")
            .alias("archive_path"),

            lit(pipeline_run_id)
            .cast("string")
            .alias("pipeline_run_id"),

            lit(attempt_id)
            .cast("string")
            .alias("attempt_id"),

            current_timestamp()
            .alias("event_timestamp")
        )
    )

    processed_files_table = _get_processed_files_delta_table(
        spark
    )

    (
        processed_files_table.alias("target")

        .merge(
            source_df.alias("source"),
            """
            target.source_file_id = source.source_file_id
            """
        )

        # Retry an uncommitted file.
        #
        # Do not reset a committed or archived file.
        .whenMatchedUpdate(
            condition="""
                target.bronze_status <> 'COMMITTED'
                AND target.archive_status <> 'ARCHIVED'
            """,
            set={
                "source_name": "source.source_name",
                "file_name": "source.file_name",
                "file_path": "source.file_path",
                "file_size": "source.file_size",
                "modification_time": (
                    "source.modification_time"
                ),
                "bronze_table": "source.bronze_table",
                "archive_path": "source.archive_path",
                "pipeline_run_id": (
                    "source.pipeline_run_id"
                ),
                "attempt_id": "source.attempt_id",
                "bronze_status": "'PROCESSING'",
                "archive_status": "'PENDING'",
                "last_updated_timestamp": (
                    "source.event_timestamp"
                ),
                "error_message": "''"
            }
        )

        # First time this physical file version is seen.
        .whenNotMatchedInsert(
            values={
                "source_file_id": "source.source_file_id",
                "source_name": "source.source_name",
                "file_name": "source.file_name",
                "file_path": "source.file_path",
                "file_size": "source.file_size",
                "modification_time": (
                    "source.modification_time"
                ),
                "bronze_table": "source.bronze_table",
                "bronze_status": "'PROCESSING'",
                "archive_status": "'PENDING'",
                "pipeline_run_id": (
                    "source.pipeline_run_id"
                ),
                "attempt_id": "source.attempt_id",
                "archive_path": "source.archive_path",
                "first_seen_timestamp": (
                    "source.event_timestamp"
                ),
                "bronze_committed_timestamp": (
                    "CAST(NULL AS TIMESTAMP)"
                ),
                "archived_timestamp": (
                    "CAST(NULL AS TIMESTAMP)"
                ),
                "last_updated_timestamp": (
                    "source.event_timestamp"
                ),
                "error_message": "''"
            }
        )

        .execute()
    )

    return get_processed_file_status(
        spark=spark,
        source_file_id=source_file_id
    )


# ==========================================================
# Mark Bronze Committed
# ==========================================================

def mark_bronze_committed(
    spark,
    source_file_id: str,
    pipeline_run_id: str,
    attempt_id: str
):
    """
    Mark the Bronze write as successfully committed.

    This must be called immediately after the Bronze Delta
    write succeeds and before the archive move starts.
    """

    current_state = _require_registered_file(
        spark=spark,
        source_file_id=source_file_id
    )

    # Idempotent retry:
    # do not change the original committed timestamp.
    if current_state["bronze_status"] == "COMMITTED":

        return current_state

    processed_files_table = _get_processed_files_delta_table(
        spark
    )

    processed_files_table.update(
        condition=(
            col("source_file_id") == lit(source_file_id)
        ),
        set={
            "bronze_status": lit("COMMITTED"),
            "archive_status": lit("PENDING"),
            "pipeline_run_id": lit(pipeline_run_id),
            "attempt_id": lit(attempt_id),
            "bronze_committed_timestamp": (
                current_timestamp()
            ),
            "last_updated_timestamp": (
                current_timestamp()
            ),
            "error_message": lit("")
        }
    )

    return get_processed_file_status(
        spark=spark,
        source_file_id=source_file_id
    )


# ==========================================================
# Mark Bronze Failed
# ==========================================================

def mark_bronze_failed(
    spark,
    source_file_id: str,
    pipeline_run_id: str,
    attempt_id: str,
    error_message: str
):
    """
    Mark file reading or Bronze writing as failed.

    This additional function is necessary so that a Bronze
    failure does not remain permanently in PROCESSING state.
    """

    current_state = _require_registered_file(
        spark=spark,
        source_file_id=source_file_id
    )

    # Never downgrade an already committed Bronze write.
    if current_state["bronze_status"] == "COMMITTED":

        return current_state

    processed_files_table = _get_processed_files_delta_table(
        spark
    )

    processed_files_table.update(
        condition=(
            col("source_file_id") == lit(source_file_id)
        ),
        set={
            "bronze_status": lit("FAILED"),
            "pipeline_run_id": lit(pipeline_run_id),
            "attempt_id": lit(attempt_id),
            "last_updated_timestamp": (
                current_timestamp()
            ),
            "error_message": lit(
                _truncate_error_message(error_message)
            )
        }
    )

    return get_processed_file_status(
        spark=spark,
        source_file_id=source_file_id
    )


# ==========================================================
# Mark Archive Succeeded
# ==========================================================

def mark_archive_succeeded(
    spark,
    source_file_id: str,
    archived_file_path: str,
    pipeline_run_id: str,
    attempt_id: str
):
    """
    Mark the source file as successfully archived.

    Archive success is allowed only after Bronze has committed.
    """

    current_state = _require_registered_file(
        spark=spark,
        source_file_id=source_file_id
    )

    _validate_required_text(
        value=archived_file_path,
        field_name="archived_file_path"
    )

    if current_state["bronze_status"] != "COMMITTED":

        raise RuntimeError(
            "Cannot mark archive as successful before the "
            f"Bronze write commits. source_file_id="
            f"'{source_file_id}'."
        )

    # Idempotent retry.
    if current_state["archive_status"] == "ARCHIVED":

        return current_state

    processed_files_table = _get_processed_files_delta_table(
        spark
    )

    processed_files_table.update(
        condition=(
            col("source_file_id") == lit(source_file_id)
        ),
        set={
            "archive_status": lit("ARCHIVED"),
            "archive_path": lit(archived_file_path),
            "pipeline_run_id": lit(pipeline_run_id),
            "attempt_id": lit(attempt_id),
            "archived_timestamp": current_timestamp(),
            "last_updated_timestamp": (
                current_timestamp()
            ),
            "error_message": lit("")
        }
    )

    return get_processed_file_status(
        spark=spark,
        source_file_id=source_file_id
    )


# ==========================================================
# Mark Archive Failed
# ==========================================================

def mark_archive_failed(
    spark,
    source_file_id: str,
    pipeline_run_id: str,
    attempt_id: str,
    error_message: str
):
    """
    Mark the archive operation as failed.

    The Bronze status remains COMMITTED. This allows a retry
    to skip the Bronze write and retry only the archive move.
    """

    current_state = _require_registered_file(
        spark=spark,
        source_file_id=source_file_id
    )

    if current_state["bronze_status"] != "COMMITTED":

        raise RuntimeError(
            "Archive failure cannot be recorded before the "
            f"Bronze write commits. source_file_id="
            f"'{source_file_id}'."
        )

    # Never downgrade an already archived file.
    if current_state["archive_status"] == "ARCHIVED":

        return current_state

    processed_files_table = _get_processed_files_delta_table(
        spark
    )

    processed_files_table.update(
        condition=(
            col("source_file_id") == lit(source_file_id)
        ),
        set={
            # bronze_status is deliberately not changed.
            "archive_status": lit("FAILED"),
            "pipeline_run_id": lit(pipeline_run_id),
            "attempt_id": lit(attempt_id),
            "last_updated_timestamp": (
                current_timestamp()
            ),
            "error_message": lit(
                _truncate_error_message(error_message)
            )
        }
    )

    return get_processed_file_status(
        spark=spark,
        source_file_id=source_file_id
    )


# ==========================================================
# Internal Helpers
# ==========================================================

def _get_processed_files_delta_table(
    spark
):
    """
    Return metadata.processed_files as a DeltaTable.
    """

    if not spark.catalog.tableExists(
        PROCESSED_FILES_TABLE
    ):

        raise RuntimeError(
            f"Required table does not exist: "
            f"{PROCESSED_FILES_TABLE}"
        )

    return DeltaTable.forName(
        spark,
        PROCESSED_FILES_TABLE
    )


def _require_registered_file(
    spark,
    source_file_id: str
):
    """
    Return a registered file state or raise an error.
    """

    _validate_required_text(
        value=source_file_id,
        field_name="source_file_id"
    )

    current_state = get_processed_file_status(
        spark=spark,
        source_file_id=source_file_id
    )

    if current_state is None:

        raise RuntimeError(
            "Source file must be registered before its "
            f"status can be updated. source_file_id="
            f"'{source_file_id}'."
        )

    return current_state


def _validate_required_text(
    value,
    field_name: str
):
    """
    Validate required string arguments.
    """

    if value is None or not str(value).strip():

        raise ValueError(
            f"{field_name} must not be empty."
        )


def _truncate_error_message(
    error_message,
    maximum_length: int = 4000
):
    """
    Prevent excessively large exception messages from being
    written into the metadata table.
    """

    if error_message is None:
        return ""

    return str(error_message)[:maximum_length]

