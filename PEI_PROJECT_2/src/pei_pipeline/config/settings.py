"""
Project-wide configuration constants.

This module contains only configuration values used across the PEI pipeline.
Do not add Spark code or business logic here.
"""

# ==========================================================
# Unity Catalog
# ==========================================================

CATALOG = "ecommerce_dev"

# ==========================================================
# Schemas
# ==========================================================

RAW_SCHEMA = "raw"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
METADATA_SCHEMA = "metadata"
AUDIT_SCHEMA = "audit"

# ==========================================================
# Metadata Tables
# ==========================================================

INGESTION_CONFIG_TABLE = (
    f"{CATALOG}.{METADATA_SCHEMA}.ingestion_config"
)

REQUIRED_SCHEMA_TABLE = (
    f"{CATALOG}.{METADATA_SCHEMA}.required_schema"
)

# COLUMN_MAPPING_TABLE = (
#     f"{CATALOG}.{METADATA_SCHEMA}.column_mapping"
# )

DATA_QUALITY_RULE_TABLE = (
    f"{CATALOG}.{METADATA_SCHEMA}.data_quality_rules"
)

# ==========================================================
# Audit Tables
# ==========================================================

PIPELINE_RUN_LOG_TABLE = (
    f"{CATALOG}.{AUDIT_SCHEMA}.pipeline_run_log"
)

SCHEMA_VALIDATION_LOG_TABLE = (
    f"{CATALOG}.{AUDIT_SCHEMA}.schema_validation_log"
)

REJECTED_RECORDS_TABLE = (
    f"{CATALOG}.{AUDIT_SCHEMA}.rejected_records"
)

# ==========================================================
# Metadata Columns
# ==========================================================

PIPELINE_RUN_ID_COLUMN = "pipeline_run_id"
SOURCE_FILE_COLUMN = "source_file"
INGESTION_TIMESTAMP_COLUMN = "ingestion_timestamp"

# ==========================================================
# Auto Loader
# ==========================================================

CHECKPOINT_ROOT = "/Volumes/ecommerce_dev/system/checkpoints"

SCHEMA_LOCATION_ROOT = "/Volumes/ecommerce_dev/system/schema"

# ==========================================================
# Supported File Types
# ==========================================================

SUPPORTED_FILE_TYPES = {
    "csv",
    "json",
    "parquet",
    "excel"
}

# ==========================================================
# Read Options
# ==========================================================

CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false"
}

JSON_OPTIONS = {
    "multiLine": "true"
}

PARQUET_OPTIONS = {}

EXCEL_OPTIONS = {
    "header": "true",
    "inferSchema": "false"
}

# ==========================================================
# Logging
# ==========================================================

LOG_LEVEL = "INFO"