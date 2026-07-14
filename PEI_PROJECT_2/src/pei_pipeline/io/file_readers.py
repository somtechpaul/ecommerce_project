"""
==========================================================
File Readers
==========================================================

Reusable file readers for supported source file types.

Responsibilities
----------------
- Read CSV
- Read JSON
- Read Parquet
- Read Excel

Returns
-------
Spark DataFrame

Does NOT

- Discover files
- Read metadata tables
- Write tables
- Validate schema
- Rename columns
"""
from pyspark.sql import DataFrame

from pei_pipeline.config.settings import (
    CSV_OPTIONS,
    JSON_OPTIONS,
    PARQUET_OPTIONS,
    EXCEL_OPTIONS
)


def read_csv(
    spark,
    file_path: str,
    options: dict | None = None
) -> DataFrame:
    """
    Read a CSV file.
    """

    read_options = CSV_OPTIONS.copy()

    if options:
        read_options.update(options)

    return (
        spark.read
        .format("csv")
        .options(**read_options)
        .load(file_path)
    )


def read_json(
    spark,
    file_path: str,
    options: dict | None = None
) -> DataFrame:
    """
    Read a JSON file.
    """

    read_options = JSON_OPTIONS.copy()

    if options:
        read_options.update(options)

    return (
        spark.read
        .format("json")
        .options(**read_options)
        .load(file_path)
    )


def read_parquet(
    spark,
    file_path: str,
    options: dict | None = None
) -> DataFrame:
    """
    Read a Parquet file.
    """

    read_options = PARQUET_OPTIONS.copy()

    if options:
        read_options.update(options)

    return (
        spark.read
        .format("parquet")
        .options(**read_options)
        .load(file_path)
    )


def read_excel(
    spark,
    file_path: str,
    options: dict | None = None
) -> DataFrame:
    """
    Read an Excel file.
    """

    read_options = EXCEL_OPTIONS.copy()

    if options:
        read_options.update(options)

    return (
        spark.read
        .format("com.crealytics.spark.excel")
        .options(**read_options)
        .load(file_path)
    )


def read_file(
    spark,
    file_path: str,
    file_type: str,
    options: dict | None = None
) -> DataFrame:
    """
    Generic file reader.

    Parameters
    ----------
    spark
    file_path
    file_type
    options

    Returns
    -------
    Spark DataFrame
    """

    readers = {

        "csv": read_csv,

        "json": read_json,

        "parquet": read_parquet,

        "excel": read_excel

    }

    file_type = file_type.lower()

    if file_type not in readers:
        raise ValueError(f"Unsupported file type : {file_type}")

    return readers[file_type](

        spark,

        file_path,

        options

    )