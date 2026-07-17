"""
==========================================================
Table Writer
==========================================================

Reusable Delta writers.

Supports

- append
- overwrite
- merge

No business logic.
"""

from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pei_pipeline.config.settings import CATALOG


VALID_MODES = {
    "append",
    "overwrite",
    "merge"
}


def write_table(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    merge_schema: bool = False,
    merge_keys: list[str] | None = None,
    partition_by: list[str] | None = None,
    replace_where: str | None = None
):
    """
    Generic table writer.
    """

    # ----------------------------------------------------------
    # Validate Mode
    # ----------------------------------------------------------

    if mode not in VALID_MODES:
        raise ValueError(
            f"Unsupported mode : {mode}"
        )

    # ----------------------------------------------------------
    # Build Fully Qualified Table Name
    # ----------------------------------------------------------

    if table_name.count(".") == 1:
        full_table_name = f"{CATALOG}.{table_name}"

    elif table_name.count(".") == 2:
        full_table_name = table_name

    else:
        raise ValueError(
            f"Invalid table name : {table_name}"
        )

    print("=" * 60)
    print(f"Target Table : {full_table_name}")
    print(f"Mode         : {mode}")
    print(f"MergeSchema  : {merge_schema}")
    print(f"PartitionBy  : {partition_by}")
    print(f"Merge Keys   : {merge_keys}")
    print("=" * 60)

    # ----------------------------------------------------------
    # MERGE
    # ----------------------------------------------------------

    if mode == "merge":

        return _write_merge(
            df,
            full_table_name,
            merge_keys
        )

    # ----------------------------------------------------------
    # APPEND / OVERWRITE
    # ----------------------------------------------------------

    return _write_dataframe(
        df,
        full_table_name,
        mode,
        merge_schema,
        partition_by
    )


def _write_dataframe(
    df,
    table_name,
    mode,
    merge_schema,
    partition_by,
    replace_where
):
    """
    Append / Overwrite Writer.
    """

    writer = (
        df.write
        .format("delta")
        .mode(mode)
    )
    

    if merge_schema:

        writer = writer.option(
            "mergeSchema",
            "true"
        )
    
    if replace_where:

        if mode != "overwrite":
            raise ValueError(
                "replace_where requires overwrite mode."
            )

        writer = writer.option(
            "replaceWhere",
            replace_where
        )

    if partition_by:
        missing_columns = [
            c
            for c in partition_by
            if c not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Partition columns not found : {missing_columns}"
            )

        writer = writer.partitionBy(
            *partition_by
        )

    writer.saveAsTable(
        table_name
    )

    print("Write completed successfully.")

    return {
        "status": "SUCCESS",
        "mode": mode,
        "table": table_name
    }


def _write_merge(
    df,
    table_name,
    merge_keys
):
    """
    Delta Merge.
    """

    if not merge_keys:
        raise ValueError(
            "merge_keys must be supplied for merge."
        )

    delta_table = DeltaTable.forName(
        df.sparkSession,
        table_name
    )

    merge_condition = " AND ".join(
        [
            f"t.{key}=s.{key}"
            for key in merge_keys
        ]
    )

    (
        delta_table.alias("t")
        .merge(
            df.alias("s"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Merge completed successfully.")

    return {
        "status": "SUCCESS",
        "mode": "merge",
        "table": table_name
    }