"""
==========================================================
Filesystem Utilities
==========================================================

Reusable filesystem functions.
"""

import fnmatch


def get_matching_files(
    dbutils,
    landing_path: str,
    file_pattern: str
):
    """
    Returns all files matching the configured pattern,
    sorted by modification time.
    """

    files = dbutils.fs.ls(landing_path)

    print(f"Landing Path : {landing_path}")
    print(f"File Pattern : {file_pattern}")

    matching_files = [

        file

        for file in files

        if fnmatch.fnmatch(
            file.name.lower(),
            file_pattern.lower()
        )

    ]

    matching_files.sort(
        key=lambda file: file.modificationTime
    )

    return matching_files


def move_file(
    dbutils,
    source_path: str,
    destination_path: str
):
    """
    Move processed file to archive.
    """

    dbutils.fs.mv(
        source_path,
        destination_path
    )