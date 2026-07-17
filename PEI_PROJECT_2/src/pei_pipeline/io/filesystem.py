"""
==========================================================
Filesystem Utilities
==========================================================

Reusable filesystem functions.
"""

import fnmatch
import hashlib


def generate_source_file_id(
    source_name: str,
    file_path: str,
    file_size: int,
    modification_time: int
) -> str:
    """
    Generate a deterministic ID for one physical version
    of a source file.

    The same file version always generates the same ID.
    If the path, size, or modification time changes,
    a new source_file_id is generated.
    """

    file_identity = "|".join([
        source_name.strip().lower(),
        file_path.strip(),
        str(file_size),
        str(modification_time)
    ])

    return hashlib.sha256(
        file_identity.encode("utf-8")
    ).hexdigest()

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