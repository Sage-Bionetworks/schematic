"""io utils"""

import json
import os
import time
import urllib.request
from typing import Any


def load_json(file_path: str) -> Any:
    """Load json document from file path or url

    :arg str file_path: The path of the url doc, could be url or file path
    """
    if file_path.startswith("http"):
        with urllib.request.urlopen(file_path) as url:
            data = json.loads(url.read().decode())
            return data
    # handle file path
    else:
        with open(file_path, encoding="utf8") as fle:
            data = json.load(fle)
            return data


def export_json(json_doc: Any, file_path: str) -> None:
    """Export JSON doc to file"""
    with open(file_path, "w", encoding="utf8") as fle:
        json.dump(json_doc, fle, sort_keys=True, indent=4, ensure_ascii=False)


def load_default() -> Any:
    """Load biolink vocabulary"""
    data_path = "data_models/biothings.model.jsonld"
    # Lazy import to avoid circular imports
    from schematic import LOADER  # pylint: disable=import-outside-toplevel

    biothings_path = LOADER.filename(data_path)
    return load_json(biothings_path)


def load_schemaorg() -> Any:
    """Load SchemaOrg vocabulary"""
    data_path = "data_models/schema_org.model.jsonld"
    # Lazy import to avoid circular imports
    from schematic import LOADER  # pylint: disable=import-outside-toplevel

    schema_org_path = LOADER.filename(data_path)
    return load_json(schema_org_path)


def cleanup_temporary_storage(
    temporary_storage_directory: str, time_delta_seconds: int
) -> None:
    """Handles cleanup of temporary storage directory. The usage of the
    `time_delta_seconds` parameter is to prevent deleting files that are currently
    being used by other requests. In production we will be deleting those files
    which have not been modified for more than 1 hour.

    Args:
        temporary_storage_directory: Path to the temporary storage directory.
        time_delta_seconds: The time delta in seconds used to determine which files
            should be deleted.
    """
    if os.path.exists(temporary_storage_directory):
        for root, all_dirs, files in os.walk(
            temporary_storage_directory, topdown=False
        ):
            # Delete files older than the specified time delta
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.isfile(file_path) and os.path.getmtime(file_path) < (
                    time.time() - time_delta_seconds
                ):
                    os.remove(file_path)

            # Delete empty directories
            for all_dir in all_dirs:
                dir_path = os.path.join(root, all_dir)
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
