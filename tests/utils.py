"""Catch all utility functions and classes used in the tests."""
from dataclasses import dataclass
from enum import Enum
from typing import Optional
import json


class CleanupAction(str, Enum):
    """Actions that can be performed on a cleanup item."""

    DELETE = "delete"

    def __str__(self) -> str:
        # See https://peps.python.org/pep-0663/
        return self.value


@dataclass(frozen=True)
class CleanupItem:
    """Simple class used to create a test finalizer and cleanup resources after test execution.

    synapse_id or (name and parent_id) must be provided.

    Attributes:
        synapse_id (str): The Synapse ID of the resource to cleanup.
        name (str): The name of the resource to cleanup.
        parent_id (str): The parent ID of the resource to cleanup.
        action (CleanupAction): The action to perform on the resource.

    """

    synapse_id: Optional[str] = None
    name: Optional[str] = None
    parent_id: Optional[str] = None
    action: CleanupAction = CleanupAction.DELETE


def dict_sort(item):
    if isinstance(item, dict):
        return sorted((key, dict_sort(values)) for key, values in item.items())
    if isinstance(item, list):
        return sorted(dict_sort(x) for x in item)
    else:
        return item


def dict_equal(dict1: dict, dict2: dict) -> bool:
    return dict_sort(dict1) == dict_sort(dict2)


def json_files_equal(file1: str, file2: str) -> bool:
    """Compare two JSON files for equality.

    Args:
        file1 (str): The path to the first JSON file.
        file2 (str): The path to the second JSON file.

    Returns:
        bool: True if the JSON files are equal, False otherwise.
    """
    with open(file1) as f1, open(file2) as f2:
        return json.load(f1) == json.load(f2)
