"""Catch all utility functions and classes used in the tests."""
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CleanupAction(str, Enum):
    """Actions that can be performed on a cleanup item."""

    DELETE = "delete"


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
