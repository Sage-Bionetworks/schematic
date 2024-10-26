"""Data Model Validator"""

import time
import logging
from typing import Tuple, Any, Iterable
import itertools
from dataclasses import dataclass

import multiprocessing
import networkx as nx  # type: ignore

from schematic.schemas.data_model_relationships import DataModelRelationships

logger = logging.getLogger(__name__)

# Characters display names of nodes that are not allowed
BLACKLISTED_CHARACTERS = ["(", ")", ".", "-"]
# Names of nodes that are used internally
RESERVED_NAMES = {"entityId"}


"""
A list of node tuples.
The first item is the name of the node.
The second item is a dict of its fields.
This object is gotten from doing nx.MultiDiGraph.nodes(data=True)
"""


@dataclass
class Node:
    """A node in graph from the data model."""

    name: Any
    """Name of the node."""

    fields: dict
    """Fields of the node"""

    def __post_init__(self) -> None:
        if "displayName" not in self.fields:
            raise ValueError(f"Node: {str(self.name)} missing displayName field")
        self.display_name = str(self.fields["displayName"])


class DataModelValidator:  # pylint: disable=too-few-public-methods
    """
    Check for consistency within data model.
    """

    def __init__(
        self,
        graph: nx.MultiDiGraph,
    ):
        """
        Args:
            graph (nx.MultiDiGraph): Graph representation of the data model.
        """
        self.graph = graph
        self.node_info = [
            Node(node[0], node[1]) for node in self.graph.nodes(data=True)
        ]
        self.dmr = DataModelRelationships()

    def run_checks(self) -> tuple[list[list[str]], list[list[str]]]:
        """Run all validation checks on the data model graph.

        Returns:
            tuple[list, list]:  Returns a tuple of errors and warnings generated.

        TODO: In future could design a way for groups to customize tests run for their groups,
           run additional tests, or move some to issuing only warnings, vice versa.
        """
        error_checks = [
            self._check_graph_has_required_node_fields(),
            self._check_is_dag(),
            self._check_reserved_names(),
        ]
        warning_checks = [
            self._check_blacklisted_characters(),
        ]
        errors = [error for error in error_checks if error]
        warnings = [warning for warning in warning_checks if warning]
        return errors, warnings

    def _check_graph_has_required_node_fields(self) -> list[str]:
        """Checks that the graph has the required node fields for all nodes.

        Returns:
            list[str]: List of error messages for each missing field.
        """
        required_fields = get_node_labels_from(self.dmr.relationships_dictionary)
        missing_fields = get_missing_fields_from(self.node_info, required_fields)
        return create_missing_fields_error_messages(missing_fields)

    def _run_cycles(self) -> None:
        """run_cycles"""
        cycles = nx.simple_cycles(self.graph)
        if cycles:  # pylint:disable=using-constant-test
            for cycle in cycles:
                logger.warning(  # pylint:disable=logging-fstring-interpolation
                    (
                        f"Schematic requires models be a directed acyclic graph (DAG). Your graph "
                        f"is not a DAG, we found a loop between: {cycle[0]} and {cycle[1]}, "
                        "please remove this loop from your model and submit again."
                    )
                )

    def _check_is_dag(self) -> list[str]:
        """Check that generated graph is a directed acyclic graph

        Returns:
            list[str]:
              List of error messages if graph is not a DAG. List will include a message
                for each cycle found, if not there is a more generic message for the
                graph as a whole.
        """
        error = []
        if not nx.is_directed_acyclic_graph(self.graph):
            cycles = multiprocessing.Process(
                target=self._run_cycles,
                name="Get Cycles",
            )
            cycles.start()

            # Give up to 5 seconds to find cycles, if not exit and issue standard error
            time.sleep(5)

            # If thread is active
            if cycles.is_alive():
                # Terminate foo
                cycles.terminate()
                # Cleanup
                cycles.join()

            error.append(
                (
                    "Schematic requires models be a directed acyclic graph (DAG). "
                    "Please inspect your model."
                )
            )

        return error

    def _check_blacklisted_characters(self) -> list[str]:
        """
        We strip these characters in store, so not sure if it matter if we have them now,
         maybe add warning

        Returns:
            list[str]: list of warnings for each node in the graph, that has a Display
              name that contains blacklisted characters.
        """
        return check_characters_in_node_display_name(
            self.node_info, BLACKLISTED_CHARACTERS
        )

    def _check_reserved_names(self) -> list[str]:
        """Identify if any names nodes in the data model graph are the same as reserved name.
        Returns:
            error, list: List of errors for every node in the graph whose name overlaps
              with the reserved names.
        """
        reserved_names_found = match_node_names_with_reserved_names(
            self.graph.nodes, RESERVED_NAMES
        )
        return create_reserve_name_error_messages(reserved_names_found)


def get_node_labels_from(input_dict: dict) -> list:
    """
    Searches dict, for nested dict.
    For each nested dict, if it contains the key "node label" that value is returned.

    Args:
        input_dict (dict): A dictionary with possible nested dictionaries

    Returns:
        list: All values for node labels
    """
    node_fields = []
    for value in input_dict.values():
        if isinstance(value, dict) and "node_label" in value.keys():
            node_fields.append(value["node_label"])
    return node_fields


def get_missing_fields_from(
    nodes: list[Node], required_fields: Iterable
) -> list[Tuple[str, str]]:
    """
    Iterates through each node and checks if it contains each required_field.
    Any missing fields are returned.

    Args:
        nodes (list[Node]): A list of nodes.
        required_fields (Iterable): A Iterable of fields each node should have

    Returns:
        list[Tuple[str, str]]: A list of missing fields.
            The first item in each field is the nodes name, and the second is the missing field.
    """
    missing_fields: list[Tuple[str, str]] = []
    for node in nodes:
        missing_fields.extend(
            [
                (str(node.name), str(field))
                for field in required_fields
                if field not in node.fields.keys()
            ]
        )
    return missing_fields


def create_missing_fields_error_messages(
    missing_fields: list[Tuple[str, str]]
) -> list[str]:
    """Creates the error message for when a node is missing a required field

    Args:
        missing_fields (list[Tuple[str, str]]): A list of tuples of nodes with missing fields
          The first item is the node
          The second item is the missing field

    Returns:
        list[str]: The error message
    """
    errors: list[str] = []
    for missing_field in missing_fields:
        errors.append(
            (
                f"For entry: {missing_field[0]}, "
                f"the required field {missing_field[1]} "
                "is missing in the data model graph, please double check your model and "
                "generate the graph again."
            )
        )
    return errors


def check_characters_in_node_display_name(
    nodes: list[Node], blacklisted_characters: list[str]
) -> list[str]:
    """Checks each node 'displayName' field has no blacklisted characters

    Args:
        nodes (list[Node]): A list of nodes.
        blacklisted_characters (list[str]): A list of characters not allowed in the node
            display name

    Raises:
        ValueError: Any node is missing the 'displayName' field

    Returns:
        list[str]: A list of warning messages
    """
    warnings: list[str] = []
    for node in nodes:
        node_display_name = node.display_name

        blacklisted_characters_found = [
            character
            for character in node_display_name
            if character in blacklisted_characters
        ]

        if blacklisted_characters_found:
            warnings.append(
                create_blacklisted_characters_error_message(
                    blacklisted_characters_found, node_display_name
                )
            )
    return warnings


def create_blacklisted_characters_error_message(
    blacklisted_characters: list[str], node_name: str
) -> str:
    """Creates am error message for the presence of blacklisted characters

    Args:
        blacklisted_characters (list[str]): A list of characters that
          are unallowed in certain node field names
        node_name (str): The name of the node with the blacklisted characters

    Returns:
        str: _description_
    """
    blacklisted_characters_str = ",".join(blacklisted_characters)
    return (
        f"Node: {node_name} contains a blacklisted character(s): "
        f"{blacklisted_characters_str}, they will be striped if used in "
        "Synapse annotations."
    )


def match_node_names_with_reserved_names(
    node_names: Iterable, reserved_names: Iterable[str]
) -> list[Tuple[str, str]]:
    """Matches node names with those from a reserved list

    Args:
        node_names (Iterable): An iterable of node names
        reserved_names (Iterable[str]): A list of names to match with the node names

    Returns:
        list[Tuple[str, str]]: A List of tuples where the node name matches a reserved name
          The first item is the reserved name
          The second item is the node name
    """
    node_name_strings = [str(name) for name in node_names]
    node_name_product = itertools.product(reserved_names, node_name_strings)
    reserved_names_found = [
        node for node in node_name_product if node[0].lower() == node[1].lower()
    ]
    return reserved_names_found


def create_reserve_name_error_messages(
    reserved_names_found: list[Tuple[str, str]]
) -> list[str]:
    """Creates the error messages when a reserved name is used

    Args:
        reserved_names_found (list[Tuple[str, str]]): A list of tuples
          The first item is the reserved name
          The second item is the node name that overlapped with a reserved name

    Returns:
        list[str]: A list of error messages
    """
    return [
        (
            f"Your data model entry name: {node_name} overlaps with the reserved name: "
            f"{reserved_name}. Please change this name in your data model."
        )
        for reserved_name, node_name in reserved_names_found
    ]
