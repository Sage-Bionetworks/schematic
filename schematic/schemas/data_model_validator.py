"""Data Model Validator"""

import time
import logging

import multiprocessing
import networkx as nx  # type: ignore

from schematic.schemas.data_model_relationships import DataModelRelationships

logger = logging.getLogger(__name__)


class DataModelValidator:
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
        TODO: put blacklisted chars and reserved_names in some global space where
          they can be accessed centrally
        """
        self.graph = graph
        self.dmr = DataModelRelationships()
        # Define blacklisted characters, taken from store.synapse
        self.blacklisted_chars = ["(", ")", ".", "-"]
        # Define reserved_names, taken from Documentation
        self.reserved_names = {"entityId"}

    def run_checks(self) -> tuple[list, list]:
        """Run all validation checks on the data model graph.

        Returns:
            tuple[list, list]:  Returns a tuple of errors and warnings generated.

        TODO: In future could design a way for groups to customize tests run for their groups,
           run additional tests, or move some to issuing only warnings, vice versa.
        """
        error_checks = [
            self.check_graph_has_required_node_fields(),
            self.check_is_dag(),
            self.check_reserved_names(),
        ]
        warning_checks = [
            self.check_blacklisted_characters(),
        ]
        errors = [error for error in error_checks if error]
        warnings = [warning for warning in warning_checks if warning]
        return errors, warnings

    def check_graph_has_required_node_fields(self) -> list[str]:
        """Checks that the graph has the required node fields for all nodes.
        Returns:
                error, list: List of error messages for each missing field.
        """
        # Get all the fields that should be recorded per node
        rel_dict = self.dmr.relationships_dictionary
        node_fields = []
        for value in rel_dict.values():
            if "node_label" in value.keys():
                node_fields.append(value["node_label"])

        error = []
        missing_fields = []
        # Check that required fields are present for each node.
        for node, node_dict in self.graph.nodes(data=True):
            missing_fields.extend(
                [(node, f) for f in node_fields if f not in node_dict.keys()]
            )

        if missing_fields:
            for missing_field in missing_fields:
                error.append(
                    (
                        f"For entry: {missing_field[0]}, the required field {missing_field[1]} "
                        "is missing in the data model graph, please double check your model and "
                        "generate the graph again."
                    )
                )
        return error

    def run_cycles(self) -> None:
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

    def check_is_dag(self) -> list[str]:
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
                target=self.run_cycles,
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

    def check_blacklisted_characters(self) -> list[str]:
        """
        We strip these characters in store, so not sure if it matter if we have them now,
         maybe add warning

        Returns:
            list[str]: list of warnings for each node in the graph, that has a Display
              name that contains blacklisted characters.
        """
        warning = []
        for _, node_dict in self.graph.nodes(data=True):
            if any(
                bl_char in node_dict["displayName"]
                for bl_char in self.blacklisted_chars
            ):
                node_display_name = node_dict["displayName"]
                blacklisted_characters_found = [
                    bl_char
                    for bl_char in self.blacklisted_chars
                    if bl_char in node_dict["displayName"]
                ]
                blacklisted_characters_str = ",".join(blacklisted_characters_found)
                warning.append(
                    (
                        f"Node: {node_display_name} contains a blacklisted character(s): "
                        f"{blacklisted_characters_str}, they will be striped if used in "
                        "Synapse annotations."
                    )
                )
        return warning

    def check_reserved_names(self) -> list[str]:
        """Identify if any names nodes in the data model graph are the same as reserved name.
        Returns:
            error, list: List of erros for every node in the graph whose name overlaps
              with the reserved names.
        """
        error = []
        reserved_names_found = [
            (name, node)
            for node in self.graph.nodes
            for name in self.reserved_names
            if name.lower() == node.lower()
        ]
        if reserved_names_found:
            for reserved_name, node_name in reserved_names_found:
                error.append(
                    (
                        f"Your data model entry name: {node_name} overlaps with the reserved name: "
                        f"{reserved_name}. Please change this name in your data model."
                    )
                )
        return error

    def check_namespace_overlap(self) -> list:
        """
        Check if name is repeated.
        Implement in the future
        """
        warning: list = []
        return warning

    def check_for_orphan_attributes(self) -> list:
        """
        Check if attribute is specified but not connected to another attribute or component.
        Implement in future
        """
        warning: list = []
        return warning

    def check_namespace_similarity(self) -> list:
        """
        Using AI, check if submitted attributes or valid values are similar to other ones,
          warn users.
        Implement in future
        """
        warning: list = []
        return warning
