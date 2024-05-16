"Data Model Json Schema"

import json
import logging
import os
from typing import Any, Optional

import networkx as nx  # type: ignore

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.utils.validate_utils import rule_in_rule_list
from schematic.utils.schema_utils import get_json_schema_log_file_path

logger = logging.getLogger(__name__)


class DataModelJSONSchema:
    "Data Model Json Schema"

    def __init__(
        self,
        jsonld_path: str,
        graph: nx.MultiDiGraph,
    ):
        # pylint:disable=fixme
        # TODO: Change jsonld_path to data_model_path (can work with CSV too)
        self.jsonld_path = jsonld_path
        self.jsonld_path_root: Optional[str] = None
        self.graph = graph  # Graph would be fully made at this point.
        self.dmge = DataModelGraphExplorer(self.graph)
        self.dmr = DataModelRelationships()
        self.rel_dict = self.dmr.relationships_dictionary

    def get_array_schema(
        self, node_range: list[str], node_name: str, blank: bool = False
    ) -> dict[str, dict[str, Any]]:
        """
        Add a list of nodes to the "enum" key in a given JSON schema object.
        Allow a node to be mapped to any subset of the list

        Args:
            node_range (list[str]): List of nodes to be added to the JSON object.
            node_name (str): Name of the "main" / "head" key in the JSON schema / object.
            blank (bool, optional): Defaults to False.
              If True, add empty node to end of node list.
              If False, do not add empty node to end of node list.

        Returns:
            dict[str, dict[str, Any]]: JSON object with array validation rule.
        """
        schema_node_range_array = {
            node_name: {
                "type": "array",
                "items": {"enum": node_range + [""] if blank else node_range},
                "maxItems": len(node_range),
            }
        }

        return schema_node_range_array

    def get_non_blank_schema(
        self, node_name: str
    ) -> dict[str, dict[str, Any]]:  # can't define heterogenous Dict generic types
        """Get a schema rule that does not allow null or empty values.

        Args:
                node_name: Name of the node on which the schema rule is to be applied.

        Returns:
                Schema rule as a JSON object.
        """
        non_blank_schema = {node_name: {"not": {"type": "null"}, "minLength": 1}}

        return non_blank_schema

    def get_range_schema(
        self, node_range: list[str], node_name: str, blank: bool = False
    ) -> dict[str, dict[str, list[str]]]:
        """
        Add a list of nodes to the "enum" key in a given JSON schema object.

        Args:
            node_range (list[str]): List of nodes to be added to the JSON object.
            node_name (str): Name of the "main" / "head" key in the JSON schema / object.
            blank (bool, optional): Defaults to False.
              If True, add empty node to end of node list.
              If False, do not add empty node to end of node list.

        Returns:
            dict[str, dict[str, list[str]]]: JSON object with nodes.
        """
        if blank:
            schema_node_range = {node_name: {"enum": node_range + [""]}}
        else:
            schema_node_range = {node_name: {"enum": node_range}}

        return schema_node_range

    def get_json_validation_schema(
        self, source_node: str, schema_name: str
    ) -> dict[str, dict[str, Any]]:
        """
        Consolidated method that aims to gather dependencies and value constraints across terms
        / nodes in a schema.org schema and store them in a jsonschema /JSON Schema schema.

        It does so for any given node in the schema.org schema (recursively) using the given
          node as starting point in the following manner:
        1) Find all the nodes / terms this node depends on (which are required as
          "additional metadata" given this node is "required").
        2) Find all the allowable metadata values / nodes that can be assigned to a particular
          node (if such a constraint is specified on the schema).

        Args:
                source_node: Node from which we can start recursive dependancy traversal
                  (as mentioned above).
                schema_name: Name assigned to JSON-LD schema (to uniquely identify it via URI
                  when it is hosted on the Internet).

        Returns:
                JSON Schema as a dictionary.
        """
        # pylint:disable=too-many-locals
        # pylint:disable=too-many-branches
        # pylint:disable=too-many-statements
        # pylint:disable=too-many-nested-blocks
        # pylint:disable=fixme

        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "http://example.com/" + schema_name,
            "title": schema_name,
            "type": "object",
            "properties": {},
            "required": [],
            "allOf": [],
        }

        # list of nodes to be checked for dependencies, starting with the source node
        nodes_to_process = []
        # keep of track of nodes whose dependencies have been processed
        processed_nodes = []
        # maintain a map between conditional nodes and their dependencies
        # (reversed) -- {dependency : conditional_node}
        reverse_dependencies: dict[str, Any] = {}
        # maintain a map between range nodes and their domain nodes {range_value : domain_value}
        # the domain node is very likely the parentof ("parentOf" relationship) of the range node
        range_domain_map: dict[str, Any] = {}
        root_dependencies = self.dmge.get_adjacent_nodes_by_relationship(
            node_label=source_node,
            relationship=self.rel_dict["requiresDependency"]["edge_key"],
        )

        # if root_dependencies is empty it means that a class with name 'source_node' exists
        # in the schema, but it is not a valid component
        if not root_dependencies:
            raise ValueError(f"'{source_node}' is not a valid component in the schema.")

        nodes_to_process += root_dependencies

        process_node = nodes_to_process.pop(0)

        while process_node:
            if not process_node in processed_nodes:
                # node is being processed
                node_is_processed = True

                node_range = self.dmge.get_adjacent_nodes_by_relationship(
                    node_label=process_node,
                    relationship=self.rel_dict["rangeIncludes"]["edge_key"],
                )

                # get node range display name
                node_range_d = self.dmge.get_nodes_display_names(node_list=node_range)

                node_dependencies = self.dmge.get_adjacent_nodes_by_relationship(
                    node_label=process_node,
                    relationship=self.rel_dict["requiresDependency"]["edge_key"],
                )

                # get process node display name
                node_display_name = self.graph.nodes[process_node][
                    self.rel_dict["displayName"]["node_label"]
                ]

                # updating map between node and node's valid values
                for node in node_range_d:
                    if not node in range_domain_map:
                        range_domain_map[node] = []
                    range_domain_map[node].append(node_display_name)

                # Get node validation rules for the current node, and the given component
                node_validation_rules = self.dmge.get_component_node_validation_rules(
                    manifest_component=source_node, node_display_name=node_display_name
                )

                # Get if the node is required for the given component
                node_required = self.dmge.get_component_node_required(
                    manifest_component=source_node,
                    node_validation_rules=node_validation_rules,
                    node_display_name=node_display_name,
                )

                if node_display_name in reverse_dependencies:
                    # if node has conditionals set schema properties and conditional dependencies
                    # set schema properties
                    if node_range:
                        # if process node has valid value range set it in schema properties
                        schema_valid_vals = self.get_range_schema(
                            node_range=node_range_d,
                            node_name=node_display_name,
                            blank=True,
                        )

                        if node_validation_rules:
                            # if this node has extra validation rules process them
                            # TODO: abstract this into its own validation rule constructor/generator
                            # module/class
                            if rule_in_rule_list("list", node_validation_rules):
                                # if this node can be mapped to a list of nodes
                                # set its schema accordingly
                                schema_valid_vals = self.get_array_schema(
                                    node_range=node_range_d,
                                    node_name=node_display_name,
                                    blank=True,
                                )

                    else:
                        # otherwise, by default allow any values
                        schema_valid_vals = {node_display_name: {}}

                    json_schema["properties"].update(schema_valid_vals)  # type: ignore

                    # set schema conditional dependencies
                    for node in reverse_dependencies[node_display_name]:
                        # set all of the conditional nodes that require this process node
                        # get node domain if any
                        # ow this node is a conditional requirement
                        if node in range_domain_map:
                            domain_nodes = range_domain_map[node]
                            conditional_properties = {}

                            for domain_node in domain_nodes:
                                # set range of conditional node schema
                                conditional_properties.update(
                                    {
                                        "properties": {domain_node: {"enum": [node]}},
                                        "required": [domain_node],
                                    }
                                )

                                # given node conditional are satisfied, this process node
                                # (which is dependent on these conditionals) has to be set
                                # or not depending on whether it is required
                                if node_range:
                                    dependency_properties = self.get_range_schema(
                                        node_range=node_range_d,
                                        node_name=node_display_name,
                                        blank=not node_required,
                                    )

                                    if node_validation_rules:
                                        if rule_in_rule_list(
                                            "list", node_validation_rules
                                        ):
                                            # TODO: get_range_schema and get_range_schema have
                                            # similar behavior - combine in one module
                                            dependency_properties = (
                                                self.get_array_schema(
                                                    node_range=node_range_d,
                                                    node_name=node_display_name,
                                                    blank=not node_required,
                                                )
                                            )

                                else:
                                    if node_required:
                                        dependency_properties = (
                                            self.get_non_blank_schema(
                                                node_name=node_display_name
                                            )
                                        )
                                    else:
                                        dependency_properties = {node_display_name: {}}
                                schema_conditional_dependencies = {
                                    "if": conditional_properties,
                                    "then": {
                                        "properties": dependency_properties,
                                        "required": [node_display_name],
                                    },
                                }

                                # update conditional-dependency rules in json schema
                                json_schema["allOf"].append(  # type: ignore
                                    schema_conditional_dependencies
                                )

                else:
                    # node doesn't have conditionals
                    if node_required:
                        if node_range:
                            schema_valid_vals = self.get_range_schema(
                                node_range=node_range_d,
                                node_name=node_display_name,
                                blank=False,
                            )

                            if node_validation_rules:
                                # If there are valid values AND they are expected to be a list,
                                # reformat the Valid Values.
                                if rule_in_rule_list("list", node_validation_rules):
                                    schema_valid_vals = self.get_array_schema(
                                        node_range=node_range_d,
                                        node_name=node_display_name,
                                        blank=False,
                                    )
                        else:
                            schema_valid_vals = self.get_non_blank_schema(
                                node_name=node_display_name
                            )

                        json_schema["properties"].update(schema_valid_vals)  # type: ignore
                        # add node to required fields
                        json_schema["required"] += [node_display_name]  # type: ignore

                    elif process_node in root_dependencies:
                        # node doesn't have conditionals and is not required; it belongs in the
                        # schema only if it is in root's dependencies

                        if node_range:
                            schema_valid_vals = self.get_range_schema(
                                node_range=node_range_d,
                                node_name=node_display_name,
                                blank=True,
                            )

                            if node_validation_rules:
                                if rule_in_rule_list("list", node_validation_rules):
                                    schema_valid_vals = self.get_array_schema(
                                        node_range=node_range_d,
                                        node_name=node_display_name,
                                        blank=True,
                                    )

                        else:
                            schema_valid_vals = {node_display_name: {}}

                        json_schema["properties"].update(schema_valid_vals)  # type: ignore

                    else:
                        # node doesn't have conditionals and it is not required and it
                        # is not a root dependency the node doesn't belong in the schema
                        # do not add to processed nodes since its conditional may be traversed
                        # at a later iteration (though unlikely for most schemas we consider)
                        node_is_processed = False

                # add process node as a conditional to its dependencies
                node_dependencies_d = self.dmge.get_nodes_display_names(
                    node_list=node_dependencies
                )

                for dep in node_dependencies_d:
                    if not dep in reverse_dependencies:
                        reverse_dependencies[dep] = []

                    reverse_dependencies[dep].append(node_display_name)

                # add nodes found as dependencies and range of this processed node
                # to the list of nodes to be processed
                nodes_to_process += node_range
                nodes_to_process += node_dependencies

                # if the node is processed add it to the processed nodes set
                if node_is_processed:
                    processed_nodes.append(process_node)

            # if the list of nodes to process is not empty
            # set the process node the next remaining node to process
            if nodes_to_process:
                process_node = nodes_to_process.pop(0)
            else:
                # no more nodes to process
                # exit the loop
                break

        logger.info("JSON schema successfully generated from schema.org schema!")

        # if no conditional dependencies were added we can't have an empty 'AllOf'
        # block in the schema, so remove it
        if not json_schema["allOf"]:
            del json_schema["allOf"]

        # If no config value and SchemaGenerator was initialized with
        # a JSON-LD path, construct
        if self.jsonld_path is not None:
            json_schema_log_file_path = get_json_schema_log_file_path(
                data_model_path=self.jsonld_path, source_node=source_node
            )
        if json_schema_log_file_path is None:
            logger.info(
                "The JSON schema file can be inspected by setting the following "
                "nested key in the configuration: (model > location)."
            )
        else:
            json_schema_dirname = os.path.dirname(json_schema_log_file_path)
            if json_schema_dirname != "":
                os.makedirs(json_schema_dirname, exist_ok=True)
            with open(json_schema_log_file_path, "w", encoding="UTF-8") as js_f:
                json.dump(json_schema, js_f, indent=2)
        return json_schema  # type: ignore
