import logging
import networkx as nx
import os
from typing import Any, Dict, Optional, Text, List

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships

from schematic.utils.validate_utils import rule_in_rule_list

logger = logging.getLogger(__name__)


class DataModelJSONSchema:
    def __init__(
        self,
        jsonld_path: str,
        graph: nx.MultiDiGraph,
    ):
        # TODO: Change jsonld_path to data_model_path (can work with CSV too)
        self.jsonld_path = jsonld_path
        self.graph = graph  # Graph would be fully made at this point.
        self.dmge = DataModelGraphExplorer(self.graph)
        self.dmr = DataModelRelationships()
        self.rel_dict = self.dmr.relationships_dictionary

    def get_array_schema(
        self, node_range: List[str], node_name: str, blank=False
    ) -> Dict[str, Dict[str, List[str]]]:
        """Add a list of nodes to the "enum" key in a given JSON schema object.
           Allow a node to be mapped to any subset of the list

        Args:
                node_name: Name of the "main" / "head" key in the JSON schema / object.
                node_range: List of nodes to be added to the JSON object.
                blank: If True, add empty node to end of node list.
                           If False, do not add empty node to end of node list.

        Returns:
                JSON object with array validation rule.
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
    ) -> Dict[str, dict[str, Any]]:  # can't define heterogenous Dict generic types
        """Get a schema rule that does not allow null or empty values.

        Args:
                node_name: Name of the node on which the schema rule is to be applied.

        Returns:
                Schema rule as a JSON object.
        """
        non_blank_schema = {node_name: {"not": {"type": "null"}, "minLength": 1}}

        return non_blank_schema

    def get_range_schema(
        self, node_range: List[str], node_name: str, blank=False
    ) -> Dict[str, Dict[str, List[str]]]:
        """Add a list of nodes to the "enum" key in a given JSON schema object.

        Args:
                node_name: Name of the "main" / "head" key in the JSON schema / object.
                node_range: List of nodes to be added to the JSON object.
                blank: If True, add empty node to end of node list.
                           If False, do not add empty node to end of node list.

        Returns:
                JSON object with nodes.
        """
        if blank:
            schema_node_range = {node_name: {"enum": node_range + [""]}}
        else:
            schema_node_range = {node_name: {"enum": node_range}}

        return schema_node_range

    def get_json_validation_schema(
        self, source_node: str, schema_name: str
    ) -> Dict[str, dict[str, Any]]:
        """
        Consolidated method that aims to gather dependencies and value constraints across terms / nodes in a schema.org schema and store them in a jsonschema /JSON Schema schema.

        It does so for any given node in the schema.org schema (recursively) using the given node as starting point in the following manner:
        1) Find all the nodes / terms this node depends on (which are required as "additional metadata" given this node is "required").
        2) Find all the allowable metadata values / nodes that can be assigned to a particular node (if such a constraint is specified on the schema).

        Args:
                source_node: Node from which we can start recursive dependancy traversal (as mentioned above).
                schema_name: Name assigned to JSON-LD schema (to uniquely identify it via URI when it is hosted on the Internet).

        Returns:
                JSON Schema as a dictionary.
        """
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "http://example.com/" + schema_name,
            "title": schema_name,
            "type": "object",
            "properties": {},
            "required": [],
            "allOf": [],
        }

        nodes_to_process = (
            []
        )  # list of nodes to be checked for dependencies, starting with the source node
        processed_nodes = (
            []
        )  # keep of track of nodes whose dependencies have been processed
        reverse_dependencies = (
            {}
        )  # maintain a map between conditional nodes and their dependencies (reversed) -- {dependency : conditional_node}
        range_domain_map = (
            {}
        )  # maintain a map between range nodes and their domain nodes {range_value : domain_value}
        # the domain node is very likely the parentof ("parentOf" relationship) of the range node

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
                for n in node_range_d:
                    if not n in range_domain_map:
                        range_domain_map[n] = []
                    range_domain_map[n].append(node_display_name)

                # can this node be map to the empty set (if required no; if not required yes)
                # TODO: change "required" to different term, required may be a bit misleading (i.e. is the node required in the schema)
                node_required = self.dmge.get_node_required(node_label=process_node)

                # get any additional validation rules associated with this node (e.g. can this node be mapped to a list of other nodes)
                node_validation_rules = self.dmge.get_node_validation_rules(
                    node_display_name=node_display_name
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
                            # TODO: abstract this into its own validation rule constructor/generator module/class
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

                    json_schema["properties"].update(schema_valid_vals)

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

                                # given node conditional are satisfied, this process node (which is dependent on these conditionals) has to be set or not depending on whether it is required
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
                                            # TODO: get_range_schema and get_range_schema have similar behavior - combine in one module
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
                                json_schema["allOf"].append(
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

                        json_schema["properties"].update(schema_valid_vals)
                        # add node to required fields
                        json_schema["required"] += [node_display_name]

                    elif process_node in root_dependencies:
                        # node doesn't have conditionals and is not required; it belongs in the schema only if it is in root's dependencies

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

                        json_schema["properties"].update(schema_valid_vals)

                    else:
                        # node doesn't have conditionals and it is not required and it is not a root dependency
                        # the node doesn't belong in the schema
                        # do not add to processed nodes since its conditional may be traversed at a later iteration (though unlikely for most schemas we consider)
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

        # if no conditional dependencies were added we can't have an empty 'AllOf' block in the schema, so remove it
        if not json_schema["allOf"]:
            del json_schema["allOf"]

        # If no config value and SchemaGenerator was initialized with
        # a JSON-LD path, construct
        if self.jsonld_path is not None:
            self.jsonld_path_root, jsonld_ext = os.path.splitext(self.jsonld_path)
            prefix = self.jsonld_path_root
            prefix_root, prefix_ext = os.path.splitext(prefix)
            if prefix_ext == ".model":
                prefix = prefix_root
            json_schema_log_file = f"{prefix}.{source_node}.schema.json"
        """
		# Commenting out loggins since the JSON Schema file is not currently saved.
		logger.info(
			"The JSON schema file can be inspected by setting the following "
			"nested key in the configuration: (model > location)."
		)

		logger.info(f"JSON schema file log stored as {json_schema_log_file}")
		"""
        return json_schema
