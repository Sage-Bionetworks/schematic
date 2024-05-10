"""Data Model Jsonld"""

import json
import logging
import copy
from typing import Union

from dataclasses import dataclass, field
from dataclasses_json import config, dataclass_json

import networkx as nx  # type: ignore

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.utils.schema_utils import (
    get_label_from_display_name,
    convert_bool_to_str,
)

logging.basicConfig()
logger = logging.getLogger(__name__)

# pylint:disable=fixme
# pylint:disable=invalid-name
# pylint:disable=too-many-instance-attributes


@dataclass_json
@dataclass
class BaseTemplate:
    """Base Template"""

    magic_context: dict[str, str] = field(
        default_factory=lambda: {
            "bts": "http://schema.biothings.io/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "schema": "http://schema.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
        },
        metadata=config(field_name="@context"),
    )
    magic_graph: list = field(
        default_factory=list, metadata=config(field_name="@graph")
    )
    magic_id: str = field(
        default="http://schema.biothings.io/#0.1", metadata=config(field_name="@id")
    )


@dataclass_json
@dataclass
class PropertyTemplate:
    """Property Template"""

    magic_id: str = field(default="", metadata=config(field_name="@id"))
    magic_type: str = field(default="rdf:Property", metadata=config(field_name="@type"))
    magic_comment: str = field(default="", metadata=config(field_name="rdfs:comment"))
    magic_label: str = field(default="", metadata=config(field_name="rdfs:label"))
    magic_domain_includes: list = field(
        default_factory=list, metadata=config(field_name="schema:domainIncludes")
    )
    magic_range_includes: list = field(
        default_factory=list, metadata=config(field_name="schema:rangeIncludes")
    )
    magic_isPartOf: dict = field(
        default_factory=dict, metadata=config(field_name="schema:isPartOf")
    )
    magic_displayName: str = field(
        default="", metadata=config(field_name="sms:displayName")
    )
    magic_required: str = field(
        default="sms:false", metadata=config(field_name="sms:required")
    )
    magic_validationRules: list = field(
        default_factory=list, metadata=config(field_name="sms:validationRules")
    )


@dataclass_json
@dataclass
class ClassTemplate:
    "Class Template"
    magic_id: str = field(default="", metadata=config(field_name="@id"))
    magic_type: str = field(default="rdfs:Class", metadata=config(field_name="@type"))
    magic_comment: str = field(default="", metadata=config(field_name="rdfs:comment"))
    magic_label: str = field(default="", metadata=config(field_name="rdfs:label"))
    magic_subClassOf: list = field(
        default_factory=list, metadata=config(field_name="rdfs:subClassOf")
    )
    magic_range_includes: list = field(
        default_factory=list, metadata=config(field_name="schema:rangeIncludes")
    )
    magic_isPartOf: dict = field(
        default_factory=dict, metadata=config(field_name="schema:isPartOf")
    )
    magic_displayName: str = field(
        default="", metadata=config(field_name="sms:displayName")
    )
    magic_required: str = field(
        default="sms:false", metadata=config(field_name="sms:required")
    )
    magic_requiresDependency: list = field(
        default_factory=list, metadata=config(field_name="sms:requiresDependency")
    )
    magic_requiresComponent: list = field(
        default_factory=list, metadata=config(field_name="sms:requiresComponent")
    )
    magic_validationRules: list = field(
        default_factory=list, metadata=config(field_name="sms:validationRules")
    )


class DataModelJsonLD:
    """
    #Interface to JSONLD_object
    """

    def __init__(self, graph: nx.MultiDiGraph, output_path: str = ""):
        # Setup
        self.graph = graph  # Graph would be fully made at this point.
        self.dmr = DataModelRelationships()
        self.rel_dict = self.dmr.relationships_dictionary
        self.dmge = DataModelGraphExplorer(self.graph)
        self.output_path = output_path

        # Gather the templates
        base_template = BaseTemplate()
        self.base_jsonld_template = json.loads(
            base_template.to_json()  # type: ignore # pylint:disable=no-member
        )

        property_template = PropertyTemplate()
        self.property_template = json.loads(
            property_template.to_json()  # type: ignore # pylint:disable=no-member
        )

        class_template = ClassTemplate()
        self.class_template = json.loads(
            class_template.to_json()  # type: ignore # pylint:disable=no-member
        )

    def get_edges_associated_with_node(
        self, node: str
    ) -> list[tuple[str, str, dict[str, int]]]:
        """Retrieve all edges traveling in and out of a node.
        Args:
            node, str: Label of node in the graph to look for assiciated edges
        Returns:
            node_edges, list: List of Tuples of edges associated with the given node,
              tuple contains the two nodes, plus the weight dict associated with
              the edge connection.
        """
        node_edges = list(self.graph.in_edges(node, data=True))
        node_edges.extend(list(self.graph.out_edges(node, data=True)))
        return node_edges

    def get_edges_associated_with_property_nodes(
        self, node: str
    ) -> list[tuple[str, str, dict[str, int]]]:
        """Get edges associated with property nodes to make sure we add that relationship.
        Args:
            node, str: Label of node property in the graph to look for assiciated edges
        Returns:
            node_edges, list: List of Tuples of edges associated with the given node,
              tuple contains the two nodes, plus the weight dict associated with the
              edge connection.
        """
        # Get edge keys for domainIncludes and subclassOf
        domain_includes_edge_key = self.rel_dict["domainIncludes"]["edge_key"]
        node_edges = []
        # Get dict of edges for the current property node
        node_edges_dict = self.graph[node]
        for node_2, edge_dict in node_edges_dict.items():
            # Look through relationships in the edge dictionary
            for edge_key in edge_dict:
                # If the edge is a property or subclass then add the edges to the list
                if edge_key in [domain_includes_edge_key]:
                    node_edges.append((node, node_2, edge_dict[edge_key]))
        return node_edges

    def add_edge_rels_to_template(  # pylint:disable=too-many-branches
        self, template: dict, rel_vals: dict, node: str
    ) -> dict:
        """
        Args:
            template, dict: single class or property JSONLD template that is in the process of being
             filled.
            rel_vals, dict: sub relationship dict for a given relationship (contains informtion
              like 'edge_rel', 'jsonld_key' etc..)
            node, str: node whose edge information is presently being added to the JSONLD
        Returns:
        """
        # Get all edges associated with the current node
        node_edges = self.get_edges_associated_with_node(node=node)

        # For properties look for reverse relationships too
        if node in self.dmge.find_properties():
            property_node_edges = self.get_edges_associated_with_property_nodes(
                node=node
            )
            node_edges.extend(property_node_edges)

        # Get node pairs and weights for each edge
        for node_1, node_2, _ in node_edges:  # pylint:disable=too-many-nested-blocks
            # Retrieve the relationship(s) and related info between the two nodes
            node_edge_relationships = self.graph[node_1][node_2]

            # Get the relationship edge key
            edge_key = rel_vals["edge_key"]

            # Check if edge_key is even one of the relationships for this node pair.
            if edge_key in node_edge_relationships:
                # for each relationship between the given nodes
                for relationship in node_edge_relationships.keys():
                    # If the relationship defined and edge_key
                    if relationship == edge_key:
                        # TODO: rewrite to use edge_dir
                        domain_includes_edge_key = self.rel_dict["domainIncludes"][
                            "edge_key"
                        ]
                        subclass_of_edge_key = self.rel_dict["subClassOf"]["edge_key"]
                        if edge_key in [subclass_of_edge_key]:
                            if node_2 == node:
                                # Make sure the key is in the template
                                # (differs between properties and classes)
                                if rel_vals["jsonld_key"] in template.keys():
                                    node_1_id = {"@id": "bts:" + node_1}
                                    # TODO Move this to a helper function to clear up.
                                    if (
                                        isinstance(
                                            template[rel_vals["jsonld_key"]], list
                                        )
                                        and node_1_id
                                        not in template[rel_vals["jsonld_key"]]
                                    ):
                                        template[rel_vals["jsonld_key"]].append(
                                            node_1_id
                                        )
                        elif edge_key in [domain_includes_edge_key]:
                            if node_1 == node:
                                # Make sure the key is in the template
                                # (differs between properties and classes)
                                if rel_vals["jsonld_key"] in template.keys():
                                    node_2_id = {"@id": "bts:" + node_2}
                                    # TODO Move this to a helper function to clear up.
                                    if (
                                        isinstance(
                                            template[rel_vals["jsonld_key"]], list
                                        )
                                        and node_2_id
                                        not in template[rel_vals["jsonld_key"]]
                                    ):
                                        template[rel_vals["jsonld_key"]].append(
                                            node_2_id
                                        )
                        else:
                            if node_1 == node:
                                # Make sure the key is in the template
                                # (differs between properties and classes)
                                if rel_vals["jsonld_key"] in template.keys():
                                    node_2_id = {"@id": "bts:" + node_2}
                                    # TODO Move this to a helper function to clear up.
                                    if (
                                        isinstance(
                                            template[rel_vals["jsonld_key"]], list
                                        )
                                        and node_2_id
                                        not in template[rel_vals["jsonld_key"]]
                                    ):
                                        template[rel_vals["jsonld_key"]].append(
                                            node_2_id
                                        )
        return template

    def add_node_info_to_template(
        self, template: dict, rel_vals: dict, node: str
    ) -> dict:
        """For a given node and relationship, add relevant value to template
        Args:
            template, dict: single class or property JSONLD template that is in the process
              of being filled.
            rel_vals, dict: sub relationship dict for a given relationship
              (contains informtion like, 'edge_rel', 'jsonld_key' etc..)
            node, str: node whose information is presently being added to the JSONLD
        Returns:
            template, dict: single class or property JSONLD template that is in the
              process of being filled, and now has had additional node information added.
        """
        # Get label for relationship used in the graph
        node_label = rel_vals["node_label"]

        # Get recorded info for current node, and the attribute type
        node_info = nx.get_node_attributes(self.graph, node_label)[node]

        # Add this information to the template
        template[rel_vals["jsonld_key"]] = node_info
        return template

    def fill_entry_template(self, template: dict, node: str) -> dict:
        """
        Fill in a blank JSONLD template with information for each node.
        All relationships are filled from the graph, based on the type of information
          (node or edge)

        Args:
            template, dict: empty class or property template to be filled with
              information for the given node.
            node, str: target node to fill the template out for.
        Returns:
            template, dict: filled class or property template, that has been
              processed and cleaned up.
        """
        data_model_relationships = self.dmr.relationships_dictionary

        # For each field in template fill out with information from the graph
        for rel_vals in data_model_relationships.values():
            # Fill in the JSONLD template for this node, with data from the graph by looking
            # up the nodes edge relationships, and the value information attached to the node.

            # Fill edge information (done per edge type)
            if rel_vals["edge_rel"]:
                template = self.add_edge_rels_to_template(
                    template=template, rel_vals=rel_vals, node=node
                )

            # Fill in node value information
            else:
                template = self.add_node_info_to_template(
                    template=template, rel_vals=rel_vals, node=node
                )

        # Clean up template
        template = self.clean_template(
            template=template,
            data_model_relationships=data_model_relationships,
        )

        # Reorder lists based on weights:
        template = self.reorder_template_entries(
            template=template,
        )

        # Add contexts to certain values
        template = self.add_contexts_to_entries(
            template=template,
        )

        return template

    def add_contexts_to_entries(self, template: dict) -> dict:
        """
        Args:
            template, dict: JSONLD template that has been filled up to
              the current node, with information
        Returns:
            template, dict: JSONLD template where contexts have been added back to certain values.
        Note: This will likely need to be modified when Contexts are truly added to the model
        """
        # pylint:disable=comparison-with-callable
        for jsonld_key in template.keys():
            # Retrieve the relationships key using the jsonld_key
            rel_key = []

            for rel, rel_vals in self.rel_dict.items():
                if "jsonld_key" in rel_vals and jsonld_key == rel_vals["jsonld_key"]:
                    rel_key.append(rel)

            if rel_key:
                rel_key = rel_key[0]
                # If the current relationship can be defined with a 'node_attr_dict'
                if "node_attr_dict" in self.rel_dict[rel_key].keys():
                    try:
                        # if possible pull standard function to get node information
                        rel_func = self.rel_dict[rel_key]["node_attr_dict"]["standard"]
                    except:  # pylint:disable=bare-except
                        # if not pull default function to get node information
                        rel_func = self.rel_dict[rel_key]["node_attr_dict"]["default"]

                    # Add appropritae contexts that have been removed in previous steps
                    # (for JSONLD) or did not exist to begin with (csv)
                    if (
                        rel_key == "id"
                        and rel_func == get_label_from_display_name
                        and "bts" not in str(template[jsonld_key]).lower()
                    ):
                        template[jsonld_key] = "bts:" + template[jsonld_key]
                    elif (
                        rel_key == "required"
                        and rel_func == convert_bool_to_str
                        and "sms" not in str(template[jsonld_key]).lower()
                    ):
                        template[jsonld_key] = (
                            "sms:" + str(template[jsonld_key]).lower()
                        )

        return template

    def clean_template(self, template: dict, data_model_relationships: dict) -> dict:
        """
        Get rid of empty k:v pairs. Fill with a default if specified in the
          relationships dictionary.

        Args:
            template, dict: JSONLD template for a single entry, keys specified in property
              and class templates.
            data_model_relationships, dict: dictionary containing information for each
              relationship type supported.
        Returns:
            template: JSONLD template where unfilled entries have been removed,
              or filled with default depending on specifications in the relationships dictionary.
        """
        for rels in data_model_relationships.values():
            # Get the current relationships, jsonld key
            relationship_jsonld_key = rels["jsonld_key"]
            # Check if the relationship_relationship_key is part of the template,
            # and if it is, look to see if it has an entry
            if (
                relationship_jsonld_key in template.keys()
                and not template[rels["jsonld_key"]]
            ):
                # If there is no value recorded, fill out the template with the
                # default relationship value (if recorded.)
                if "jsonld_default" in rels.keys():
                    template[relationship_jsonld_key] = rels["jsonld_default"]
                else:
                    # If there is no default specified in the relationships dictionary,
                    # delete the empty value from the template.
                    del template[relationship_jsonld_key]
        return template

    def reorder_template_entries(self, template: dict) -> dict:
        """
        In JSONLD some classes or property keys have list values.
        We want to make sure these lists are ordered according to the order supplied by the user.
        This will look specically in lists and reorder those.

        Args:
            template, dict: JSONLD template for a single entry, keys specified in
              property and class templates.
        Returns:
            template, dict: list entries re-ordered to match user supplied order.
        Note:
            User order only matters for nodes that are also attributes
        """
        template_label = template["rdfs:label"]

        for jsonld_key, entry in template.items():
            # Make sure dealing with an edge relationship:
            is_edge = [
                "True"
                for rel_vals in self.rel_dict.values()
                if rel_vals["jsonld_key"] == jsonld_key
                if rel_vals["edge_rel"]
            ]

            # if the entry is of type list and theres more than one value in the
            # list attempt to reorder
            if is_edge and isinstance(entry, list) and len(entry) > 1:
                # Get edge key from data_model_relationships using the jsonld_key:
                key, _ = [
                    (rel_key, rel_vals["edge_key"])
                    for rel_key, rel_vals in self.rel_dict.items()
                    if jsonld_key == rel_vals["jsonld_key"]
                ][0]

                # Order edges
                sorted_edges = self.dmge.get_ordered_entry(
                    key=key, source_node_label=template_label
                )
                if not len(entry) == len(sorted_edges):
                    logger.error(
                        (
                            "There is an error with sorting values in the JSONLD, "
                            "please issue a bug report."
                        )
                    )

                edge_weights_dict = {edge: i for i, edge in enumerate(sorted_edges)}
                ordered_edges: list[Union[int, dict]] = [0] * len(
                    edge_weights_dict.keys()
                )
                for edge, normalized_weight in edge_weights_dict.items():
                    ordered_edges[normalized_weight] = {"@id": "bts:" + edge}

                # Throw an error if ordered_edges does not get fully filled as expected.
                if 0 in ordered_edges:
                    logger.error(
                        (
                            "There was an issue getting values to match order specified in "
                            "the data model, please submit a help request."
                        )
                    )
                template[jsonld_key] = ordered_edges
        return template

    def generate_jsonld_object(self) -> dict:
        """Create the JSONLD object.
        Returns:
            jsonld_object, dict: JSONLD object containing all nodes and related information
        """
        # Get properties.
        properties = self.dmge.find_properties()

        # Get JSONLD Template
        json_ld_template = self.base_jsonld_template

        # Iterativly add graph nodes to json_ld_template as properties or classes
        for node in self.graph.nodes:
            if node in properties:
                # Get property template
                property_template = copy.deepcopy(self.property_template)
                obj = self.fill_entry_template(template=property_template, node=node)
            else:
                # Get class template
                class_template = copy.deepcopy(self.class_template)
                obj = self.fill_entry_template(template=class_template, node=node)
            json_ld_template["@graph"].append(obj)
        return json_ld_template


def convert_graph_to_jsonld(graph: nx.MultiDiGraph) -> dict:
    """convert graph to jsonld"""
    # Make the JSONLD object
    data_model_jsonld_converter = DataModelJsonLD(graph=graph)
    jsonld_dm = data_model_jsonld_converter.generate_jsonld_object()
    return jsonld_dm
