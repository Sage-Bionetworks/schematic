import os
import string
import json
import logging

from typing import Any, Dict, Optional, Text, List

import inflection
import networkx as nx

from rdflib import Graph, Namespace, plugin, query
from networkx.algorithms.cycles import find_cycle
from networkx.readwrite import json_graph

from schematic.utils.curie_utils import (
    expand_curies_in_schema,
    uri2label,
    extract_name_from_uri_or_curie,
)
from schematic.utils.general import find_duplicates
from schematic.utils.io_utils import load_default, load_json, load_schemaorg
from schematic.utils.schema_utils import (
    load_schema_into_networkx,
    node_attrs_cleanup,
    class_to_node,
    relationship_edges,
)
from schematic.utils.general import dict2list, unlist
from schematic.utils.viz_utils import visualize
from schematic.utils.validate_utils import (
    validate_class_schema,
    validate_property_schema,
    validate_schema,
)
from schematic.schemas.curie import uri2curie, curie2uri

namespaces = dict(rdf=Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))


logger = logging.getLogger(__name__)


class SchemaExplorer:
    """Class for exploring schema"""

    def __init__(self):
        self.load_default_schema()

    def load_schema(self, schema):
        """Load schema and convert it to networkx graph"""
        self.schema = load_json(schema)
        self.schema_nx = load_schema_into_networkx(self.schema)

    def export_schema(self, file_path):
        with open(file_path, "w") as f:
            json.dump(self.schema, f, sort_keys=True, indent=4, ensure_ascii=False)

    def load_default_schema(self):
        """Load default schema, either schema.org or biothings"""
        self.schema = load_default()
        self.schema_nx = load_schema_into_networkx(self.schema)

    def get_nx_schema(self):
        return self.schema_nx

    def get_edges_by_relationship(
        self, class_label: str, relationship: str
    ) -> List[str]:
        """Get a list of out-edges of a node where the edges match a specifc type of relationship.

        i.e., the edges connecting a node to its neighbors are of relationship type -- "parentOf" (set of edges to children / sub-class nodes).
        Note: possible edge relationships are -- parentOf, rangeValue, requiresDependency.

        Args:
            node: the node whose edges we need to look at.
            relationship: the type of link(s) that the above node and its immediate neighbors share.

        Returns:
            List of edges that are connected to the node.
        """
        edges = []

        mm_graph = self.get_nx_schema()

        for (u, v, key, c) in mm_graph.out_edges(node, data=True, keys=True):
            if key == relationship:
                edges.append((u, v))

        return edges

    def get_descendants_by_edge_type(
        self,
        source_node: str,
        relationship: str,
        connected: bool = True,
        ordered: bool = False,
    ) -> List[str]:
        """Get all nodes that are descendants of a given source node, based on a specific type of edge / relationship type.

        Args:
            source_node: The node whose descendants need to be retreived.
            relationship: Edge / link relationship type with possible values same as in above docs.
            connected: If True, we need to ensure that all descendant nodes are reachable from the source node, i.e., they are part of the same connected component.
                       If False, the descendants could be in multiple connected components.
                       Default value is True.
            ordered: If True, the list of descendants will be topologically ordered.
                     If False, the list has no particular order (depends on the order in which the descendats were traversed in the subgraph).

        Returns:
            List of nodes that are descendants from a particular node (sorted / unsorted)
        """
        mm_graph = self.get_nx_schema()

        # if mm_graph.has_node(source_node):
        # get all nodes that are reachable from a specified root /source node in the data model

        root_descendants = nx.descendants(mm_graph, source_node)
        # else:
        # print("The specified source node could not be found im the Networkx graph.")
        # return []

        subgraph_nodes = list(root_descendants)
        subgraph_nodes.append(source_node)
        descendants_subgraph = mm_graph.subgraph(subgraph_nodes)

        # prune the descendants subgraph so as to include only those edges that match the relationship type
        rel_edges = []
        for (u, v, key, c) in descendants_subgraph.edges(data=True, keys=True):
            if key == relationship:
                rel_edges.append((u, v))

        relationship_subgraph = nx.DiGraph()
        relationship_subgraph.add_edges_from(rel_edges)

        descendants = relationship_subgraph.nodes()

        if not descendants:
            # return empty list if there are no nodes that are reachable from the source node based on this relationship type
            return []

        if connected and ordered:
            # get the set of reachable nodes from the source node
            descendants = nx.descendants(relationship_subgraph, source_node)
            descendants.add(source_node)

            # normally, the descendants from a node are unordered (peculiarity of nx descendants call)
            # form the subgraph on descendants and order it topologically
            # this assumes an acyclic subgraph
            descendants = nx.topological_sort(
                relationship_subgraph.subgraph(descendants)
            )
        elif connected:
            # get the nodes that are reachable from a given source node
            # after the pruning process above some nodes in the root_descendants subgraph might have become disconnected and will be omitted
            descendants = nx.descendants(relationship_subgraph, source_node)
            descendants.add(source_node)
        elif ordered:
            # sort the nodes topologically
            # this requires the graph to be an acyclic graph
            descendants = nx.topological_sort(relationship_subgraph)

        return list(descendants)

    def get_adjacent_nodes_by_relationship(
        self, node: str, relationship: str
    ) -> List[str]:
        """Get a list of nodes that is / are adjacent to a given node, based on a relationship type.

        Args:
            node: the node whose edges we need to look at.
            relationship: the type of link(s) that the above node and its immediate neighbors share.

        Returns:
            List of nodes that are adjacent to the given node.
        """
        nodes = set()

        mm_graph = self.get_nx_schema()

        for (u, v, key, c) in mm_graph.out_edges(node, data=True, keys=True):
            if key == relationship:
                nodes.add(v)

        return list(nodes)

    def is_class_in_schema(self, class_label):
        if self.schema_nx.nodes[class_label]:
            return True
        else:
            return False

    def full_schema_graph(self, size=None):
        edges = self.schema_nx.edges()
        return visualize(edges, size=size)

    def sub_schema_graph(self, source, direction, size=None):
        if direction == "down":
            edges = list(nx.edge_bfs(self.schema_nx, [source]))
            return visualize(edges, size=size)
        elif direction == "up":
            paths = self.find_parent_classes(source)
            edges = []
            for _path in paths:
                _path.append(source)
                for i in range(0, len(_path) - 1):
                    edges.append((_path[i], _path[i + 1]))
            return visualize(edges, size=size)
        elif direction == "both":
            paths = self.find_parent_classes(source)
            edges = list(nx.edge_bfs(self.schema_nx, [source]))
            for _path in paths:
                _path.append(source)
                for i in range(0, len(_path) - 1):
                    edges.append((_path[i], _path[i + 1]))
            return visualize(edges, size=size)

    def find_parent_classes(self, schema_class):
        """Find all parents of the class"""

        digraph = self.get_digraph_by_edge_type("parentOf")

        root_node = list(nx.topological_sort(digraph))[0]
        # root_node = list(nx.topological_sort(self.schema_nx))[0]

        paths = nx.all_simple_paths(
            self.schema_nx, source=root_node, target=schema_class
        )
        # print(root_node)
        return [_path[:-1] for _path in paths]

    def find_class_specific_properties(self, schema_class):
        """Find properties specifically associated with a given class"""
        schema_uri = self.schema_nx.nodes[schema_class]["uri"]
        properties = []
        for record in self.schema["@graph"]:
            if record["@type"] == "rdf:Property":
                if (
                    type(record["schema:domainIncludes"]) == dict
                    and record["schema:domainIncludes"]["@id"] == schema_uri
                ):
                    properties.append(record["rdfs:label"])
                elif (
                    type(record["schema:domainIncludes"]) == list
                    and [
                        item
                        for item in record["schema:domainIncludes"]
                        if item["@id"] == schema_uri
                    ]
                    != []
                ):
                    properties.append(record["rdfs:label"])
        return properties

    def find_all_class_properties(self, schema_class, display_as_table=False):
        """Find all properties associated with a given class
        # TODO : need to deal with recursive paths
        """
        parents = self.find_parent_classes(schema_class)
        # print(schema_class)
        # print(parents)
        properties = [
            {
                "class": schema_class,
                "properties": self.find_class_specific_properties(schema_class),
            }
        ]
        for path in parents:
            path.reverse()
            for _parent in path:
                # print(_parent)
                properties.append(
                    {
                        "class": _parent,
                        "properties": self.find_class_specific_properties(_parent),
                    }
                )
        if not display_as_table:
            return properties
        else:
            content = [["Property", "Expected Type", "Description", "Class"]]
            for record in properties:
                for _property in record["properties"]:
                    property_info = self.explore_property(_property)
                    if "range" in property_info:
                        content.append(
                            [
                                _property,
                                property_info["range"],
                                property_info["description"],
                                record["class"],
                            ]
                        )
                    else:
                        content.append(
                            [_property, property_info["description"], record["class"]]
                        )

            # TODO: Log content

    def find_class_usages(self, schema_class):
        """Find where a given class is used as a value of a property"""
        usages = []
        schema_uri = self.schema_nx.nodes[schema_class]["uri"]
        for record in self.schema["@graph"]:
            usage = {}
            if record["@type"] == "rdf:Property":
                if "schema:rangeIncludes" in record:
                    p_range = dict2list(record["schema:rangeIncludes"])
                    for _doc in p_range:
                        if _doc["@id"] == schema_uri:
                            usage["property"] = record["rdfs:label"]
                            p_domain = dict2list(record["schema:domainIncludes"])
                            usage["property_used_on_class"] = unlist(
                                [self.uri2label(record["@id"]) for record in p_domain]
                            )
                            usage["description"] = record["rdfs:comment"]
            if usage:
                usages.append(usage)
        return usages

    def find_child_classes(self, schema_class):
        """Find schema classes that inherit from the given class"""
        return unlist(list(self.schema_nx.successors(schema_class)))

    def find_adjacent_child_classes(self, schema_class):

        return self.get_adjacent_nodes_by_relationship(schema_class, "parentOf")

    def explore_class(self, schema_class):
        """Find details about a specific schema class"""
        parents = []
        if "subClassOf" in self.schema_nx.nodes[schema_class]:
            schema_node_val = self.schema_nx.nodes[schema_class]["subClassOf"]

            parents_list = []
            if isinstance(schema_node_val, dict):
                parents_list.append(self.schema_nx.nodes[schema_class]["subClassOf"])
            else:
                parents_list = schema_node_val

            for parent in parents_list:
                parents.append(extract_name_from_uri_or_curie(parent["@id"]))

        requires_range = []
        if "rangeIncludes" in self.schema_nx.nodes[schema_class]:
            schema_node_val = self.schema_nx.nodes[schema_class]["rangeIncludes"]

            if isinstance(schema_node_val, dict):
                subclass_list = []
                subclass_list.append(
                    self.schema_nx.nodes[schema_class]["rangeIncludes"]
                )
            else:
                subclass_list = schema_node_val

            for range_class in subclass_list:
                requires_range.append(
                    extract_name_from_uri_or_curie(range_class["@id"])
                )

        requires_dependencies = []
        if "requiresDependency" in self.schema_nx.nodes[schema_class]:
            schema_node_val = self.schema_nx.nodes[schema_class]["requiresDependency"]

            if isinstance(schema_node_val, dict):
                subclass_list = []
                subclass_list.append(
                    self.schema_nx.nodes[schema_class]["requiresDependency"]
                )
            else:
                subclass_list = schema_node_val

            for dep_class in subclass_list:
                requires_dependencies.append(
                    extract_name_from_uri_or_curie(dep_class["@id"])
                )

        requires_components = []
        if "requiresComponent" in self.schema_nx.nodes[schema_class]:
            schema_node_val = self.schema_nx.nodes[schema_class]["requiresComponent"]

            if isinstance(schema_node_val, dict):
                subclass_list = []
                subclass_list.append(
                    self.schema_nx.nodes[schema_class]["requiresComponent"]
                )
            else:
                subclass_list = schema_node_val

            for comp_dep_class in subclass_list:
                requires_components.append(
                    extract_name_from_uri_or_curie(comp_dep_class["@id"])
                )

        required = False
        if "required" in self.schema_nx.nodes[schema_class]:
            required = self.schema_nx.nodes[schema_class]["required"]

        validation_rules = []
        if "validationRules" in self.schema_nx.nodes[schema_class]:
            validation_rules = self.schema_nx.nodes[schema_class]["validationRules"]

        # TODO: make class_info keys here the same as keys in schema graph nodes(e.g. schema_class above); note that downstream code using explore_class would have to be updated as well (e.g. csv_2_schemaorg)

        class_info = {
            "properties": self.find_class_specific_properties(schema_class),
            "description": self.schema_nx.nodes[schema_class]["description"],
            "uri": curie2uri(self.schema_nx.nodes[schema_class]["uri"], namespaces),
            #'usage': self.find_class_usages(schema_class),
            "usage": "NA",
            "child_classes": self.find_adjacent_child_classes(schema_class),
            "subClassOf": parents,
            "range": requires_range,
            "dependencies": requires_dependencies,
            "validation_rules": validation_rules,
            "required": required,
            "component_dependencies": requires_components,
            "parent_classes": parents
            #'parent_classes': self.find_parent_classes(schema_class)
        }

        if "displayName" in self.schema_nx.nodes[schema_class]:
            class_info["displayName"] = self.schema_nx.nodes[schema_class][
                "displayName"
            ]

        return class_info

    def get_property_label_from_display_name(self, display_name):
        """Convert a given display name string into a proper property label string"""
        """
        label = ''.join(x.capitalize() or ' ' for x in display_name.split(' '))
        label = label[:1].lower() + label[1:] if label else ''
        """
        display_name = display_name.translate({ord(c): None for c in string.whitespace})

        label = inflection.camelize(display_name.strip(), uppercase_first_letter=False)
        return label

    def get_class_label_from_display_name(self, display_name):
        """Convert a given display name string into a proper class label string"""
        """
        label = ''.join(x.capitalize() or ' ' for x in display_name.split(' '))"""
        display_name = display_name.translate({ord(c): None for c in string.whitespace})
        label = inflection.camelize(display_name.strip(), uppercase_first_letter=True)

        return label

    def get_class_by_property(self, property_display_name):
        schema_property = self.get_property_label_from_display_name(
            property_display_name
        )

        for record in self.schema["@graph"]:
            if record["@type"] == "rdf:Property":
                if record["rdfs:label"] == schema_property:
                    p_domain = dict2list(record["schema:domainIncludes"])
                    return unlist(
                        [
                            self.uri2label(schema_class["@id"])
                            for schema_class in p_domain
                        ]
                    )

        return None

    def uri2label(self, uri):
        return uri.split(":")[1]

    def explore_property(self, schema_property):
        """Find details about a specific property
        TODO: refactor so that explore class and explore property reuse logic - they are *very* similar
        """
        property_info = {}
        for record in self.schema["@graph"]:
            if record["@type"] == "rdf:Property":
                if record["rdfs:label"] == schema_property:
                    property_info["id"] = record["rdfs:label"]
                    property_info["description"] = record["rdfs:comment"]
                    property_info["uri"] = curie2uri(record["@id"], namespaces)

                    p_domain = dict2list(record["schema:domainIncludes"])
                    property_info["domain"] = unlist(
                        [self.uri2label(record["@id"]) for record in p_domain]
                    )
                    if "schema:rangeIncludes" in record:
                        p_range = dict2list(record["schema:rangeIncludes"])
                        property_info["range"] = [
                            self.uri2label(record["@id"]) for record in p_range
                        ]
                    else:
                        property_info["range"] = []

                    if "sms:required" in record:
                        if "sms:true" == record["sms:required"]:
                            property_info["required"] = True
                        else:
                            property_info["required"] = False

                    validation_rules = []
                    if "sms:validationRules" in record:
                        property_info["validation_rules"] = record[
                            "sms:validationRules"
                        ]

                    if "sms:requiresDependency" in record:
                        p_dependencies = dict2list(record["sms:requiresDependency"])
                        property_info["dependencies"] = [
                            self.uri2label(record["@id"]) for record in p_dependencies
                        ]
                    else:
                        property_info["dependencies"] = []

                    if "sms:displayName" in record:
                        property_info["displayName"] = record["sms:displayName"]

                    break

        # check if properties are added multiple times

        return property_info

    def generate_class_template(self):
        """Generate a template for schema class"""
        template = {
            "@id": "uri or curie of the class",
            "@type": "rdfs:Class",
            "rdfs:comment": "description of the class",
            "rdfs:label": "class label, should match @id",
            "rdfs:subClassOf": {"@id": "parent class, could be list"},
            "schema:isPartOf": {"@id": "http://schema.biothings.io"},
        }
        return template

    def generate_property_template(self):
        """Generate a template for schema property"""
        template = {
            "@id": "url or curie of the property",
            "@type": "rdf:Property",
            "rdfs:comment": "description of the property",
            "rdfs:label": "carmel case, should match @id",
            "schema:domainIncludes": {
                "@id": "class which use it as a property, could be list"
            },
            "schema:isPartOf": {"@id": "http://schema.biothings.io"},
            "schema:rangeIncludes": {
                "@id": "relates a property to a class that constitutes (one of) the expected type(s) for values of the property"
            },
        }
        return template

    def edit_class(self, class_info):
        """Edit an existing class into schema"""
        for i, schema_class in enumerate(self.schema["@graph"]):
            if schema_class["rdfs:label"] == class_info["rdfs:label"]:
                validate_class_schema(class_info)  # why are we doing this in a loop?

                self.schema["@graph"][i] = class_info
                break

        # TODO: do we actually need to validate the entire schema if a class is just edited and the class passes validation?
        # validate_schema(self.schema)

        logger.info(f"Edited the class {class_info['rdfs:label']} successfully.")
        self.schema_nx = load_schema_into_networkx(self.schema)

    def update_class(self, class_info):
        """Add a new class into schema"""
        # print(class_info)
        validate_class_schema(class_info)
        self.schema["@graph"].append(class_info)
        validate_schema(self.schema)
        logger.info(f"Updated the class {class_info['rdfs:label']} successfully.")
        self.schema_nx = load_schema_into_networkx(self.schema)

    def edit_property(self, property_info):
        """Edit an existing property into schema"""
        for i, schema_property in enumerate(self.schema["@graph"]):
            if schema_property["rdfs:label"] == property_info["rdfs:label"]:
                validate_property_schema(property_info)
                self.schema["@graph"][i] = property_info

                # TODO: check if properties are added/edited multiple times (e.g. look at explore_property)
                break

        validate_schema(self.schema)
        logger.info(f"Edited the property {property_info['rdfs:label']} successfully.")
        self.schema_nx = load_schema_into_networkx(self.schema)

    def update_property(self, property_info):
        """Add a new property into schema"""
        validate_property_schema(property_info)
        self.schema["@graph"].append(property_info)
        validate_schema(self.schema)
        logger.info(f"Updated the property {property_info['rdfs:label']} successfully.")

    def get_digraph_by_edge_type(self, edge_type):

        multi_digraph = self.schema_nx

        digraph = nx.DiGraph()
        for (u, v, key, c) in multi_digraph.edges(data=True, keys=True):
            if key == edge_type:
                digraph.add_edge(u, v)

        # print(nx.find_cycle(digraph, orientation = "ignore"))

        return digraph

    # version of edit_class() method that directly acts on the networkx graph
    def edit_schema_object_nx(self, schema_object: dict) -> None:
        node_to_replace = class_to_node(class_to_convert=schema_object)

        # get the networkx graph associated with the SchemaExplorer object in its current state
        schema_graph_nx = self.get_nx_schema()

        # outer loop to loop over all the nodes in the graph constructed from master schema
        for node, data in schema_graph_nx.nodes(data=True):

            # innner loop to loop over the single node that is to be replaced/edited in the master graph
            for replace_node, replace_data in node_to_replace.nodes(data=True):

                # find the node to be replaced in the graph
                if node == replace_node:

                    # for the "comment", "required", "displayName", "validationRules" fields/keys it's okay to do a direct replacement
                    # without having to worry about adding/removing any associated edges

                    # ques. is it more expensive to do a checking operation (diff b/w fields) or a replace operation?

                    if (
                        "comment" in data and "comment" in replace_data
                    ):  # replace contents of "comment" from replacement node
                        schema_graph_nx.nodes[node]["comment"] = node_to_replace.nodes[
                            replace_node
                        ]["comment"]
                        schema_graph_nx.nodes[node][
                            "description"
                        ] = node_to_replace.nodes[replace_node]["description"]

                    if (
                        "required" in data and "required" in replace_data
                    ):  # replace boolean value of "required" from replacement node
                        schema_graph_nx.nodes[node]["required"] = node_to_replace.nodes[
                            replace_node
                        ]["required"]

                    if (
                        "displayName" in data and "displayName" in replace_data
                    ):  # replace contents of "displayName" from replacement node
                        schema_graph_nx.nodes[node][
                            "displayName"
                        ] = node_to_replace.nodes[replace_node]["displayName"]

                    if (
                        "validationRules" in data and "validationRules" in replace_data
                    ):  # replace contents of "validationRules" from replacement node
                        schema_graph_nx.nodes[node][
                            "validationRules"
                        ] = node_to_replace.nodes[replace_node]["validationRules"]

                    # for the "subClassOf", "requiresDependency", "requiresComponent", "rangeIncludes" fields/keys require rejiggering
                    # of associated edges
                    # general strategy we follow for rejiggering is remove edges that existed formerly and add new edges based on contents
                    # of the replacement node

                    # "subClassOf" key related edge manipulation
                    if "subClassOf" in replace_data:

                        # if the "subClassOf" attribute already exists on the node, then remove all the "parentOf" in-edges
                        # associated with that node
                        if "subClassOf" in data:
                            # remove formerly existent edges from the master schema/graph
                            for (u, v) in list(schema_graph_nx.in_edges([node])):

                                # there are certain nodes which have "subClassOf" data in list format
                                if type(data["subClassOf"]) == list:
                                    for _edges_to_replace in data["subClassOf"]:
                                        edge_repl = extract_name_from_uri_or_curie(
                                            _edges_to_replace["@id"]
                                        )

                                        if edge_repl == u:

                                            try:
                                                # we need to make sure to remove only edges that are tagged with the "parentOf" label
                                                schema_graph_nx.remove_edges_from(
                                                    [(u, v, "parentOf")]
                                                )
                                            except TypeError:
                                                pass

                                # there are certain nodes which have "subClassOf" data in dict format
                                elif type(data["subClassOf"]) == dict:
                                    for k_id, v_curie in data["subClassOf"].items():
                                        edge_repl = extract_name_from_uri_or_curie(
                                            v_curie
                                        )

                                        if edge_repl == u:

                                            try:
                                                schema_graph_nx.remove_edges_from(
                                                    [(u, v, "parentOf")]
                                                )
                                            except TypeError:
                                                pass

                        # extract node names from replacement node and use it to add edges to the master schema/graph
                        parents = replace_data["subClassOf"]
                        if type(parents) == list:
                            for _parent in parents:
                                target_node = extract_name_from_uri_or_curie(
                                    _parent["@id"]
                                )

                                # label to be associated with "subClassOf" keys is "parentOf"
                                if target_node != replace_node:

                                    # make note of the fact that we are changing in-edges here
                                    schema_graph_nx.add_edge(
                                        target_node, replace_node, key="parentOf"
                                    )
                        elif type(parents) == dict:
                            for _k_parent, _v_parent in parents.items():
                                target_node = extract_name_from_uri_or_curie(_v_parent)

                                # label to be associated with "subClassOf" keys is "parentOf"
                                if target_node != replace_node:

                                    # make note of the fact that we are changing in-edges here
                                    schema_graph_nx.add_edge(
                                        target_node, replace_node, key="parentOf"
                                    )

                        # once the edges have been added, change the contents of the node
                        schema_graph_nx.nodes[node][
                            "subClassOf"
                        ] = node_to_replace.nodes[replace_node]["subClassOf"]

                    # "requiresDependency" key related edge manipulation
                    if "requiresDependency" in replace_data:

                        # if the "requiresDependency" attribute already exists on the node, then remove all the "requiresDependency" in-edges
                        # associated with that node
                        if "requiresDependency" in data:

                            for (u, v) in list(schema_graph_nx.out_edges([node])):
                                # there are certain nodes which have "requiresDependency" data in list format
                                if type(data["requiresDependency"]) == list:
                                    for _edges_to_replace in data["requiresDependency"]:
                                        edge_repl = extract_name_from_uri_or_curie(
                                            _edges_to_replace["@id"]
                                        )

                                        if edge_repl == v:

                                            try:
                                                schema_graph_nx.remove_edges_from(
                                                    [u, v, "requiresDependency"]
                                                )
                                            except TypeError:
                                                pass

                                # there are certain nodes which have "requiresDependency" data in dict format
                                elif type(data["requiresDependency"]) == dict:
                                    for k_id, v_curie in data[
                                        "requiresDependency"
                                    ].items():
                                        edge_repl = extract_name_from_uri_or_curie(
                                            v_curie
                                        )

                                        if edge_repl == u:

                                            try:
                                                schema_graph_nx.remove_edges_from(
                                                    [u, v, "requiresDependency"]
                                                )
                                            except TypeError:
                                                pass

                            deps = replace_data["requiresDependency"]
                            if type(deps) == list:
                                for _dep in deps:
                                    target_node = extract_name_from_uri_or_curie(
                                        _dep["@id"]
                                    )

                                    if target_node != replace_node:

                                        # make not of the fact that edges being added here are out-edges
                                        schema_graph_nx.add_edge(
                                            replace_node,
                                            target_node,
                                            key="requiresDependency",
                                        )
                            elif type(deps) == dict:
                                for _k_dep, _v_dep in deps.items():
                                    target_node = extract_name_from_uri_or_curie(_v_dep)

                                    if target_node != replace_node:

                                        # make not of the fact that edges being added here are out-edges
                                        schema_graph_nx.add_edge(
                                            replace_node,
                                            target_node,
                                            key="requiresDependency",
                                        )

                        schema_graph_nx.nodes[node][
                            "requiresDependency"
                        ] = node_to_replace.nodes[replace_node]["requiresDependency"]

                    # "requiresComponent" key related edge manipulation
                    if "requiresComponent" in replace_data:

                        if "requiresComponent" in data:
                            for (u, v) in list(schema_graph_nx.out_edges([node])):
                                # there are certain nodes which have "requiresComponent" data in list format
                                if type(data["requiresComponent"]) == list:
                                    for _edges_to_replace in data["requiresComponent"]:
                                        edge_repl = extract_name_from_uri_or_curie(
                                            _edges_to_replace["@id"]
                                        )

                                        if edge_repl == v:

                                            try:
                                                schema_graph_nx.remove_edges_from(
                                                    [u, v, "requiresComponent"]
                                                )
                                            except TypeError:
                                                pass

                                elif type(data["requiresComponent"]) == dict:
                                    for k_id, v_curie in data[
                                        "requiresComponent"
                                    ].items():
                                        edge_repl = extract_name_from_uri_or_curie(
                                            v_curie
                                        )

                                        if edge_repl == v:

                                            try:
                                                schema_graph_nx.remove_edges_from(
                                                    [u, v, "requiresComponent"]
                                                )
                                            except TypeError:
                                                pass

                        comps = replace_data["requiresComponent"]
                        if type(comps) == list:
                            for _comp in comps:
                                target_node = extract_name_from_uri_or_curie(
                                    _comp["@id"]
                                )

                                if target_node != replace_node:
                                    schema_graph_nx.add_edge(
                                        replace_node,
                                        target_node,
                                        key="requiresComponent",
                                    )
                        elif type(comps) == dict:
                            for _k_comp, _v_comp in deps.items():
                                target_node = extract_name_from_uri_or_curie(_v_comp)

                                if target_node != replace_node:

                                    # make not of the fact that edges being added here are out-edges
                                    schema_graph_nx.add_edge(
                                        replace_node,
                                        target_node,
                                        key="requiresDependency",
                                    )

                        schema_graph_nx.nodes[node][
                            "requiresComponent"
                        ] = node_to_replace.nodes[replace_node]["requiresComponent"]

                    # "rangeIncludes" key related edge manipulation
                    if "rangeIncludes" in replace_data:

                        if "rangeIncludes" in data:
                            for (u, v) in list(schema_graph_nx.out_edges([node])):
                                # there are certain nodes which have "rangeIncludes" data in list format
                                if type(data["rangeIncludes"]) == list:
                                    for _edges_to_replace in data["rangeIncludes"]:
                                        edge_repl = extract_name_from_uri_or_curie(
                                            _edges_to_replace["@id"]
                                        )

                                        if edge_repl == v:
                                            try:
                                                schema_graph_nx.remove_edges_from(
                                                    [u, v, "rangeIncludes"]
                                                )
                                            except TypeError:
                                                pass

                                elif type(data["rangeIncludes"]) == dict:
                                    for k_id, v_curie in data["rangeIncludes"].items():
                                        edge_repl = extract_name_from_uri_or_curie(
                                            v_curie
                                        )

                                        if edge_repl == v:
                                            try:
                                                schema_graph_nx.remove_edges_from(
                                                    [u, v, "rangeIncludes"]
                                                )
                                            except TypeError:
                                                pass

                        range_inc = replace_data["rangeIncludes"]
                        if type(range_inc) == list:
                            for _rinc in range_inc:
                                target_node = extract_name_from_uri_or_curie(
                                    _rinc["@id"]
                                )

                                if target_node != replace_node:
                                    schema_graph_nx.add_edge(
                                        replace_node, target_node, key="rangeValue"
                                    )
                        elif type(range_inc) == dict:
                            for _k_rinc, _v_rinc in deps.items():
                                target_node = extract_name_from_uri_or_curie(_v_rinc)

                                if target_node != replace_node:

                                    # make not of the fact that edges being added here are out-edges
                                    schema_graph_nx.add_edge(
                                        replace_node, target_node, key="rangeValue"
                                    )

                        schema_graph_nx.nodes[node][
                            "rangeIncludes"
                        ] = node_to_replace.nodes[replace_node]["rangeIncludes"]

        # set the networkx schema graph to the the modified networkx schema
        self.schema_nx = schema_graph_nx

        # print("Added node {} to the graph successfully.".format(schema_object["rdfs:label"]))

        # part of the code that replaces the modified class in the original JSON-LD schema (not in the data/ folder though)
        for i, schema_class in enumerate(self.schema["@graph"]):
            if schema_class["rdfs:label"] == schema_object["rdfs:label"]:
                # validate_class_schema(schema_object)    # validate that the class to be modified follows the structure for any generic class (node)

                self.schema["@graph"][i] = schema_object
                break

    # version of update_class() method that directly acts on the networkx graph
    def add_schema_object_nx(self, schema_object: dict, **kwargs: dict) -> None:
        node = node_attrs_cleanup(schema_object)

        if "required" in node:
            if "sms:true" == schema_object["sms:required"]:
                node["required"] = True
            else:
                node["required"] = False

        if "sms:validationRules" in schema_object:
            node["validationRules"] = schema_object["sms:validationRules"]
        else:
            node["validationRules"] = []

        node["uri"] = schema_object["@id"]
        node["description"] = schema_object["rdfs:comment"]

        # get the networkx graph associated with the SchemaExplorer object in its current state
        schema_graph_nx = self.get_nx_schema()

        # add node to graph
        schema_graph_nx.add_node(schema_object["rdfs:label"], **node)

        schema_graph_nx = relationship_edges(schema_graph_nx, schema_object, **kwargs)

        # set the networkx schema graph to the the modified networkx schema
        self.schema_nx = schema_graph_nx

        # print("Edited node {} successfully.".format(schema_object["rdfs:label"]))

        # update the JSON-LD schema after modifying the networkx graph
        # validate_class_schema(schema_object)
        self.schema["@graph"].append(schema_object)
        # validate_schema(self.schema)
