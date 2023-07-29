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

from schematic.schemas.data_model_edges import DataModelEdges
from schematic.schemas.data_model_nodes import DataModelNodes 

from schematic.schemas.data_model_relationships import (
    DataModelRelationships
    )

from schematic.utils.curie_utils import (
    expand_curies_in_schema,
    uri2label,
    extract_name_from_uri_or_curie,
)
from schematic.utils.general import find_duplicates

from schematic.utils.io_utils import load_default, load_json, load_schemaorg
from schematic.utils.schema_util import get_property_label_from_display_name, get_class_label_from_display_name
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



class DataModelGraphMeta(object):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class DataModelGraph():
    '''
    Generate graph network (networkx) from the attributes and relationships returned
    fromt he data model parser.

    Create a singleton.
    '''
    __metaclass__ = DataModelGraphMeta

    def __init__(self, attribute_relationships_dict):
        '''Load parsed data model.
        '''
        
        self.attribute_relationships_dict = attribute_relationships_dict
        self.dmn = DataModelNodes(self.attribute_relationships_dict)
        self.dme = DataModelEdges()
        self.data_model_relationships = DataModelRelationships()

        if not self.attribute_relationships_dict:
            raise ValueError(
                    "Something has gone wrong, a data model was not loaded into the DataModelGraph Class. Please check that your paths are correct"
                )
        self.graph = self.generate_data_model_graph()


    def generate_data_model_graph(self):
        '''Generate NetworkX Graph from the Relationships/attributes dictionary
        
        '''
        # Get all relationships with edges
        edge_relationships = self.data_model_relationships.define_edge_relationships()

        # Instantiate NetworkX MultiDigraph
        G = nx.MultiDiGraph()

        # Find all nodes
        all_nodes = self.dmn.gather_all_nodes(self.attribute_relationships_dict)
        all_node_dict = {}
        ## Fill in MultiDigraph with nodes and edges
        for node in all_nodes:
            
            # Gather information for each node
            node_dict = self.dmn.generate_node_dict(node, self.attribute_relationships_dict)

            all_node_dict[node] = node_dict
            # Generate node and attach information
            G = self.dmn.generate_node(G, node_dict)

        for node in all_nodes:
            # Generate edges
            G = self.dme.generate_edge(G, node, all_node_dict, self.attribute_relationships_dict, edge_relationships)
        return G

class DataModelGraphExporer():
    def __init__(self,
                 G,):
        '''
        Load data model graph as a singleton.
        '''
        self.graph = G

    def find_properties(self):
        """
        TODO: handle 'domainValue' with relationship edge parameters.
        """
        properties=[]
        for node_1, node_2, rel in self.graph.edges:
            if rel == 'domainValue':
                properties.append(node_1)
        properties = set(properties)
        return properties

    def find_classes(self):
        #checked
        nodes = self.graph.nodes
        properties = self.find_properties()
        classes = nodes - properties
        return classes

    def get_adjacent_nodes_by_relationship(self,
                                           node: str,
                                           relationship: str) -> List[str]:
        """Get a list of nodes that is / are adjacent to a given node, based on a relationship type.

        Args:
            node: the node whose edges we need to look at.
            relationship: the type of link(s) that the above node and its immediate neighbors share.

        Returns:
            List of nodes that are adjacent to the given node.
        #checked
        """
        nodes = set()

        for (u, v, key, c) in self.graph.out_edges(node, data=True, keys=True):
            if key == relationship:
                nodes.add(v)

        return list(nodes)

    def get_component_requirements(self,
                                   source_component: str,
                                   requires_component_relationship: str = "requiresComponent") -> List[str]:
        """Get all components that are associated with a given source component and are required by it.

        Args:
            source_component: source component for which we need to find all required downstream components.

        Returns:
            List of nodes that are descendants from the source component are are related to the source through a specific component relationship.
        """

        req_components = list(
            reversed(
                self.get_descendants_by_edge_type(
                    source_component, requires_component_relationship, ordered=True
                )
            )
        )

        return req_components

    def get_component_requirements_graph(self,
                                         source_component: str,
                                         requires_component_relationship: str = "requiresComponent") -> nx.DiGraph:
        """Get all components that are associated with a given source component and are required by it; return the components as a dependency graph (i.e. a DAG).

        Args:
            source_component: source component for which we need to find all required downstream components.

        Returns:
            A subgraph of the schema graph induced on nodes that are descendants from the source component and are related to the source through a specific component relationship.
        """

        # get a list of required component nodes
        req_components = self.get_component_requirements(source_component)

        # get the subgraph induced on required component nodes
        req_components_graph = self.get_subgraph_by_edge_type(
            self.graph, requires_component_relationship
        ).subgraph(req_components)

        return req_components_graph

    def get_descendants_by_edge_type(self,
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

        root_descendants = nx.descendants(self.graph, source_node)
        breakpoint()

        subgraph_nodes = list(root_descendants)
        subgraph_nodes.append(source_node)
        descendants_subgraph = self.graph.subgraph(subgraph_nodes)

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

    def get_digraph_by_edge_type(self):

        digraph = nx.DiGraph()
        for (u, v, key, c) in self.graph.edges(data=True, keys=True):
            if key == edge_type:
                digraph.add_edge(u, v)

        return digraph

    def get_edges_by_relationship(self,
                                  class_label: str,
                                  relationship: str,
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

        for (u, v, key, c) in self.graph.out_edges(node, data=True, keys=True):
            if key == relationship:
                edges.append((u, v))

        return edges

    def get_node_definition(self, node_display_name: str) -> str:
        """Get the node definition, i.e., the "comment" associated with a given node display name.

        Args:
            node_display_name: Display name of the node which you want to get the label for.

        Returns:
            Comment associated with node, as a string.
        """
        node_label = self.get_node_label(node_display_name)

        if not node_label:
            return ""

        node_definition = self.graph.nodes[node_label]["comment"]
        return node_definition


    def get_node_dependencies(self,
                              source_node: str,
                              display_names: bool = True,
                              schema_ordered: bool = True,
                              requires_dependency_relationship: str = "requiresDependency", 
    ) -> List[str]:
        """Get the immediate dependencies that are related to a given source node.

        Args:
            source_node: The node whose dependencies we need to compute.
            display_names: if True, return list of display names of each of the dependencies.
                           if False, return list of node labels of each of the dependencies.
            schema_ordered: if True, return the dependencies of the node following the order of the schema (slower).
                            if False, return dependencies from graph without guaranteeing schema order (faster)

        Returns:
            List of nodes that are dependent on the source node.
        """

        # NOTE might not be necessary to move through explore_class in this refactored version.
        if schema_ordered:
            # get dependencies in the same order in which they are defined in the schema
            required_dependencies = self.explore_class(source_node)["dependencies"]
        else:
            required_dependencies = self.get_adjacent_nodes_by_relationship(
                source_node, self.requires_dependency_relationship
            )

        if display_names:
            # get display names of dependencies
            dependencies_display_names = []

            for req in required_dependencies:
                dependencies_display_names.append(self.graph.nodes[req]["displayName"])

            return dependencies_display_names

        return required_dependencies

    def get_node_label(self, node_display_name: str) -> str:
        """Get the node label for a given display name.

        Args:
            node_display_name: Display name of the node which you want to get the label for.

        Returns:
            Node label associated with given node.

        Raises:
            KeyError: If the node cannot be found in the graph.
        """

        node_class_label = SchemaUtils.get_class_label_from_display_name(node_display_name)
        node_property_label = SchemaUtils.get_property_label_from_display_name(
            node_display_name
        )

        if node_class_label in self.graph.nodes:
            node_label = node_class_label
        elif node_property_label in self.graph.nodes:
            node_label = node_property_label
        else:
            node_label = ""

        return node_label

    def find_adjacent_child_classes(self, schema_class):

        return self.get_adjacent_nodes_by_relationship(schema_class, "parentOf")

    def find_all_class_properties(self):
        """
        does not seem used. do not transfer now.
        """
        breakpoint()
        return

    def find_class_specific_properties(self, schema_class):
        """Find properties specifically associated with a given class"""
        
        #This is called directly from the API
        # Needs to be refactored no longer be JSONLD specific
        
        breakpoint()
        schema_uri = self.graph.nodes[schema_class]["uri"]
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
        return

    def find_class_usages(self):
        """
        Does not look used, do not transfer for now.
        """
        return

    def is_node_required(self, node_display_name: str) -> bool:
        """Check if a given node is required or not.

        Note: The possible options that a node can be associated with -- "required" / "optional".

        Args:
            node_display_name: Display name of the node which you want to get the label for.

        Returns:
            True: If the given node is a "required" node.
            False: If the given node is not a "required" (i.e., an "optional") node.
        """
        node_label = self.get_node_label(node_display_name)

        node_required = self.graph.nodes[node_label]["required"]

        return node_required

    def explore_class(self):
        """
        nx specific version of this? This might not be necessary since each nx node should already contain all required information.
        Put this here for now as a dummy function so this can be explored more.
        """
        breakpoint()
        return

    def explore_property(self):
        breakpoint()
        return

    