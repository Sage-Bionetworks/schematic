import os
import json
import networkx as nx
from orderedset import OrderedSet

from typing import Any, Dict, Optional, Text, List

from schematic.schemas.explorer import SchemaExplorer

from schematic.utils.io_utils import load_json
from schematic.utils.schema_utils import load_schema_into_networkx
from schematic.utils.validate_utils import validate_schema
from schematic.utils.config_utils import load_yaml

from definitions import CONFIG_PATH, DATA_PATH

config_data = load_yaml(CONFIG_PATH)

class SchemaGenerator(object):
    def __init__(self,
                path_to_json_ld: str,
                requires_dependency_relationship: str = "requiresDependency",  # optional parameter(s) with default value
                requires_range: str = "rangeIncludes",
                range_value_relationship: str = "rangeValue",
                requires_component_relationship: str = "requiresComponent"
                ) -> None:
        """Create / Initialize object of type SchemaGenerator().

        Methods / utilities that are part of this module can be used to generate JSON validation schemas for different schema.org 
        specification models.

        Args:
            path_to_json_ld: Path to the JSON-LD file that is representing the schema.org data model that we want to validate.
            requires_dependency_relationship: Edge relationship between two nodes indicating that they are dependent on each other.
            requires_range: A node propertly indicating that a term can assume a value equal to any of the terms that are in the current term's range.
            range_value_relationship: Edge relationship that indicates a term / node that another node depends on, is part of the other node's range.
            requires_component_relationship: A node property indicating that this node requires a component for its full characterization.

        Returns:
            None
        """
        # create an instance of SchemaExplorer
        self.se = SchemaExplorer()
        
        if path_to_json_ld.rpartition('.')[-1] == "jsonld":
            # convert the JSON-LD data model to networkx object
            self.se.load_schema(path_to_json_ld)
        else:
            print("Please make sure the 'path_to_json_ld' parameter is pointing to a valid JSON-LD file.")
            return

        # custom value(s) of following relationship attributes are passed during initialization
        self.requires_dependency_relationship = requires_dependency_relationship
        self.requires_range = requires_range
        self.range_value_relationship = range_value_relationship
        self.requires_component_relationship = requires_component_relationship
        

    def get_edges_by_relationship(self,
                                node: str,
                                relationship: str) -> List[str]:
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

        mm_graph = self.se.get_nx_schema()

        for (u, v, key, c) in mm_graph.out_edges(node, data=True, keys=True):
            if key == relationship:
                edges.append((u, v))

        return sorted(edges)


    def get_adjacent_nodes_by_relationship(self,
                                          node: str,
                                          relationship: str) -> List[str]:
        """Get a list of nodes that is / are adjacent to a given node, based on a relationship type.

        Args:
            node: the node whose edges we need to look at.
            relationship: the type of link(s) that the above node and its immediate neighbors share.
        
        Returns:
            List of nodes that are adjacent to the given node.
        """
        nodes = set()
        
        mm_graph = self.se.get_nx_schema()

        for (u, v, key, c) in mm_graph.out_edges(node, data=True, keys=True):
            if key == relationship:
                nodes.add(v)

        return sorted(list(nodes))


    def get_descendants_by_edge_type(self,
                                    source_node: str,
                                    relationship: str,
                                    connected: bool = True,
                                    ordered: bool = False) -> List[str]:
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
        mm_graph = self.se.get_nx_schema()

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
            descendants = nx.topological_sort(relationship_subgraph.subgraph(descendants))
        elif connected:
            # get the nodes that are reachable from a given source node
            # after the pruing process above some nodes in the root_descendants subgraph might have become disconnected and will be omitted
            descendants = nx.descendants(relationship_subgraph, source_node)
            descendants.add(source_node)
        elif ordered:
            # sort the nodes topologically
            # this requires the graph to be an acyclic graph
            descendants = nx.topological_sort(relationship_subgraph)

        return sorted(list(descendants))


    def get_component_requirements(self,
                                  source_component: str) -> List[str]:
        """Get all components that are associated with a given source component and are required by it.

        Args:
            source_component: source component for which we need to find all required downstream components.

        Returns:
            List of nodes that are descendants from the source component are are related to the source through a specific component relationship.
        """
        req_components = self.get_descendants_by_edge_type(source_component, self.requires_component_relationship)
        
        return req_components


    def get_node_dependencies(self,
                            source_node: str,
                            display_names: bool = True,
                            schema_ordered: bool = True) -> List[str]:
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
        mm_graph = self.se.get_nx_schema()

        if schema_ordered:
            # get dependencies in the same order in which they are defined in the schema
            required_dependencies = self.se.explore_class(source_node)["dependencies"]
        else:
            required_dependencies = self.get_adjacent_nodes_by_relationship(source_node, self.requires_dependency_relationship)

        if display_names:
            # get display names of dependencies
            dependencies_display_names = []
            
            for req in required_dependencies:
                dependencies_display_names.append(mm_graph.nodes[req]["displayName"])

            return dependencies_display_names

        return required_dependencies


    def get_node_range(self,
                      node_label: str,
                      display_names: bool = True) -> List[str]:
        """Get the range, i.e., all the valid values that are associated with a node label.

        Args:
            node_label: Node / termn for which you need to retrieve the range.

        Returns:
            List of display names of nodes associateed with the given node.
        """
        mm_graph = self.se.get_nx_schema()

        try:
            # get node range in the order defined in schema for given node
            required_range = self.se.explore_class(node_label)["range"]
        except KeyError:
            print("The given source node cannot be found in the graph. Please try a different node label.")
            return []

        if display_names:
            # get the display name(s) of all dependencies
            dependencies_display_names = []

            for req in required_range:
                dependencies_display_names.append(mm_graph.nodes[req]["displayName"])

            return dependencies_display_names

        return required_range


    def get_node_label(self,
                      node_display_name: str) -> str:
        """Get the node label for a given display name.
        
        Args:
            node_display_name: Display name of the node which you want to get the label for.

        Returns:
            Node label associated with given node.

        Raises:
            KeyError: If the node cannot be found in the graph.
        """
        mm_graph = self.se.get_nx_schema()

        node_class_label = self.se.get_class_label_from_display_name(node_display_name)
        node_property_label = self.se.get_property_label_from_display_name(node_display_name)

        if node_class_label in mm_graph.nodes:
            node_label = node_class_label
        elif node_property_label in mm_graph.nodes:
            node_label = node_property_label
        else:
            node_label = ""

        return node_label


    def get_node_definition(self,
                           node_display_name: str) -> str:
        """Get the node definition, i.e., the "comment" associated with a given node display name. 

        Args:
            node_display_name: Display name of the node which you want to get the label for.

        Returns:
            Comment associated with node, as a string.
        """
        node_label = self.get_node_label(node_display_name)

        if not node_label:
            return ""

        mm_graph = self.se.get_nx_schema()
        node_definition = mm_graph.nodes[node_label]["comment"] 
         
        return node_definition


    def get_node_validation_rules(self, node_display_name: str) -> str:
        """Get validation rules associated with a node,  

        Args:
            node_display_name: Display name of the node which you want to get the label for.

        Returns:
            A set of validation rules associated with node, as a list.
        """
        node_label = self.get_node_label(node_display_name)

        if not node_label:
            return []

        mm_graph = self.se.get_nx_schema()
        node_validation_rules = mm_graph.nodes[node_label]["validationRules"] 
         
        return node_validation_rules


    def is_node_required(self,
                        node_display_name: str) -> bool:
        """Check if a given node is required or not.

        Note: The possible options that a node can be associated with -- "required" / "optional".

        Args:
            node_display_name: Display name of the node which you want to get the label for.

        Returns:
            True: If the given node is a "required" node.
            False: If the given node is not a "required" (i.e., an "optional") node.
        """
        node_label = self.get_node_label(node_display_name)

        mm_graph = self.se.get_nx_schema()
        node_required = mm_graph.nodes[node_label]["required"]

        return node_required


    def get_nodes_display_names(self,
                               node_list: List[str],
                               mm_graph: nx.MultiDiGraph) -> List[str]:
        """Get display names associated with the given list of nodes.

        Args:
            node_list: List of nodes whose display names we need to retrieve.

        Returns:
            List of display names.
        """
        node_list_display_names = [mm_graph.nodes[node]["displayName"] for node in node_list]

        return node_list_display_names


    def get_range_schema(self,
                        node_range: List[str],
                        node_name: str,
                        blank = False) -> Dict[str, Dict[str, List[str]]]:
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


    def get_array_schema(self,
                        node_range: List[str],
                        node_name: str,
                        blank = False) -> Dict[str, Dict[str, List[str]]]:
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
                                    node_name:{
                                        "type": "array",
                                        "items": {
                                            "enum": node_range + [""] if blank else node_range 
                                        },
                                        "maxItems": len(node_range)
                                    }
        }
        
        return schema_node_range_array


    def get_non_blank_schema(self,
                            node_name: str) -> Dict:    # can't define heterogenous Dict generic types
        """Get a schema rule that does not allow null or empty values.

        Args:
            node_name: Name of the node on which the schema rule is to be applied.

        Returns:
            Schema rule as a JSON object.
        """
        non_blank_schema = {node_name: {"not": {"type": "null"}, "minLength": 1}}

        return non_blank_schema


    def is_required(self, node_name: str, mm_graph: nx.MultiDiGraph) -> bool:
        """
        Check if a node is required

        Args:
            node_name: Name of the node on which the check is to be applied.

        Returns:
            Boolean value indicating if the node is required or not.
                True: yes, it is required.
                False: no, it is not required.
        """
        return mm_graph.nodes[node_name]["required"]


    def get_json_schema_requirements(self,
                                    source_node: str,
                                    schema_name: str) -> Dict:
        """Consolidated method that aims to gather dependencies and value constraints across terms / nodes in a schema.org schema and store them in a jsonschema /JSON Schema schema.
        
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
            "$id":"http://example.com/" + schema_name,
            "title": schema_name,
            "type": "object",
            "properties":{},
            "required":[],
            "allOf":[]
        }

        # get graph corresponding to data model schema
        mm_graph = self.se.get_nx_schema()
    
        nodes_to_process = []   # list of nodes to be checked for dependencies, starting with the source node
        processed_nodes = []    # keep of track of nodes whose dependencies have been processed
        reverse_dependencies = {}   # maintain a map between conditional nodes and their dependencies (reversed) -- {dependency : conditional_node}
        range_domain_map = {}   # maintain a map between range nodes and their domain nodes {range_value : domain_value}
                                # the domain node is very likely the parentof ("parentOf" relationship) of the range node

        root_dependencies = self.get_adjacent_nodes_by_relationship(source_node, self.requires_dependency_relationship)

        nodes_to_process += root_dependencies
        
        process_node = nodes_to_process.pop(0)

        while process_node:
            
            if not process_node in processed_nodes:
                # node is being processed
                node_is_processed = True

                node_range = self.get_adjacent_nodes_by_relationship(process_node, self.range_value_relationship)

                # get node range display name
                node_range_d = self.get_nodes_display_names(node_range, mm_graph)
                
                node_dependencies = self.get_adjacent_nodes_by_relationship(process_node, self.requires_dependency_relationship)
                
                # get process node display name
                node_display_name = mm_graph.nodes[process_node]["displayName"]  

                # updating map between node and node's valid values 
                for n in node_range_d:
                    if not n in range_domain_map:
                        range_domain_map[n] = []
                    range_domain_map[n].append(node_display_name)

                # can this node be map to the empty set (if required no; if not required yes)
                # TODO: change "required" to different term, required may be a bit misleading (i.e. is the node required in the schema) 
                node_required = self.is_required(process_node, mm_graph)

                # get any additional validation rules associated with this node (e.g. can this node be mapped to a list of other nodes)
                node_validation_rules = self.get_node_validation_rules(node_display_name)

                if node_display_name in reverse_dependencies:
                    # if node has conditionals set schema properties and conditional dependencies
                    # set schema properties
                    if node_range:
                        # if process node has valid value range set it in schema properties
                        schema_valid_vals = self.get_range_schema(node_range_d, node_display_name, blank = True)
                        
                        if node_validation_rules:
                            # if this node has extra validation rules process them
                            # TODO: abstract this into its own validation rule constructor/generator module/class
                            
                            if "list" in node_validation_rules:
                                # if this node can be mapped to a list of nodes
                                # set its schema accordingly
                                schema_valid_vals = self.get_array_schema(node_range_d, node_display_name, blank = True)
            
                    else:
                        # otherwise, by default allow any values
                        schema_valid_vals = {node_display_name:{}}

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
                                conditional_properties.update({"properties":{domain_node:{"enum":[node]}}, "required":[domain_node]})

                                # given node conditional are satisfied, this process node (which is dependent on these conditionals) has to be set or not depending on whether it is required
                                if node_range:    
                                    dependency_properties = self.get_range_schema(node_range_d, node_display_name, blank = not node_required)

                                    if node_validation_rules: 
                                        if "list" in node_validation_rules:
                                            #TODO: get_range_schema and get_range_schema have similar behavior - combine in one module
                                            dependency_properties = self.get_array_schema(node_range_d, node_display_name, blank = not node_required)

                                else:
                                    if node_required:
                                        dependency_properties = self.get_non_blank_schema(node_display_name)    
                                    else:
                                        dependency_properties = {node_display_name:{}}
                                schema_conditional_dependencies = {
                                        "if": conditional_properties, 
                                        "then":{
                                            "properties":dependency_properties,
                                            "required":[node_display_name]
                                        }
                                }
                                    
                                # update conditional-dependency rules in json schema
                                json_schema["allOf"].append(schema_conditional_dependencies)

                else:
                    # node doesn't have conditionals
                    if node_required:
                        if node_range:
                            schema_valid_vals = self.get_range_schema(node_range_d, node_display_name, blank = False)

                            if node_validation_rules: 
                                if "list" in node_validation_rules:
                                    schema_valid_vals = self.get_array_schema(node_range_d, node_display_name, blank = False)
                        else:
                            schema_valid_vals = self.get_non_blank_schema(node_display_name)
                        
                        json_schema["properties"].update(schema_valid_vals)
                        # add node to required fields
                        json_schema["required"] += [node_display_name]

                    elif process_node in root_dependencies:
                        # node doesn't have conditionals and is not required; it belongs in the schema only if it is in root's dependencies
                        
                        if node_range:
                            schema_valid_vals = self.get_range_schema(node_range_d, node_display_name, blank = True)
                            
                            if node_validation_rules: 
                                if "list" in node_validation_rules:
                                    schema_valid_vals = self.get_array_schema(node_range_d, node_display_name, blank = True)

                        else:
                            schema_valid_vals = {node_display_name:{}}
                        
                        json_schema["properties"].update(schema_valid_vals)
                        
                    else:
                        # node doesn't have conditionals and it is not required and it is not a root dependency
                        # the node doesn't belong in the schema
                        # do not add to processed nodes since its conditional may be traversed at a later iteration (though unlikely for most schemas we consider)
                        node_is_processed = False
                    
                # add process node as a conditional to its dependencies
                node_dependencies_d = self.get_nodes_display_names(node_dependencies, mm_graph)

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


        print("========================================================================================")
        print("JSON schema successfully generated from schema.org schema!")
        
        # if no conditional dependencies were added we can't have an empty 'AllOf' block in the schema, so remove it
        if not json_schema["allOf"]:
            del json_schema["allOf"]
        
        json_schema_log_file = os.path.join(DATA_PATH, '', config_data["model"]["input"]["log_location"])
        with open(json_schema_log_file, "w") as js_f:
            json.dump(json_schema, js_f, indent = 2)
        
        print("JSON schema file log stored as {}".format(json_schema_log_file))
        print("========================================================================================")

        return json_schema
