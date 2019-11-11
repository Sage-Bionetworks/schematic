import os
import json
import networkx as nx
from orderedset import OrderedSet

from schema_explorer import SchemaExplorer

#TODO: refactor into class; expose parameters as constructor args

"""
Gather all metadata/annotations requirements for an object part of the schema.org metadata model.

Assumed semantic sugar for requirements:

- requireChildAsValue: a node property indicating that the term associated with this node
    can be set to a value equal to any of the terms associated with this node's children. 

    E.g. suppose resourceType has property requireChildAsValue set to true and suppose
    resourceType has children curatedData, experimentalData, tool, analysis. Then 
    resourceType must be set either to a value curatedData, experimentalData, tool, analysis.

- rangeIncludes: a node property indicating that the term associated with this node
    can be set to a value equal to any of the terms associated with this node's range: nodes that are adjacent to this node on links of type "rangeValue". 

    E.g. suppose resourceType has property rangeIncludes and suppose
    resourceType is connected to curatedData, experimentalData, tool, analysis on links of type "rangeValue". Then 
    resourceType must be set either to a value curatedData, experimentalData, tool, analysis.
 

- requiresDependency: a relationship type corresponding to an edge between two nodes/terms x and y.
    requiresDependency indicates that if a value for term x is specified, then a value for term y 
    is also required.


    E.g. suppose node experimentalData has requiresDependency edges respectively to assay and dataType.
    Then, if experimentalData is specified that requires that both assay and dataType are specified
    (e.g. in annotations of an object).


This semantic sugar enables the generation different kinds of validation schemas including:
    - validate that all required annotation terms (i.e. properties) of an object are present in the 
    annotations json file associated with this object
    - validate that the value provided for a given property of an object is within a set of admissible values
    for that property
    - validate conditional annotations: if a property of an object is set to a specific value x, check if value 
    x requires any additional annotation properties to be specified for this object. E.g. if resourceType
    is experimentalData, require also that annotations/properties assay, dataType are set. 
    This support cascades of conditional validation (of arbitrary lengths).
"""

requires_dependency_relationship = "requiresDependency"
requires_range = "requiresChildAsValue" # "requiresChildAsValue" is also an option here, but will be deprecated
#requires_range = "rangeIncludes" # "requiresChildAsValue" is also an option here, but will be deprecated
range_value_relationship = "parentOf" # "parentOf" is also an option here but will be deprecated
#range_value_relationship = "rangeValue" # "parentOf" is also an option here but will be deprecated

"""
Get the out-edges of a node, where the edges match specific type of relationship: 
i.e. edges connecting to nodes neighbors are of relationship type "parentOf" - set of edges to children/subclass nodes;

Possible edge relationship types are parentOf, rangeValue, requiresDependency

"""
def get_edges_by_relationship(mm_graph, u, relationship):

    edges = []
    for u, v, properties in mm_graph.out_edges(u, data = True):
        if properties["relationship"] == relationship: 
            edges.append((u,v))

    return edges



"""
Get the  adjacent nodes of a node by a relationship type: i.e. nodes neighbors of node u on edges of type "parentOf"

Possible edge relationship types are parentOf, rangeValue, requiresDependency

"""
def get_adgacent_node_by_relationship(mm_graph, u, relationship):
    nodes = set()
    for u, v, properties in mm_graph.out_edges(u, data = True):
        if properties["relationship"] == relationship: 
            nodes.add(v)

    return list(nodes)


"""
Get the nodes that this node requires as dependencies that are also neihbors of node u
on edges of type "requiresDependency"
def get_node_neighbor_dependencies(mm_graph, u):
    
    children = set()
    for u, v, properties in mm_graph.out_edges(u, data = True):
        if properties["relationship"] == requires_dependency: 
            children.add(v)

    return list(children)
"""



"""
TODO: check if needed and remove the get_node_dependencies method
Get the nodes that this node requires as dependencies.
These are all nodes *reachable* on edges of type "requiresDependency"
def get_node_dependencies(req_graph, u):
    
    descendants = nx.descendants(req_graph, u)

    return list(descendants)
"""


"""
Gather dependencies and value-constraints across terms/nodes in 
a schema.org schema and store them as a JSONSchema schema. 
I.e. recursively, for any given node in the schema.org schema starting at a root
node, 1) find all the terms that this node depends on (and hence are required as 
additional metadata, given this node is required); 2) find all the 
allowable metadata values/nodes that can be assigned to a particular node
(if such constraint is specified in the schema)
"""
def get_JSONSchema_requirements(se, root, schema_name):
    
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
    mm_graph = se.get_nx_schema()
   
    # nodes to check for dependencies, starting with the provided root
    nodes_to_process = OrderedSet()
    nodes_to_process.add(root) 

    # keep track of nodes with processed dependencies
    nodes_with_processed_dependencies = set()

    '''
    keep checking for dependencies until there are no nodes
    left to process
    '''
    while nodes_to_process:  
        process_node = nodes_to_process.pop()

        '''
        get allowable values for this node;
        each of these values is a node that in turn is processed for
        dependencies and allowed values
        '''
        
        if requires_range in mm_graph.nodes[process_node]:
            if mm_graph.nodes[process_node][requires_range]:
                node_range = get_adgacent_node_by_relationship(mm_graph, process_node, range_value_relationship)
                
                # set allowable values based on range nodes
                if node_range:
                    schema_properties = {mm_graph.nodes[process_node]["displayName"]:{"enum":[mm_graph.nodes[node]["displayName"] for node in node_range]}}
                    json_schema["properties"].update(schema_properties)                
                
                    # add range nodes for requirements processing
                    nodes_to_process.update(node_range)
                
                    # set conditional dependencies based on node range dependencies
                    for node in node_range:
                        node_dependencies = get_adgacent_node_by_relationship(mm_graph, node, requires_dependency_relationship)
                        if node_dependencies:
                            schema_conditional_dependencies = {
                                    "if": {
                                        "properties": {
                                        mm_graph.nodes[process_node]["displayName"]: { "enum": [mm_graph.nodes[node]["displayName"]] }
                                        },
                                        "required":[mm_graph.nodes[process_node]["displayName"]],
                                      },
                                    "then": { "required": [mm_graph.nodes[node_dependency]["displayName"] for node_dependency in node_dependencies] },
                            }
                            nodes_with_processed_dependencies.add(node)
                            nodes_to_process.update(node_dependencies)


        '''
        get required nodes by this node (e.g. other terms/nodes
        that need to be specified based on a data model, if the 
        given term is specified); each of these node/terms needs to be 
        processed for dependencies in turn.
        '''
        if not process_node in nodes_with_processed_dependencies:
            process_node_dependencies = get_adgacent_node_by_relationship(mm_graph, process_node, requires_dependency_relationship)
            print(process_node_dependencies)
            if process_node_dependencies:
                if process_node == root: # these are unconditional dependencies
                    
                    json_schema["required"] += [mm_graph.nodes[process_node_dependency]["displayName"] for process_node_dependency in process_node_dependencies]
                else: # these are dependencies given the processed node 
                    schema_conditional_dependencies = {
                            "if": {
                                "properties": {
                                mm_graph.nodes[process_node]["displayName"]: { "string":"*" }
                                },
                                "required":[mm_graph.nodes[process_node]["displayName"]],
                              },
                            "then": { "required": [mm_graph.nodes[process_node_dependency]["displayName"] for process_node_dependency in process_node_dependencies] },
                    }

                nodes_to_process.update(process_node_dependencies)
                nodes_with_processed_dependencies.add(process_node)


    print("=================")
    print("JSONSchema successfully generated from Schema.org schema:")
    
    # if no conditional dependencies were added we can't have an empty 'AllOf' block in the schema, so remove it
    if not json_schema["allOf"]:
        del json_schema["allOf"]
    
    print(json_schema)
    print("=================")

    return json_schema
