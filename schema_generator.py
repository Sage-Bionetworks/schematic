import os
import json
import networkx as nx
from orderedset import OrderedSet

from schema_explorer import SchemaExplorer

"""
Gather all metadata/annotations requirements for an object part of the schema.org metadata model.

Assumed semantic sugar for requirements:

- requireChildAsValue: a node property indicating that the term associated with this node
    can be set to a value equal to any of the terms associated with this node's children. 

    E.g. suppose resourceType has property requireChildAsValue set to true and suppose
    resourceType has children curatedData, experimentalData, tool, analysis. Then 
    resourceType must be set either to curatedData, experimentalData, tool, analysis.

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

requires_dependency = "requiresDependency"
requires_child = "requiresChildAsValue"


"""
Get the parent-children out-edges of a node: i.e. edges connecting to nodes neighbors
of node u on edges of type "parentOf"
"""
def get_children_edges(mm_graph, u):

    children_edges = []
    for u, v, properties in mm_graph.out_edges(u, data = True):
        if properties["relationship"] == "parentOf": 
            children_edges.append((u,v))

    return children_edges



"""
Get the children nodes of a node: i.e. nodes neighbors of node u
on edges of type "parentOf"
"""
def get_node_children(mm_graph, u):

    children = set()
    for u, v, properties in mm_graph.out_edges(u, data = True):
        if properties["relationship"] == "parentOf": 
            children.add(v)

    return list(children)


"""
Get the nodes that this node requires as dependencies that are also neihbors of node u
on edges of type "requiresDependency"
"""
def get_node_neighbor_dependencies(mm_graph, u):
    
    children = set()
    for u, v, properties in mm_graph.out_edges(u, data = True):
        if properties["relationship"] == requires_dependency: 
            children.add(v)

    return list(children)


"""
Get the nodes that this node requires as dependencies.
These are all nodes *reachable* on edges of type "requiresDependency"
"""
def get_node_dependencies(req_graph, u):
    

    descendants = nx.descendants(req_graph, u)

    return list(descendants)


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
        
        if requires_child in mm_graph.nodes[process_node]:
            if mm_graph.nodes[process_node][requires_child]:
                children = get_node_children(mm_graph, process_node)
                
                # set allowable values based on children nodes
                if children:
                    schema_properties = {mm_graph.nodes[process_node]["displayName"]:{"enum":[mm_graph.nodes[child]["displayName"] for child in children]}}
                    json_schema["properties"].update(schema_properties)                
                
                    # add children for requirements processing
                    nodes_to_process.update(children)
                
                    # set conditional dependencies based on children dependencies
                    for child in children:
                        child_dependencies = get_node_neighbor_dependencies(mm_graph, child)
                        if child_dependencies:
                            schema_conditional_dependencies = {
                                    "if": {
                                        "properties": {
                                        mm_graph.nodes[process_node]["displayName"]: { "enum": [mm_graph.nodes[child]["displayName"]] }
                                        },
                                        "required":[mm_graph.nodes[process_node]["displayName"]],
                                      },
                                    "then": { "required": [mm_graph.nodes[child_dependency]["displayName"] for child_dependency in child_dependencies] },
                            }
                            nodes_with_processed_dependencies.add(child)
                            nodes_to_process.update(child_dependencies)


        '''
        get required nodes by this node (e.g. other terms/nodes
        that need to be specified based on a data model, if the 
        given term is specified); each of these node/terms needs to be 
        processed for dependencies in turn.
        '''
        if not process_node in nodes_with_processed_dependencies:
            process_node_dependencies = get_node_neighbor_dependencies(mm_graph, process_node)

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
    print("JSONSchema successfully generated from Schema.org schema!")
    print("=================")
    print(json_schema)
    # if no conditional dependencies were added we can't have an empty 'AllOf' block in the schema, so remove it
    if not json_schema["allOf"]:
        del json_schema["allOf"]

    return json_schema
