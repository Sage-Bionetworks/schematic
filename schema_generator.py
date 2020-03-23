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
 

- requiresComponent: a node property indicating that this node requires a component (a higher level class/ontology category containing multiple attributes/objects) for its full characterization

    E.g. scRNA-seq Assay is an object that requires components Biospecimen (e.g. containing attributes describing the assay sample input); scRNA-seq QC is an object that requires component scRNA-seq Assay (e.g .containing attributes describing the assay for which this QC is performed).

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

requires_range = "rangeIncludes" # "requiresChildAsValue" is also an option here, but will be deprecated
#requires_range = "requiresChildAsValue"

range_value_relationship = "rangeValue" # "parentOf" is also an option here but will be deprecated
#range_value_relationship = "parentOf"

requires_component_relationship = "requiresComponent"


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


def get_component_requirements(graph: nx.MultiDiGraph, source_component: str) -> list:
    """
    Get all components that are associated with the given source component and required by it

    Args:
        graph: metadata model schema graph
        source_component: source component for which to find all downstream required components
    Returns: A list of required components
    """

    req_components = get_descendants_by_edge_type(graph, source_component, relationship_type = requires_component_relationship)

    return req_components


def get_descendants_by_edge_type(graph: nx.MultiDiGraph, source_node_label: str, relationship_type: str, connected: bool = True, ordered: bool = False) -> list:
    """
    Get all nodes that are descendent from this node on a specific type of edge, i.e. edge relationship type.

    Args: 
        graph: a networkx directed hypergraph; with typed edges
        source_node_label: node whose descendants are requested
        relationship: edge relationship type (see possible types above)
        connected: if True ensure that all descendent nodes are reachable - i.e. are in the same connected component - from the source node; if False, the descendent nodes could be in multiple connected components. Default is True. 
        ordered: if True, the list of descendant nodes will be topologically ordered. Default is False. 
    Returns: a list of nodes, descending from this node
    """


    # get all nodes reachable from the specified root node in the data model
    # TODO: catch if root is not in graph currently networkx would throw an exception?

    root_descendants = nx.descendants(graph, source_node_label)
    # get the subgraph induced on all nodes reachable from the root node
    subgraph_nodes = list(root_descendants)
    subgraph_nodes.append(source_node_label)
    descendants_subgraph = graph.subgraph(subgraph_nodes)

    '''
    prune the descendants subgraph to include only relationship edges matching relationship type
    '''
    rel_edges = []
    for u, v, properties in descendants_subgraph.edges(data = True):
        if properties["relationship"] == relationship_type:
            rel_edges.append((u,v))
    
    
    relationship_subgraph = nx.DiGraph()
    relationship_subgraph.add_edges_from(rel_edges)

    #relationship_subgraph = descendants_subgraph.edge_subgraph(rel_edges)

    descendants = relationship_subgraph.nodes()
    
    if connected and ordered:
        # get the set of reachable nodes from the source node
        descendants = nx.descendants(relationship_subgraph, source_node_label)
        descendants.add(source_node_label)
        # the descendants are unordered (peculiarity of nx descendants call)
        # form the subgraph on descendants and order it topologically
        # this assumes an acyclic subgraph
        descendants = nx.topological_sort(relationship_subgraph.subgraph(descendants))
    elif connected:
        # get only the nodes reachable from the root node (after the pruning above some nodes in the root-descendants subgraph might have become disconnected and will be omitted)
        descendants = nx.descendants(relationship_subgraph, source_node_label)
        descendants.add(source_node_label)
    elif ordered:
        # sort nodes topologically - this requires an acyclic graph 
        descendants = nx.topological_sort(relationship_subgraph)


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
                            json_schema["allOf"].append(schema_conditional_dependencies)


        '''
        get required nodes by this node (e.g. other terms/nodes
        that need to be specified based on a data model, if the 
        given term is specified); each of these node/terms needs to be 
        processed for dependencies in turn.
        '''
        if not process_node in nodes_with_processed_dependencies:
            process_node_dependencies = get_adgacent_node_by_relationship(mm_graph, process_node, requires_dependency_relationship)
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
                    json_schema["allOf"].append(schema_conditional_dependencies)

                nodes_to_process.update(process_node_dependencies)
                nodes_with_processed_dependencies.add(process_node)


    print("=================")
    print("JSONSchema successfully generated from Schema.org schema:")
    
    # if no conditional dependencies were added we can't have an empty 'AllOf' block in the schema, so remove it
    if not json_schema["allOf"]:
        del json_schema["allOf"]
    
    print(json.dumps(json_schema))
    with open("./schemas/json_schema_log.json", "w") as js_f:
        json.dump(json_schema, js_f, indent = 2)
    print("=================")

    return json_schema
