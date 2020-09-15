import networkx as nx
import json

from schematic.utils.curie_utils import extract_name_from_uri_or_curie
from schematic.utils.validate_utils import validate_class_schema

def load_schema_into_networkx(schema):
    G = nx.MultiDiGraph()
    for record in schema["@graph"]:
       
        # TODO: clean up obsolete code 
        #if record["@type"] == "rdfs:Class":
            
        # creation of nodes
        # adding nodes to the graph
        node = {}
        for (k, value) in record.items():
            if ":" in k:
                key = k.split(":")[1]
                node[key] = value
            elif "@" in k:
                key = k[1:]
                node[key] = value
            else:
                node[k] = value

        # creation of edges
        # adding edges to the graph
        if "rdfs:subClassOf" in record:
            parents = record["rdfs:subClassOf"]
            if type(parents) == list:
                for _parent in parents:
                    n1 = extract_name_from_uri_or_curie(_parent["@id"])
                    n2 = record["rdfs:label"]

                    # do not allow self-loops
                    if n1 != n2:
                        G.add_edge(n1, n2, key="parentOf")
            elif type(parents) == dict:
                n1 = extract_name_from_uri_or_curie(parents["@id"])
                n2 = record["rdfs:label"]

                # do not allow self-loops
                if n1 != n2:
                    G.add_edge(n1, n2, key="parentOf")

        # TODO: refactor: abstract adding relationship method
        if "sms:requiresDependency" in record:
            dependencies = record["sms:requiresDependency"]
            if type(dependencies) == list:
                for _dep in dependencies:
                    n1 = record["rdfs:label"]  
                    n2 = extract_name_from_uri_or_curie(_dep["@id"]) 
                    # do not allow self-loops
                    if n1 != n2:
                        G.add_edge(n1, n2, key="requiresDependency")

        if "sms:requiresComponent" in record:
            components = record["sms:requiresComponent"]
            if type(components) == list:
                for _comp in components:
                    n1 = record["rdfs:label"]  
                    n2 = extract_name_from_uri_or_curie(_comp["@id"]) 
                    # do not allow self-loops
                    if n1 != n2:
                        G.add_edge(n1, n2, key="requiresComponent")

        if "schema:rangeIncludes" in record:
            range_nodes = record["schema:rangeIncludes"]
            if type(range_nodes) == list:
                for _range_node in range_nodes:
                    n1 = record["rdfs:label"]  
                    n2 = extract_name_from_uri_or_curie(_range_node["@id"]) 
                    # do not allow self-loops
                    if n1 != n2:
                        G.add_edge(n1, n2, key="rangeValue")
            elif type(range_nodes) == dict:
                n1 = record["rdfs:label"]  
                n2 = extract_name_from_uri_or_curie(range_nodes["@id"]) 
                # do not allow self-loops
                if n1 != n2:
                    G.add_edge(n1, n2, key="rangeValue")
 
        if "schema:domainIncludes" in record:
            domain_nodes = record["schema:domainIncludes"]
            if type(domain_nodes) == list:
                for _domain_node in domain_nodes:
                    n1 = extract_name_from_uri_or_curie(_domain_node["@id"])
                    n2 = record["rdfs:label"]
                    # do not allow self-loops
                    if n1 != n2:
                        G.add_edge(n1, n2, key="domainValue")
            elif type(domain_nodes) == dict:
                n1 = extract_name_from_uri_or_curie(domain_nodes["@id"])
                n2 = record["rdfs:label"]
                # do not allow self-loops
                if n1 != n2:
                    G.add_edge(n1, n2, key="domainValue")
        
        # check schema generator (JSON validation schema gen)
        if "requiresChildAsValue" in node and node["requiresChildAsValue"]["@id"] == "sms:True":
            node["requiresChildAsValue"] = True
        
        if "required" in node:
            if "sms:true" == record["sms:required"]:
                node["required"] = True  
            else:
                node["required"] = False

        # not sure if this is required?
        if "sms:validationRules" in record:
            node["validationRules"] = record["sms:validationRules"]
        else:
            node["validationRules"] = []

        node['uri'] = record["@id"] 
        node['description'] = record["rdfs:comment"]
        G.add_node(record['rdfs:label'], **node)
        #print(node)
        #print(G.nodes())

    return G

def node_attrs_cleanup(class_add_mod: dict) -> dict:
    # clean map that will be inputted into the node/graph
    node = {}
    for (k, value) in class_add_mod.items():
        if ":" in k:
            key = k.split(":")[1]
            node[key] = value
        elif "@" in k:
            key = k[1:]
            node[key] = value
        else:
            node[k] = value

    return node

def relationship_edges(schema_graph_nx: nx.MultiDiGraph, class_add_mod: dict, **kwargs) -> nx.MultiDiGraph:
    """
    Notes:
    =====
    # pass the below dictionary as the third argument (kwargs) to relationship_edges().
    # "in" indicates that the relationship has an in-edges behaviour.
    # "out" indicates that the relationship has an out-edges behaviour.

    rel_dict = {
        "rdfs:subClassOf": {
            "parentOf": "in"
        },
        "sms:requiresDependency": {
            "requiresDependency": "out"
        },
        "sms:requiresComponent": {
            "requiresComponent": "out"
        },
        "schema:rangeIncludes": {
            "rangeValue": "out"
        }
    }
    """
    for rel, rel_lab_node_type in kwargs.items():
        for rel_label, node_type in rel_lab_node_type.items():
            if rel in class_add_mod:
                parents = class_add_mod[rel]
                if type(parents) == list:
                    for _parent in parents:

                        if node_type == "in":
                            n1 = extract_name_from_uri_or_curie(_parent["@id"])
                            n2 = class_add_mod["rdfs:label"]

                        if node_type == "out":
                            n1 = class_add_mod["rdfs:label"]
                            n2 = extract_name_from_uri_or_curie(_parent["@id"])

                        # do not allow self-loops
                        if n1 != n2:
                            schema_graph_nx.add_edge(n1, n2, key=rel_label)
                elif type(parents) == dict:
                    if node_type == "in":
                        n1 = extract_name_from_uri_or_curie(_parent["@id"])
                        n2 = class_add_mod["rdfs:label"]

                    if node_type == "out":
                        n1 = class_add_mod["rdfs:label"]
                        n2 = extract_name_from_uri_or_curie(_parent["@id"])

                    # do not allow self-loops
                    if n1 != n2:
                        schema_graph_nx.add_edge(n1, n2, key=rel_label)

    return schema_graph_nx

def class_to_node(class_to_convert: dict) -> nx.Graph:
    G = nx.Graph()

    node = {}   # node to be added the above graph and returned
    for (k, v) in class_to_convert.items():
        if ":" in k:    # if ":" is present in key
            key = k.split(":")[1]
            node[key] = v
        elif "@" in k:  # if "@" is present in key
            key = k[1:]
            node[key] = v
        else:
            node[k] = v

    if "required" in node:
        if class_to_convert["sms:required"] == "sms:true":
            node["required"] = True
        else:
            node["required"] = False

    node["uri"] = class_to_convert["@id"]   # add separate "uri" key
    node["description"] = class_to_convert["rdfs:comment"] # separately store "comment" as "description"
    G.add_node(class_to_convert["rdfs:label"], **node)

    return G

def replace_node_in_schema(schema: nx.MultiDiGraph, class_add_mod: dict) -> None:
    # part of the code that replaces the modified class in the original JSON-LD schema (not in the data/ folder though)
    for i, schema_class in enumerate(schema["@graph"]):
        if schema_class["rdfs:label"] == class_add_mod["rdfs:label"]:
            validate_class_schema(class_add_mod)    # validate that the class to be modified follows the structure for any generic class (node)

            schema["@graph"][i] = class_add_mod
            break

def export_schema(schema, file_path):
    with open(file_path, 'w') as f:
        json.dump(schema, f, sort_keys = True, indent = 4, ensure_ascii = False)