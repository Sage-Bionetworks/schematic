import networkx as nx
import json

from schematic.utils.curie_utils import extract_name_from_uri_or_curie

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

def export_schema(schema, file_path):
    with open(file_path, 'w') as f:
        json.dump(schema, f, sort_keys = True, indent = 4, ensure_ascii = False)
