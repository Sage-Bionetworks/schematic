import networkx as nx
import json

from ingresspipe.utils.curie_utils import extract_name_from_uri_or_curie

def load_schema_into_networkx(schema):
    G = nx.MultiDiGraph()
    for record in schema["@graph"]:
       
        # TODO: clean up obsolete code 
        #if record["@type"] == "rdfs:Class":
            
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

            if "rdfs:subClassOf" in record:
                parents = record["rdfs:subClassOf"]
                if type(parents) == list:
                    for _parent in parents:
                        n1 = extract_name_from_uri_or_curie(_parent["@id"])
                        n2 = record["rdfs:label"]

                        # do not allow self-loops
                        if n1 != n2:
                            G.add_edge(n1, n2, relationship = "parentOf")
                elif type(parents) == dict:
                    n1 = extract_name_from_uri_or_curie(parents["@id"])
                    n2 = record["rdfs:label"]

                    # do not allow self-loops
                    if n1 != n2:
                        G.add_edge(n1, n2, relationship = "parentOf")

            # TODO: refactor: abstract adding relationship method
            if "sms:requiresDependency" in record:
                dependencies = record["sms:requiresDependency"]
                if type(dependencies) == list:
                    for _dep in dependencies:
                        n1 = record["rdfs:label"]  
                        n2 = extract_name_from_uri_or_curie(_dep["@id"]) 
                        # do not allow self-loops
                        if n1 != n2:
                            G.add_edge(n1, n2, relationship = "requiresDependency")

            if "sms:requiresComponent" in record:
                components = record["sms:requiresComponent"]
                if type(components) == list:
                    for _comp in components:
                        n1 = record["rdfs:label"]  
                        n2 = extract_name_from_uri_or_curie(_comp["@id"]) 
                        # do not allow self-loops
                        if n1 != n2:
                            G.add_edge(n1, n2, relationship = "requiresComponent")

            if "schema:rangeIncludes" in record:
                range_nodes = record["schema:rangeIncludes"]
                if type(range_nodes) == list:
                    for _range_node in range_nodes:
                        n1 = record["rdfs:label"]  
                        n2 = extract_name_from_uri_or_curie(_range_node["@id"]) 
                        # do not allow self-loops
                        if n1 != n2:
                            G.add_edge(n1, n2, relationship = "rangeValue")

                elif type(range_nodes) == dict:
                    n1 = record["rdfs:label"]  
                    n2 = extract_name_from_uri_or_curie(range_nodes["@id"]) 
                    # do not allow self-loops
                    if n1 != n2:
                        G.add_edge(n1, n2, relationship = "rangeValue")
            
            if "requiresChildAsValue" in node and node["requiresChildAsValue"]["@id"] == "sms:True":
                node["requiresChildAsValue"] = True
            
            if "required" in node:
                if "sms:true" == record["sms:required"]:
                    node["required"] = True  
                else:
                    node["required"] = False


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

def export_schema(self, file_path):
    with open(file_path, 'w') as f:
        json.dump(self.schema, f, sort_keys = True, indent = 4,
            ensure_ascii = False)
