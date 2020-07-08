import os
import string
import json

import tabletext
from rdflib import Graph, Namespace, plugin, query
import inflection
import networkx as nx
from networkx.algorithms.cycles import find_cycle

from ingresspipe.utils.curie_utils import expand_curies_in_schema, uri2label, extract_name_from_uri_or_curie
from ingresspipe.utils.general import find_duplicates
from ingresspipe.utils.io_utils import load_default, load_json, load_schemaorg
from ingresspipe.utils.schema_utils import load_schema_into_networkx
from ingresspipe.utils.general import dict2list, unlist
from ingresspipe.utils.viz_utils import visualize
from ingresspipe.utils.validate_utils import validate_class_schema, validate_property_schema, validate_schema

from .curie import uri2curie, curie2uri

namespaces = dict(rdf=Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))

class SchemaExplorer():
    """Class for exploring schema
    """
    def __init__(self):
        self.load_default_schema()

    def load_schema(self, schema):
        """Load schema and convert it to networkx graph
        """
        self.schema = load_json(schema)
        self.schema_nx = load_schema_into_networkx(self.schema)

    def export_schema(self, file_path):
        with open(file_path, 'w') as f:
            json.dump(self.schema, f, sort_keys = True, indent = 4, ensure_ascii = False)


    def load_default_schema(self):
        """Load default schema, either schema.org or biothings
        """
        self.schema = load_default()
        self.schema_nx = load_schema_into_networkx(self.schema)

    def get_nx_schema(self):
        return self.schema_nx

    def is_class_in_schema(self, class_label):
        if self.schema_nx.node[class_label]:
            return True
        else:
            return False

    def full_schema_graph(self, size=None):
        edges = self.schema_nx.edges()
        return visualize(edges, size=size)

    def sub_schema_graph(self, source, direction, size=None):
        if direction == 'down':
            edges = list(nx.edge_bfs(self.schema_nx, [source]))
            return visualize(edges, size=size)
        elif direction == 'up':
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

    def find_children_classes(self, schema_class):
        return list(self.schema_nx.successors(schema_class))

    def find_parent_classes(self, schema_class):
        """Find all parents of the class
        """

        digraph = self.get_digraph_by_edge_type("parentOf")
        
        root_node = list(nx.topological_sort(digraph))[0]
        # root_node = list(nx.topological_sort(self.schema_nx))[0]
        
        paths = nx.all_simple_paths(self.schema_nx,
                                    source=root_node,
                                    target=schema_class)
        # print(root_node)
        return [_path[:-1] for _path in paths]

    def find_class_specific_properties(self, schema_class):
        """Find properties specifically associated with a given class
        """
        #print(schema_class)
        schema_uri = self.schema_nx.node[schema_class]["uri"]
        properties = []
        for record in self.schema["@graph"]:
            if record['@type'] == "rdf:Property":
                if type(record["schema:domainIncludes"]) == dict and record["schema:domainIncludes"]["@id"] == schema_uri:
                    properties.append(record["rdfs:label"])
                elif type(record["schema:domainIncludes"]) == list and [item for item in record["schema:domainIncludes"] if item["@id"] == schema_uri] != []:
                    properties.append(record["rdfs:label"])
        return properties

    def find_all_class_properties(self, schema_class, display_as_table=False):
        """Find all properties associated with a given class
        # TODO : need to deal with recursive paths
        """
        parents = self.find_parent_classes(schema_class)
        #print(schema_class)
        #print(parents)
        properties = [{'class': schema_class,
                       'properties': self.find_class_specific_properties(schema_class)}]
        for path in parents:
            path.reverse()
            for _parent in path:
                #print(_parent)
                properties.append({"class": _parent,
                                    "properties": self.find_class_specific_properties(_parent)})
        if not display_as_table:
            return properties
        else:
            content = [['Property', 'Expected Type', 'Description', 'Class']]
            for record in properties:
                for _property in record['properties']:
                    property_info = self.explore_property(_property)
                    if "range" in property_info:
                        content.append([_property, property_info['range'],
                                    property_info['description'],
                                    record['class']])
                    else:
                        content.append([_property,
                                    property_info['description'],
                                    record['class']])

            print(tabletext.to_text(content))

    def find_class_usages(self, schema_class):
        """Find where a given class is used as a value of a property
        """
        usages = []
        schema_uri = self.schema_nx.node[schema_class]["uri"]
        for record in self.schema["@graph"]:
            usage = {}
            if record["@type"] == "rdf:Property":
                if "schema:rangeIncludes" in record:
                    p_range = dict2list(record["schema:rangeIncludes"])
                    for _doc in p_range:
                        if _doc['@id'] == schema_uri:
                            usage["property"] = record["rdfs:label"]
                            p_domain = dict2list(record["schema:domainIncludes"])
                            usage["property_used_on_class"] = unlist([self.uri2label(record["@id"]) for record in p_domain])
                            usage["description"] = record["rdfs:comment"]
            if usage:
                usages.append(usage)
        return usages

    def find_child_classes(self, schema_class):
        """Find schema classes that inherit from the given class
        """
        return unlist(list(self.schema_nx.successors(schema_class)))

    def explore_class(self, schema_class):
        """Find details about a specific schema class
        """
        subclasses = []
        if  "subClassOf" in self.schema_nx.node[schema_class]:
            # the below if/else block exists to solve the inconsitencies in the spec of "subClassOf" in the HTAN schema
            # a few classes are specified as lists and a few as simple dicts
            schema_node_val = self.schema_nx.node[schema_class]["subClassOf"]

            subclass_list = []
            if isinstance(schema_node_val, dict):
                subclass_list.append(self.schema_nx.node[schema_class]["subClassOf"])
            else:
                subclass_list = schema_node_val
            
            for subclass in subclass_list:
                subclasses.append(extract_name_from_uri_or_curie(subclass["@id"]))
        
        requires_range = []
        if  "rangeIncludes" in self.schema_nx.node[schema_class]:
            # the below if/else block exists to solve the inconsitencies in the spec of "rangeIncludes" in the HTAN schema
            # a few classes are specified as lists and a few as simple dicts
            schema_node_val = self.schema_nx.node[schema_class]["rangeIncludes"]

            if isinstance(schema_node_val, dict):
                subclass_list = []
                subclass_list.append(self.schema_nx.node[schema_class]["rangeIncludes"])
            else:
                subclass_list = schema_node_val

            for range_class in subclass_list:
                requires_range.append(extract_name_from_uri_or_curie(range_class["@id"]))

        requires_dependencies = []
        if  "requiresDependency" in self.schema_nx.node[schema_class]:
            # the below if/else block exists to solve the inconsitencies in the spec of "requiresDependency" in the HTAN schema
            # a few classes are specified as lists and a few as simple dicts
            schema_node_val = self.schema_nx.node[schema_class]["requiresDependency"]

            if isinstance(schema_node_val, dict):
                subclass_list = []
                subclass_list.append(self.schema_nx.node[schema_class]["requiresDependency"])
            else:
                subclass_list = schema_node_val
                
            for dep_class in subclass_list:
                requires_dependencies.append(extract_name_from_uri_or_curie(dep_class["@id"])) 

        requires_components = []
        if  "requiresComponent" in self.schema_nx.node[schema_class]:
            # the below if/else block exists to solve the inconsitencies in the spec of "requiresComponent" in the HTAN schema
            # a few classes are specified as lists and a few as simple dicts
            schema_node_val = self.schema_nx.node[schema_class]["requiresComponent"]

            if isinstance(schema_node_val, dict):
                subclass_list = []
                subclass_list.append(self.schema_nx.node[schema_class]["requiresComponent"])
            else:
                subclass_list = schema_node_val

            for comp_dep_class in subclass_list:
                requires_components.append(extract_name_from_uri_or_curie(comp_dep_class["@id"])) 

        required = False
        if "required" in self.schema_nx.node[schema_class]:
            required = self.schema_nx.node[schema_class]["required"]

        validation_rules = []
        if "validationRules" in self.schema_nx.node[schema_class]:
            validation_rules = self.schema_nx.node[schema_class]["validationRules"]

        # TODO: make class_info keys here the same as keys in schema graph nodes(e.g. schema_class above); note that downstream code using explore_class would have to be updated as well (e.g. csv_2_schemaorg)
      
        class_info = {'properties': self.find_all_class_properties(schema_class),
                      'description': self.schema_nx.node[schema_class]['description'],
                      'uri': curie2uri(self.schema_nx.node[schema_class]["uri"], namespaces),
                      'usage': self.find_class_usages(schema_class),
                      'child_classes': self.find_child_classes(schema_class),
                      'subClassOf': subclasses, 
                      'range': requires_range,
                      'dependencies': requires_dependencies,
                      'validation_rules': validation_rules,
                      'required': required,
                      'component_dependencies': requires_components,
                      'parent_classes': self.find_parent_classes(schema_class)
        }

        if "displayName" in self.schema_nx.node[schema_class]:
            class_info['displayName'] = self.schema_nx.node[schema_class]['displayName']

        return class_info
    

    def get_property_label_from_display_name(self, display_name):
        """Convert a given display name string into a proper property label string
        """
        ''' 
        label = ''.join(x.capitalize() or ' ' for x in display_name.split(' '))
        label = label[:1].lower() + label[1:] if label else ''
        '''
        display_name = display_name.translate({ord(c): None for c in string.whitespace})
        
        label = inflection.camelize(display_name.strip(), uppercase_first_letter=False)
        return label

    def get_class_label_from_display_name(self, display_name):
        """Convert a given display name string into a proper class label string
        """
        '''
        label = ''.join(x.capitalize() or ' ' for x in display_name.split(' '))'''
        display_name = display_name.translate({ord(c): None for c in string.whitespace})
        label = inflection.camelize(display_name.strip(), uppercase_first_letter=True)

        return label

    def get_class_by_property(self, property_display_name):
        schema_property = self.get_property_label_from_display_name(property_display_name)

        for record in self.schema["@graph"]:
            if record["@type"] == "rdf:Property":
                if record["rdfs:label"] == schema_property:
                    p_domain = dict2list(record["schema:domainIncludes"])
                    return unlist([self.uri2label(schema_class["@id"]) for schema_class in p_domain])
        
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
                    property_info["domain"] = unlist([self.uri2label(record["@id"]) for record in p_domain])
                    if "schema:rangeIncludes" in record:
                        p_range = dict2list(record["schema:rangeIncludes"])
                        property_info["range"] = [self.uri2label(record["@id"]) for record in p_range]
                    else:
                        property_info["range"] = []
   
                    if "sms:required" in record:
                        if "sms:true" == record["sms:required"]:
                            property_info["required"] = True  
                        else: 
                            property_info["required"] = False 

                    validation_rules = []
                    if "sms:validationRules" in record:
                        property_info["validation_rules"] = record["sms:validationRules"]
                            

                    if  "sms:requiresDependency" in record:
                        p_dependencies = dict2list(record["sms:requiresDependency"])
                        property_info["dependencies"] = [self.uri2label(record["@id"]) for record in p_dependencies]
                    else:
                        property_info["dependencies"] = []

                    if "sms:displayName" in record:
                        property_info['displayName'] = record['sms:displayName']

                    break
        
        #check if properties are added multiple times
        
        return property_info

    def generate_class_template(self):
        """Generate a template for schema class
        """
        template = {
            "@id": "uri or curie of the class",
            "@type": "rdfs:Class",
            "rdfs:comment": "description of the class",
            "rdfs:label": "class label, should match @id",
            "rdfs:subClassOf": {
                "@id": "parent class, could be list"
            },
            "schema:isPartOf": {
                "@id": "http://schema.biothings.io"
            }
        }
        return template

    def generate_property_template(self):
        """Generate a template for schema property
        """
        template = {
            "@id": "url or curie of the property",
            "@type": "rdf:Property",
            "rdfs:comment": "description of the property",
            "rdfs:label": "carmel case, should match @id",
            "schema:domainIncludes": {
                "@id": "class which use it as a property, could be list"
            },
            "schema:isPartOf": {
                "@id": "http://schema.biothings.io"
            },
            "schema:rangeIncludes": {
                "@id": "relates a property to a class that constitutes (one of) the expected type(s) for values of the property"
            }
        }
        return template

    def edit_class(self, class_info):
        """Edit an existing class into schema
        """ 
        for i, schema_class in enumerate(self.schema["@graph"]):
            if schema_class["rdfs:label"] == class_info["rdfs:label"]:
                validate_class_schema(class_info)

                self.schema["@graph"][i] = class_info
                break

        # TODO: do we actually need to validate the entire schema if a class is just edited and the class passes validation?
        #validate_schema(self.schema)
        print("Edited the class {} successfully!".format(class_info["rdfs:label"]))
        self.schema_nx = load_schema_into_networkx(self.schema)

    def update_class(self, class_info):
        """Add a new class into schema
        """
        #print(class_info)
        validate_class_schema(class_info)
        self.schema["@graph"].append(class_info)
        validate_schema(self.schema)
        print("Updated the class {} successfully!".format(class_info["rdfs:label"]))
        self.schema_nx = load_schema_into_networkx(self.schema)
        
    def edit_property(self, property_info):
        """Edit an existing property into schema
        """
        for i, schema_property in enumerate(self.schema["@graph"]):
            if schema_property["rdfs:label"] == property_info["rdfs:label"]:
                validate_property_schema(property_info)
                self.schema["@graph"][i] = property_info
               
                #TODO: check if properties are added/edited multiple times (e.g. look at explore_property)
                break
        
        validate_schema(self.schema)
        print("Edited the property {} successfully!".format(property_info["rdfs:label"]))
        self.schema_nx = load_schema_into_networkx(self.schema)

    def update_property(self, property_info):
        """Add a new property into schema
        """
        validate_property_schema(property_info)
        self.schema["@graph"].append(property_info)
        validate_schema(self.schema)
        print("Updated the property {} successfully!".format(property_info["rdfs:label"]))

    def get_digraph_by_edge_type(self, edge_type):

        multi_digraph = self.schema_nx   

        digraph = nx.DiGraph()
        for edge in multi_digraph.edges(data = True, keys = True):
            if edge[3]["relationship"] == edge_type:
                digraph.add_edge(edge[0], edge[1])

        #print(nx.find_cycle(digraph, orientation = "ignore"))

        return digraph
