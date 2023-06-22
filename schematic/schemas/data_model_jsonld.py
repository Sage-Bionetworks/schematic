from functools import wraps
from typing import Any, Dict, Optional, Text, List
import networkx as nx

from schematic.schemas.data_model_graph import DataModelGraphExporer
from schematic.schemas.data_model_relationships import DataModelRelationships


class DataModelJsonLD(object):
    '''
    #Interface to JSONLD_object
    '''

    def __init__(self, Graph: nx.MultiDiGraph):
        # Setup
        self.graph = Graph
        self.dmr = DataModelRelationships()
        '''
        self.jsonld_object = JSONLD_object(DataModelJsonLD)
        self.jsonld_class = JSONLD_class(self.jsonld_object)
        self.jsonld_property = JSONLD_property(self.jsonld_object)
        '''
        self.DME = DataModelGraphExporer(self.graph)

    
    def base_jsonld_template(self):
        """
        #Base starter template, to be filled out with model. For entire file.
        TODO: when done adding contexts fill out this section here.
        """
        base_template = {"@context": {},
                         "@graph": [],
                         "@id": "http://schema.biothings.io/#0.1",
                        }
        return base_template
    
    def add_contexts(self):
        breakpoint()
        return

    def create_object(self, template, node):
        """
        create a single JSONLD object per node
        Use the relationship dictionary
        """
        data_model_relationships = self.dmr.relationships_dictionary

        #template_keys = list(template.keys())
        #template_keys.remove('@type')

        # For each field in template fill out with information from the graph
        #for jsonld_key in template_keys:
        for rel, rel_vals in data_model_relationships.items():
            # Get column name linked to node. Need to do this now bc of relationship_dict structure
            #node_column_name = list(data_model_relationships[jsonld_key].keys())[0]

            # Fill edge information (done per edge type)
            if rel_vals['edge_rel']:
            #if data_model_relationships[jsonld_key][node_column_name]['edge_rel']:
                for node_1, node_2, relationship in self.graph.edges:
                    key_context, key_rel = rel_vals['jsonld_key'].split(':')
                    if relationship == key_rel:
                        '''
                        if relationship in ['domainIncludes', 'subClassOf']:
                        #if relationship in ['domainIncludes', 'subClassOf']:
                            if node_1 == node:
                                if node_1 == 'Patient' and node_2 == 'HTANParticipantID':
                                    breakpoint()
                                # use display names for the nodes
                                node_2_id = {'@id': 'bts:'+ node_2}
                                try:
                                    if isinstance(template[rel_vals['jsonld_key']], list):
                                        # TODO Format ids properly in future to take in proper context
                                        template[rel_vals['jsonld_key']].append(node_2_id)
                                    else:
                                        template[rel_vals['jsonld_key']] == node_2
                                except:
                                    breakpoint()
                        else:
                            if node_2 == node:
                                # use display names for the nodes
                                node_1_id = {'@id': 'bts:'+node_1}
                                try:
                                    if isinstance(template[rel_vals['jsonld_key']], list):
                                        # TODO Format ids properly in future to take in proper context
                                        template[rel_vals['jsonld_key']].append(node_1_id)
                                    else:
                                        template[rel_vals['jsonld_key']] == node_1
                                except:
                                    breakpoint()
                        '''
                        if node_2 == node:
                            # use display names for the nodes
                            node_1_id = {'@id': 'bts:'+node_1}
                            try:
                                if isinstance(template[rel_vals['jsonld_key']], list):
                                    # TODO Format ids properly in future to take in proper context
                                    template[rel_vals['jsonld_key']].append(node_1_id)
                                else:
                                    template[rel_vals['jsonld_key']] == node_1
                            except:
                                breakpoint()


            # Fill node information
            else:
                #if 'node_dict' in data_model_relationships[key][node_column_name].keys():
                # attribute here refers to node attibutes (come up with better name.)
                #node_attribute_name = list(data_model_relationships[jsonld_key][node_column_name]['node_dict'].keys())[0]
                node_attribute_name = rel_vals['node_label']
                # Get recorded info for current node, and the attribute type
                node_info = nx.get_node_attributes(self.graph, node_attribute_name)[node]

                # Add this information to the template
                template[rel_vals['jsonld_key']] =  node_info
        return template

    def property_template(self):
        '''
        TODO: Create this from relationship class
        '''
        # Default required to False but add validation for this in the future.
        # Only allowing a single class type, other models could have lists.
        
        # Domain includes needs to pull a dict id {'@id': 'mutations'}

        property_template = {
                            "@id": "",
                            "@type": "rdf:Property",
                            "rdfs:comment": "",
                            "rdfs:label": "",
                            "schema:domainIncludes": [],
                            "schema:rangeIncludes": [],
                            "schema:isPartOf": {},
                            "sms:displayName": "",
                            "sms:required": "sms:false",
                            "sms:validationRules": [],
                            }
        return property_template

    def class_template(self):
        """
        Only allowing a single class type, other models could have lists.
        """
        class_template = {
                        "@id": "",
                        "@type": "rdfs:Class",
                        "rdfs:comment": "",
                        "rdfs:label": "",
                        "rdfs:subClassOf": [],
                        "schema:isPartOf": {},
                        "schema:rangeIncludes": [],
                        "sms:displayName": "",
                        "sms:required": "sms:false",
                        "sms:requiresDependency": [],
                        "sms:requiresComponent": [],
                        "sms:validationRules": [],
                    }
        return class_template


    def generate_jsonld_object(self):
        '''
        #Will call JSONLD_object class to create properties and classes in the process.
        '''
        
        # Get properties.
        properties = self.DME.find_properties()
        #classes = self.DME.find_classes()

        # Get JSONLD Template
        self.json_ld_object = self.base_jsonld_template()
        
        # Iterativly add graph nodes to json_ld_object as properties and classes
        for node in self.graph.nodes:
            if node in properties:
                obj = self.create_object(template = self.property_template(), node = node)
            else:
                obj = self.create_object(template = self.class_template(), node = node)
            self.json_ld_object['@graph'].append(obj)
        return self.json_ld_object

"""
class DataModelJsonLD(object):
    '''
    #Interface to JSONLD_object
    '''

    def __init__(self, Graph: nx.MultiDiGraph):
        # Setup
        self.graph = Graph
        self.jsonld_object = JSONLD_object(DataModelJsonLD)
        self.jsonld_class = JSONLD_class(self.jsonld_object)
        self.jsonld_property = JSONLD_property(self.jsonld_object)
        self.DME = DataModelGraphExporer(self.graph)

    def generate_jsonld_object(self):
        '''
        #Will call JSONLD_object class to create properties and classes in the process.
        '''
        
        # Get properties and classes.
        properties = self.DME.find_properties()
        classes = self.DME.find_classes()

        # Get JSONLD Template
        template = JSONLD_object
        base

        # Generate properties and classes and add to the template.

        return

    def base_jsonld_template(self):
        '''
        #Base starter template, to be filled out with model.
        '''
        return

class JSONLD_object(DataModelJsonLD):
    '''
    #Decorator class design
    #Base decorator class.
    '''
    _DataModelJsonLD: DataModelJsonLD = None

    def __init__(self, DataModelJsonLD) -> None:
        self.dataModelJsonLD = DataModelJsonLD

    def _create_template(self) -> DataModelJsonLD:
        '''
        Returns jsonld_class_template or jsonld_property_template
        '''
        return self._DataModelJsonLD

    @property
    def to_template(self):
        return self._DataModelJsonLD.to_template()

    

class JSONLD_property(JSONLD_object):
    '''
    Property Decorator
    '''
    def to_template(self):
        return JSONLD_property(self._DataModelJsonLD.to_template())

    def explore_property():
        return

    def edit_property():
        return

class JSONLD_class(JSONLD_object):
    '''
    Class Decorator
    '''
    def to_template(self):
        return JSONLD_class(self._DataModelJsonLD.to_template())

    def explore_class():
        return

    def edit_class():
        return
"""
def convert_graph_to_jsonld(Graph):
    # Make the JSONLD object
    data_model_jsonld_converter = DataModelJsonLD(Graph=Graph)
    jsonld_dm = data_model_jsonld_converter.generate_jsonld_object()
    
    return jsonld_dm

