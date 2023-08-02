from functools import wraps
from typing import Any, Dict, Optional, Text, List
import networkx as nx

from schematic.schemas.data_model_graph import DataModelGraphExporer
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.utils.schema_util import get_label_from_display_name, get_display_name_from_label, convert_bool


class DataModelJsonLD(object):
    '''
    #Interface to JSONLD_object
    '''

    def __init__(self, Graph: nx.MultiDiGraph):
        # Setup
        self.graph = Graph
        self.dmr = DataModelRelationships()
        self.rel_dict = self.dmr.relationships_dictionary
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

    def create_object(self, template, node):
        data_model_relationships = self.dmr.relationships_dictionary
        #edge_to_jsonld_keys = {rel_vals['edge_key']: rel_vals['jsonld_key'] for rel, rel_vals in data_model_relationships.items() if rel_vals['edge_rel']}

        # For each field in template fill out with information from the graph
        for rel, rel_vals in data_model_relationships.items():
            
            key_context, key_rel = self.strip_context(context_value=rel_vals['jsonld_key'])

            # Fill edge information (done per edge type)
            if rel_vals['edge_rel']:
                # Get all edges associated with the current node
                node_edges = list(self.graph.in_edges(node, data=True))
                node_edges.extend(list(self.graph.out_edges(node,data=True)))

                for node_1, node_2, weight in node_edges:
                    # Get 'AtlasView'('relationship':{weight:value}) of edge
                    # need to convert this relationship back to the JSONLD key_rel
                    node_edge_relationships = self.graph[node_1][node_2]
                    edge_rel = rel_vals['edge_key']


                    
                    #node_edge_key_rels  = [for rel in node_edge_relationships.keys]
                    
                    # Check if key_rel is even one of the relationships for this node pair.
                    #if key_rel in node_edge_relationships:
                    if edge_rel in node_edge_relationships:
                        
                        for relationship, weight_dict in node_edge_relationships.items():
                            #if relationship == key_rel:
                            if relationship == edge_rel:
                                
                                if edge_rel in ['domainIncludes', 'parentOf']:
                                    #breakpoint()
                                    if node_2 == node:
                                        # Make sure the key is in the template (differs between properties and classes)
                                        if rel_vals['jsonld_key'] in template.keys():
                                            node_1_id = {'@id': 'bts:'+node_1}
                                            # TODO Move this to a helper function to clear up.
                                            if (isinstance(template[rel_vals['jsonld_key']], list) and
                                                node_1_id not in template[rel_vals['jsonld_key']]):
                                                template[rel_vals['jsonld_key']].append(node_1_id)
                                            else:
                                                template[rel_vals['jsonld_key']] == node_1
                                else:
                                    if node_1 == node:
                                        # Make sure the key is in the template (differs between properties and classes)
                                        if rel_vals['jsonld_key'] in template.keys():
                                            node_2_id = {'@id': 'bts:'+node_2}
                                            # TODO Move this to a helper function to clear up.
                                            if (isinstance(template[rel_vals['jsonld_key']], list) and
                                                node_2_id not in template[rel_vals['jsonld_key']]):
                                                # could possibly keep track of weights here but that might slow things down
                                                template[rel_vals['jsonld_key']].append(node_2_id)
                                            else:
                                                template[rel_vals['jsonld_key']] == node_2
            else:               
                # attribute here refers to node attibutes (come up with better name.)
                node_attribute_name = rel_vals['node_label']
                # Get recorded info for current node, and the attribute type
                node_info = nx.get_node_attributes(self.graph, node_attribute_name)[node]
                # Add this information to the template
                template[rel_vals['jsonld_key']] =  node_info
        # Clean up template
        template = self.clean_template(template=template,
                                       data_model_relationships=data_model_relationships,
                                       )
        # Reorder lists based on weights:
        template = self.reorder_template_entries(template=template,)

        # Add contexts back
        template = self.add_contexts_to_entries(template=template,)

        return template

    def add_contexts_to_entries(self, template):
        for jsonld_key, entry in template.items():
            try:
                key= [k for k, v in self.rel_dict.items() if jsonld_key == v['jsonld_key']][0]
            except:
                continue
            if 'node_attr_dict' in self.rel_dict[key].keys():
                # Changes to data_model_relationships may mean this part will need to be updated.
                try:
                    rel_func = self.rel_dict[key]['node_attr_dict']['standard']
                except:
                    rel_func = self.rel_dict[key]['node_attr_dict']['default']
                if key == 'id' and rel_func == get_label_from_display_name:
                    template[jsonld_key] = 'bts:' + template[jsonld_key]
                elif key == 'required' and rel_func == convert_bool:
                    #clean up use of convert bool here.
                    template[jsonld_key] = 'sms:' + str(template[jsonld_key]).lower()
        return template

    def clean_template(self, template, data_model_relationships):
        '''
        Get rid of empty k:v pairs. Fill with a default if specified in the relationships dictionary.
        '''
        for rels in data_model_relationships.values():
            if rels['jsonld_key'] in template.keys() and not template[rels['jsonld_key']]:
                if 'jsonld_default' in rels.keys():
                    template[rels['jsonld_key']] = rels['jsonld_default']
                else:
                    del template[rels['jsonld_key']]
        return template

    def strip_context(self, context_value):
        if ':' in context_value:
            context, v = context_value.split(':')
        elif '@' in context_value:
            context, v = context_value.split('@')
        return context, v

    def reorder_template_entries(self, template):
        '''In JSONLD some classes or property keys have list values. We want to make sure these lists are ordered according to the order supplied by the user.
        This will look specically in lists and reorder those.
        Args:
            template (dict):
        Returns:
            template (dict): list entries re-ordered to match user supplied order.

        '''
       

        # user order only matters for nodes that are also attributes
        template_label = template['rdfs:label']

        for jsonld_key, entry in template.items():
            #if the entry is of type list and theres more than one value in the list attempt to reorder
            if isinstance(entry, list) and len(entry)>1:
                # Get edge key from data_model_relationships using the jsonld_key:
                key, edge_key = [(k, v['edge_key']) for k, v in self.rel_dict.items() if jsonld_key == v['jsonld_key']][0]

                # Order edges
                sorted_edges = self.DME.get_ordered_entry(key=key, source_node_label=template_label)
                edge_weights_dict={edge:i for i, edge in enumerate(sorted_edges)}
                ordered_edges = [0]*len(edge_weights_dict.keys())
                for k,v in edge_weights_dict.items():
                    ordered_edges[v] = {'@id': 'bts:' + k}
                
                # TODO: Throw an error if ordered_edges does not get fully filled as expected.
                if 0 in ordered_edges:
                    breakpoint()
                template[jsonld_key] = ordered_edges
        return template

    def property_template(self):
        '''
        '''      
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
        '''        
        # Get properties.
        properties = self.DME.find_properties()
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

