from dataclasses import dataclass, field, asdict
from dataclasses_json import config, dataclass_json
from functools import wraps
from typing import Any, Dict, Optional, Text, List
import networkx as nx

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.utils.schema_utils import get_label_from_display_name, convert_bool_to_str

@dataclass_json
@dataclass
class BaseTemplate:
    magic_context: str = field(default_factory=lambda: {"bts": "http://schema.biothings.io/",
                                                        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                                        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                                                        "schema": "http://schema.org/",
                                                        "xsd": "http://www.w3.org/2001/XMLSchema#",
                                                        },
                                metadata=config(field_name="@context"))
    magic_graph: str = field(default_factory=list, metadata=config(field_name="@graph"))
    magic_id: str = field(default="http://schema.biothings.io/#0.1", metadata=config(field_name="@id"))

@dataclass_json
@dataclass
class PropertyTemplate:
    magic_id: str = field(default="", metadata=config(field_name="@id"))
    magic_type: str = field(default="rdf:Property", metadata=config(field_name="@type"))
    magic_comment: str = field(default="", metadata=config(field_name="rdfs:comment"))
    magic_label: str = field(default="", metadata=config(field_name="rdfs:label"))
    magic_domain_includes: list = field(default_factory=list, metadata=config(field_name="schema:domainIncludes"))
    magic_range_includes: list = field(default_factory=list, metadata=config(field_name="schema:rangeIncludes"))
    magic_isPartOf: dict = field(default_factory=dict, metadata=config(field_name="schema:isPartOf"))
    magic_displayName:str = field(default="", metadata=config(field_name="sms:displayName"))
    magic_required: str = field(default="sms:false", metadata=config(field_name="sms:required"))
    magic_validationRules: list = field(default_factory=list, metadata=config(field_name="sms:validationRules"))

@dataclass_json
@dataclass
class ClassTemplate:
    magic_id: str = field(default="", metadata=config(field_name="@id"))
    magic_type: str = field(default="rdfs:Class", metadata=config(field_name="@type"))
    magic_comment: str = field(default="", metadata=config(field_name="rdfs:comment"))
    magic_label: str = field(default="", metadata=config(field_name="rdfs:label"))
    magic_subClassOf: list = field(default_factory=list, metadata=config(field_name="rdfs:subClassOf"))
    magic_range_includes: list = field(default_factory=list, metadata=config(field_name="schema:rangeIncludes"))
    magic_isPartOf: dict = field(default_factory=dict, metadata=config(field_name="schema:isPartOf"))
    magic_displayName:str = field(default="", metadata=config(field_name="sms:displayName"))
    magic_requiresDependency: list = field(default_factory=list, metadata=config(field_name="sms:requiresDependency"))
    magic_requiresComponent: list = field(default_factory=list, metadata=config(field_name="sms:requiresComponent"))
    magic_validationRules: list = field(default_factory=list, metadata=config(field_name="sms:validationRules"))

class DataModelJsonLD(object):
    '''
    #Interface to JSONLD_object
    '''

    def __init__(self, Graph: nx.MultiDiGraph, output_path:str = ''):
        # Setup
        self.graph = Graph # Graph would be fully made at this point.
        self.dmr = DataModelRelationships()
        self.rel_dict = self.dmr.relationships_dictionary
        self.DME = DataModelGraphExplorer(self.graph)
        self.output_path = output_path

        # Gather the templates
        base_template = BaseTemplate()
        self.base_jsonld_template = base_template.to_json()

        property_template = PropertyTemplate()
        self.property_template = property_template.to_json()

        class_template = ClassTemplate()
        self.class_template = class_template.to_json()

    def fill_entry_template(self, template:dict, node:str)->dict:
        """ Fill in a blank JSONLD entry template with information for each node. All relationships are filled from the graph, based on the type of information (node or edge)
        Args:
            template, dict: empty class or property template to be filled with information for the given node.
            node, str: target node to fill the template out for.
        Returns:
            template, dict: filled class or property template, that has been processed and cleaned up.
        """
        data_model_relationships = self.dmr.relationships_dictionary

        # For each field in template fill out with information from the graph
        for rel, rel_vals in data_model_relationships.items():
            
            key_context, key_rel = self.strip_context(context_value=rel_vals['jsonld_key'])

            # Fill edge information (done per edge type)
            if rel_vals['edge_rel']:
                # Get all edges associated with the current node
                node_edges = list(self.graph.in_edges(node, data=True))
                node_edges.extend(list(self.graph.out_edges(node,data=True)))

                # Get node pairs and weights for each edge
                for node_1, node_2, weight in node_edges:
                    
                    # Retrieve the relationship(s) and related info between the two nodes
                    node_edge_relationships = self.graph[node_1][node_2]

                    # Get the relationship edge key
                    edge_key = rel_vals['edge_key']

                    # Check if edge_key is even one of the relationships for this node pair.
                    if edge_key in node_edge_relationships:
                        # for each relationship between the given nodes
                        for relationship, weight_dict in node_edge_relationships.items():
                            # If the relationship defined and edge_key
                            if relationship == edge_key:
                                # TODO: rewrite to use edge_dir
                                if edge_key in ['domainIncludes', 'parentOf']:
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
                                                template[rel_vals['jsonld_key']].append(node_2_id)
                                            else:
                                                template[rel_vals['jsonld_key']] == node_2
            # Fill in node value information
            else:               
                node_label = rel_vals['node_label']
                
                # Get recorded info for current node, and the attribute type
                node_info = nx.get_node_attributes(self.graph, node_label)[node]
                
                # Add this information to the template
                template[rel_vals['jsonld_key']] =  node_info
        
        # Clean up template
        template = self.clean_template(template=template,
                                       data_model_relationships=data_model_relationships,
                                       )
        # Reorder lists based on weights:
        template = self.reorder_template_entries(template=template,)

        # Add contexts to certain values
        template = self.add_contexts_to_entries(template=template,)

        return template

    def add_contexts_to_entries(self, template:dict) -> dict:
        """
        Args:
            template, dict: JSONLD template that has been filled up to the current node, with information
        Returns:
            template, dict: JSONLD template where contexts have been added back to certain values.
        Note: This will likely need to be modified when Contexts are truly added to the model
        """
        for jsonld_key, entry in template.items():
            try:
                # Retrieve the relationships key using the jsonld_key
                key= [k for k, v in self.rel_dict.items() if jsonld_key == v['jsonld_key']][0]
            except:
                continue
            # If the current relationship can be defined with a 'node_attr_dict'
            if 'node_attr_dict' in self.rel_dict[key].keys():
                try:
                    # if possible pull standard function to get node information
                    rel_func = self.rel_dict[key]['node_attr_dict']['standard']
                except:
                    # if not pull default function to get node information
                    rel_func = self.rel_dict[key]['node_attr_dict']['default']

                # Add appropritae contexts that have been removed in previous steps (for JSONLD) or did not exist to begin with (csv)
                if key == 'id' and rel_func == get_label_from_display_name:
                    template[jsonld_key] = 'bts:' + template[jsonld_key]
                elif key == 'required' and rel_func == convert_bool_to_str:
                    template[jsonld_key] = 'sms:' + str(template[jsonld_key]).lower()
        return template

    def clean_template(self, template: dict, data_model_relationships: dict) -> dict:
        '''Get rid of empty k:v pairs. Fill with a default if specified in the relationships dictionary.
        Args:
            template, dict: JSONLD template for a single entry, keys specified in property and class templates.
            data_model_relationships, dict: dictionary containing information for each relationship type supported.
        Returns:
            template: JSONLD template where unfilled entries have been removed, or filled with default depending on specifications in the relationships dictionary.
        '''
        for rels in data_model_relationships.values():
            if rels['jsonld_key'] in template.keys() and not template[rels['jsonld_key']]:
                if 'jsonld_default' in rels.keys():
                    template[rels['jsonld_key']] = rels['jsonld_default']
                else:
                    del template[rels['jsonld_key']]
        return template

    def strip_context(self, context_value: str) -> tuple[str]:
        """Strip contexts from str entry.
        Args:
            context_value, str: string from which to strip context from
        Returns:
            context, str: the original context
            v, str: value separated from context
        """
        if ':' in context_value:
            context, v = context_value.split(':')
        elif '@' in context_value:
            context, v = context_value.split('@')
        return context, v

    def reorder_template_entries(self, template:dict) -> dict:
        '''In JSONLD some classes or property keys have list values. We want to make sure these lists are ordered according to the order supplied by the user.
        This will look specically in lists and reorder those.
        Args:
            template, dict: JSONLD template for a single entry, keys specified in property and class templates.
        Returns:
            template, dict: list entries re-ordered to match user supplied order.
        Note:
            User order only matters for nodes that are also attributes
        '''
        template_label = template['rdfs:label']

        for jsonld_key, entry in template.items():
            # Make sure dealing with an edge relationship:
            is_edge = ['True' for k, v in self.rel_dict.items() if v['jsonld_key']==jsonld_key if v['edge_rel'] == True]
            
            #if the entry is of type list and theres more than one value in the list attempt to reorder
            if is_edge and isinstance(entry, list) and len(entry)>1:
                # Get edge key from data_model_relationships using the jsonld_key:
                key, edge_key = [(k, v['edge_key']) for k, v in self.rel_dict.items() if jsonld_key == v['jsonld_key']][0]
                
                # Order edges
                sorted_edges = self.DME.get_ordered_entry(key=key, source_node_label=template_label)
                edge_weights_dict={edge:i for i, edge in enumerate(sorted_edges)}
                ordered_edges = [0]*len(edge_weights_dict.keys())
                for k,v in edge_weights_dict.items():
                    ordered_edges[v] = {'@id': 'bts:' + k}
                
                # Throw an error if ordered_edges does not get fully filled as expected.
                if 0 in ordered_edges:
                    logger.error("There was an issue getting values to match order specified in the data model, please submit a help request.")
                template[jsonld_key] = ordered_edges
        return template

    def generate_jsonld_object(self):
        '''Create the JSONLD object.
        Returns:
            jsonld_object, dict: JSONLD object containing all nodes and related information
        '''        
        # Get properties.
        properties = self.DME.find_properties()

        # Get JSONLD Template
        json_ld_template = self.base_jsonld_template
        
        # Iterativly add graph nodes to json_ld_template as properties or classes
        for node in self.graph.nodes:
            if node in properties:
                obj = self.fill_entry_template(template = self.property_template, node = node)
            else:
                obj = self.fill_entry_template(template = self.class_template, node = node)

            json_ld_template['@graph'].append(obj)
        return json_ld_template

def convert_graph_to_jsonld(Graph):
    # Make the JSONLD object
    data_model_jsonld_converter = DataModelJsonLD(Graph=Graph)
    jsonld_dm = data_model_jsonld_converter.generate_jsonld_object()
    return jsonld_dm

