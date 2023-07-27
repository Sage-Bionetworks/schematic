from typing import Dict
from schematic.utils.schema_util import get_label_from_display_name, get_display_name_from_label, convert_bool
from schematic.schemas.curie import uri2curie, curie2uri

class DataModelRelationships():
    def __init__(self) -> None:
        self.relationships_dictionary = self.define_data_model_relationships()
        #self.delimiters = ['@', ':']
        return

    def define_data_model_relationships(self) -> Dict:
        """ Define the relationships in the model so they can be accessed in a central location.
        If adding anew relationship make sure to follow the conventions closely.
            key:{ 
                jsonld_key:,

                csv_header: 
                jsonld_default: if at the end of processing there is no value present, this is the value we want to fill.
                    can also fill with type to ensure the key does not get deleted.
                edge_rel: True, if this relationship defines an edge
                                  False, if is a value relationship
                required_header: True, if relationship header is required for the csv
                node_dict: set default values for this relationship
                                    key is the node relationship name, value is the default value.
                                    If want to set default as a function create a nested dictionary.
                                        {'default': default_function, 
                                         'standard': alternative function to call if relationship is present for a node}
                    }                   If adding new functions to node_dict will
                                        need to modify data_model_nodes.generate_node_dict in 
                }
        TODO: 
        Key:
            jsonld_key: get_json_key_from_context
            csv_header:
            jsonld_default: if at the end of processing there is  no
            edge_rel:
            required_header:
            node_label:
            node_attr_dict:

        TODO: 
        - Functionally implement jsonld_edge key
        - Add JSONLD Directionality:
            Default Forward:
            Reverse Domain Includes
        - Add edge directionality:
            Default in.
            Out domainIncludes.
        TODO:
        - Use class inheritance to set up relationships.
        """
        map_data_model_relationships = {
                
                'displayName': {
                    'jsonld_key': 'sms:displayName',
                    'csv_header': 'Attribute',
                    'node_label': 'displayName',
                    'type': str,
                    'edge_rel': False,
                    'required_header': True,
                    'node_attr_dict':{'default': get_display_name_from_label,
                                    'standard': get_display_name_from_label,
                                    },
                },
               'label':{
                    'jsonld_key': 'rdfs:label',
                    'csv_header': None,
                    'node_label': 'label',
                    'type': str,
                    'edge_rel': False,
                    'required_header': False,
                    'node_attr_dict':{'default': get_label_from_display_name,
                                    'standard': get_label_from_display_name,
                                    },
                },
                'comment': {
                    'jsonld_key': 'rdfs:comment',
                    'csv_header': 'Description',
                    'node_label': 'comment',
                    'type': str,
                    'edge_rel': False,
                    'required_header': True,
                    'node_attr_dict':{'default': 'TBD'},
                },
                'rangeIncludes': {
                    'jsonld_key': 'schema:rangeIncludes',
                    'csv_header': 'Valid Values',
                    'edge_key': 'rangeValue',
                    'jsonld_direction': 'in',
                    'edge_dir': 'out',
                    'type': list,
                    'edge_rel': True,
                    'required_header': True,
                },
                'requiresDependency': {
                    'jsonld_key': 'sms:requiresDependency',
                    'csv_header': 'DependsOn',
                    'edge_key': 'requiresDependency',
                    'jsonld_direction': 'in',
                    'edge_dir': 'in',
                    'type': list,
                    'edge_rel': True,
                    'required_header': True,
                },
                'requiresComponent': {
                    'jsonld_key': 'sms:requiresComponent',
                    'csv_header': 'DependsOn Component',
                    'edge_key': 'requiresComponent',
                    'jsonld_direction': 'in',
                    'edge_dir': 'in',
                    'type': list,
                    'edge_rel': True,
                    'required_header': True,
                },
                'required': {
                    'jsonld_key': 'sms:required',
                    'csv_header': 'Required',
                    'node_label': 'required',
                    'type': str,
                    'edge_rel': False,
                    'required_header': True,
                    'node_attr_dict':{'default': 'sms:false',
                                      'standard': convert_bool,
                                },
                },
                'subClassOf': {
                    'jsonld_key': 'rdfs:subClassOf',
                    'csv_header': 'Parent',
                    'edge_key': 'parentOf',
                    'jsonld_direction': 'in',
                    'edge_dir': 'in',
                    'jsonld_default': [{"@id": "schema:Thing"}],
                    'type': list,
                    'edge_rel': True,
                    'required_header': True,
                },
                'validationRules': {
                    'jsonld_key': 'sms:validationRules',
                    'csv_header': 'Validation Rules',
                    'node_label': 'validationRules',
                    'jsonld_direction': 'in',
                    'edge_dir': 'in',
                    'jsonld_default': [],
                    'type': list,
                    'edge_rel': False,
                    'required_header': True,
                    'node_attr_dict':{'default': [],
                                },
                },
                'domainIncludes': {
                    'jsonld_key': 'schema:domainIncludes',
                    'csv_header': 'Properties',
                    'edge_key': 'domainValue',
                    'jsonld_direction': 'out',
                    'edge_dir': 'out',
                    'type': list,
                    'edge_rel': True,
                    'required_header': True,
                },
                'isPartOf': {
                    'jsonld_key': 'schema:isPartOf',
                    'csv_header': None,
                    'node_label': 'isPartOf',
                    'type': dict,
                    'edge_rel': False,
                    'required_header': False,
                    'node_attr_dict':{'default': {"@id": "http://schema.biothings.io"},
                                },
                },
                'id': {
                    'jsonld_key': '@id',
                    'csv_header': 'Source',
                    'node_label': 'uri',
                    'type': str,
                    'edge_rel': False,
                    'required_header': True,
                    'node_attr_dict':{'default': get_label_from_display_name,
                                    'standard': get_label_from_display_name,
                                    },
                },
            }

        return map_data_model_relationships

    def define_required_csv_headers(self):
        required_headers = []
        for k, v in self.relationships_dictionary.items():
            try:
                if v['required_header']:
                    required_headers.append(v['csv_header'])
            except KeyError:
                print(f"Did not provide a 'required_header' key, value pair for the nested dictionary {k} : {key}")

        return required_headers

    def define_edge_relationships(self):
        edge_relationships = {}
        for k, v in self.relationships_dictionary.items():
            try:
                if v['edge_rel']:
                    edge_relationships.update({k:v['csv_header']})
            except KeyError:
                print(f"Did not provide a 'edge_rel' key, value pair for the nested dictionary {k} : {key}")

        return edge_relationships

    def define_value_relationships(self):
        """
        Think about changing outputs.
        """
        value_relationships = {}
        for k, v in self.relationships_dictionary.items():
            try:
                if not v['edge_rel']:
                    value_relationships.update({k:v['csv_header']})
                    '''
                    if ':' in v['jsonld_key']:
                        value_relationships.update({k:v['jsonld_key'].split(':')[1]})
                    elif '@' in v['jsonld_key']:
                        value_relationships.update({k:v['jsonld_key'].split('@')[1]})
                    '''
            except KeyError:
                print(f"Did not provide a 'edge_rel' for key {k}")

        return value_relationships


