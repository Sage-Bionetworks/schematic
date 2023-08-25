from inspect import isfunction
from rdflib import Namespace

from schematic.schemas.data_model_relationships import (
    DataModelRelationships
    )

from schematic.utils.schema_utils import get_label_from_display_name, get_display_name_from_label, convert_bool, parse_validation_rules
from schematic.utils.validate_rules_utils import validate_schema_rules
from schematic.schemas.curie import uri2curie, curie2uri


class DataModelNodes():
    def __init__(self, attribute_relationships_dict):
        self.namespaces = dict(rdf=Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
        self.data_model_relationships = DataModelRelationships()
        self.value_relationships = self.data_model_relationships.define_value_relationships()
        self.edge_relationships_dictionary = self.data_model_relationships.define_edge_relationships()
        self.ar_dict = attribute_relationships_dict
        # Identify all properties
        self.properties = self.get_data_model_properties(ar_dict=self.ar_dict)
        
        
        return

    def node_present(self, G, node_name):
        if node_name in G.nodes():
            return True
        else:
            return False
    
    def gather_nodes(self, attr_info):
        """Take in a tuple containing attriute name and relationship dictionary, and find all nodes defined in attribute information.
        Args:

        Returns:
            list, nodes defined by attribute_info as being related to that attribute.
        """
        # retrieve a list of relationship types that will produce nodes.
        self.node_relationships =list(self.edge_relationships_dictionary.values())

        # Extract attribure and relationship dictionary
        attribute, relationship = attr_info
        relationships = relationship['Relationships']

        nodes = []
        if attribute not in nodes:
            nodes.append(attribute)
        for rel in self.node_relationships:
            if rel in relationships.keys():
                nodes.extend([node.strip()
                                for node in relationships[rel]])
        return nodes

    def gather_all_nodes(self, data_model):
        """
        Args:

        Returns:
        """
        all_nodes = []
        for attr_info in data_model.items():
            nodes = self.gather_nodes(attr_info=attr_info)
            all_nodes.extend(nodes)
        all_nodes = list(dict.fromkeys(all_nodes).keys())
        return all_nodes

    def get_rel_default_info(self, relationship):
        """
        For each display name fill out defaults. Maybe skip default.
        """
        for k,v in self.data_model_relationships.relationships_dictionary.items():
            if k == relationship:
                if 'node_attr_dict' in v.keys():
                    rel_key = v['node_label']
                    rel_default = v['node_attr_dict']
                    return rel_key, rel_default

    def get_data_model_properties(self, ar_dict):
        properties=[]
        for attribute, relationships in ar_dict.items():
            if 'Properties' in relationships['Relationships'].keys():
                properties.extend(relationships['Relationships']['Properties'])
        properties = list(set(properties))
        return properties

    def get_entry_type(self, node_display_name):
        if node_display_name in self.properties:
                entry_type = 'property'
        else:
            entry_type = 'class'
        return entry_type

    def run_rel_functions(self, rel_func, node_display_name='', key='', attr_relationships='', csv_header='', entry_type=''):
        ''' This function exists to centralzie handling of functions for filling out node information.
        TODO: and an ending else statement to alert to no func being caught.
        - Implement using a factory pattern.
        elif key == 'id' and rel_func == get_property_label_from_display_name:
            func_output = 'bts:' + get_property_label_from_display_name(node_display_name)
        
        elif rel_func == get_class_label_from_display_name:
            func_output = get_class_label_from_display_name(node_display_name)
        '''

        func_output = ''
        if rel_func == get_display_name_from_label:
            func_output = get_display_name_from_label(node_display_name, attr_relationships)
        elif rel_func == parse_validation_rules:
            func_output = parse_validation_rules(attr_relationships[csv_header])
        elif key == 'id' and rel_func == get_label_from_display_name:
            #func_output = 'bts:' + get_label_from_display_name(display_name =node_display_name, entry_type=entry_type)
            func_output = get_label_from_display_name(display_name =node_display_name, entry_type=entry_type)
        elif rel_func == get_label_from_display_name:
            func_output = get_label_from_display_name(display_name =node_display_name, entry_type=entry_type)
        elif rel_func == convert_bool:
            #func_output = 'sms:' + convert_bool(attr_relationships[csv_header]).lower()
            if type(attr_relationships[csv_header]) == str:
                if attr_relationships[csv_header].lower() == 'true':
                    func_output = True
                elif attr_relationships[csv_header].lower() == 'false':
                    func_output = False
            elif type(attr_relationships[csv_header]) == bool:
                func_output = attr_relationships[csv_header]
        else:
            # raise error here to catch non valid function.
            breakpoint()
        return func_output

    def generate_node_dict(self, node_display_name, data_model):
        """Gather information to be attached to each node.
        Args:
            node_display_name: display name for current node
            data_model:

        Returns:
            node_dict
        Note:
            If the default calls function, call that function for the default or alternate implementation.
            May need to update this logic for varying function calls. (for example the current function takes in the node display name
            ould need to update if new function took in something else.)
        """

        # Strip whitespace from node display name
        node_display_name = node_display_name.strip()

        # Determine if property or class
        entry_type = self.get_entry_type(node_display_name=node_display_name)
        
        # If the node is an attribute, find its relationships.
        attr_relationships = {}
        if node_display_name in data_model.keys():
            attr_relationships = data_model[node_display_name]['Relationships']
        
        # Initialize node_dict
        node_dict = {}

        # Look through relationship types that represent values (i.e. do not define edges)
        for key, csv_header in self.value_relationships.items():

            # Get key and defalt values current relationship type.
            rel_key, rel_default = self.get_rel_default_info(key)

            # If we have information to add about this particular node
            if csv_header in attr_relationships.keys():
                # Check if the default specifies calling a function.
                if 'standard' in rel_default.keys() and isfunction(rel_default['standard']):
                    # Add to node_dict The value comes from the standard function call. 
                    node_dict.update({rel_key: self.run_rel_functions(rel_default['standard'], node_display_name=node_display_name, key=key, attr_relationships=attr_relationships, csv_header=csv_header, entry_type=entry_type)})
                else:
                    # For standard entries, get information from attr_relationship dictionary
                    node_dict.update({rel_key: attr_relationships[csv_header]})
            # else, add default values
            else: 
                # Check if the default specifies calling a function.
                if 'default' in rel_default.keys() and isfunction(rel_default['default']):
                    node_dict.update({rel_key: self.run_rel_functions(rel_default['default'], node_display_name=node_display_name, key=key, attr_relationships=attr_relationships, csv_header=csv_header, entry_type=entry_type)})
                else:
                    # Set value to defaults.
                    node_dict.update({rel_key: rel_default['default']})

        return node_dict

    def generate_node(self, G, node_dict):
        """
        Args:

        Returns:
        """
        G.add_node(node_dict['label'], **node_dict)
        return G

    def edit_node():
        return