from inspect import isfunction
from rdflib import Namespace

from schematic.schemas.data_model_relationships import (
    DataModelRelationships
    )

from schematic.utils.schema_util import get_property_label_from_display_name, get_class_label_from_display_name, get_display_name_from_label
from schematic.utils.validate_rules_utils import validate_schema_rules
from schematic.schemas.curie import uri2curie, curie2uri


class DataModelNodes():
    def __init__(self):
        self.namespaces = dict(rdf=Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
        self.data_model_relationships = DataModelRelationships()
        self.value_relationships = self.data_model_relationships.define_value_relationships()
        self.edge_relationships_dictionary = self.data_model_relationships.define_edge_relationships()
        
        
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
        self.node_relationships =list(self.edge_relationships_dictionary.keys())

        attribute, relationship = attr_info
        relationship = relationship['Relationships']

        nodes = []
        if attribute not in nodes:
            nodes.append(attribute)
        for rel in self.node_relationships:
            if rel in relationship.keys():
                nodes.extend([node.strip()
                                for node in relationship[rel]])
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
        all_nodes = [*set(all_nodes)]
        return all_nodes

    def get_rel_default_info(self, relationship):
        """
        For each display name fill out defaults. Maybe skip default.
        """
        for k,v in self.data_model_relationships.relationships_dictionary.items():
            for key, value in v.items():
                if key == relationship:
                    if 'node_dict' in value.keys():
                        rel_key = list(value['node_dict'].keys())[0]
                        rel_default = value['node_dict'][rel_key]
                        return rel_key, rel_default

    def run_rel_functions(self, rel_func, node_display_name='', attr_relationships={}):
        ''' This function exists to centralzie handling of functions for filling out node information.
        TODO: and an ending else statement to alert to no func being caught.
        '''
        func_output = ''

        if rel_func == get_display_name_from_label:
            func_output = get_display_name_from_label(node_display_name, attr_relationships)
        elif rel_func == get_class_label_from_display_name:
            func_output = get_class_label_from_display_name(node_display_name)
        elif rel_func == get_property_label_from_display_name:
            func_output = get_property_label_from_display_name(node_display_name)
        elif rel_func == uri2curie:
            func_output = uri2curie(node_display_name, self.namespaces)
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
        
        # If the node is an attribute, find its relationships.
        attr_relationships = {}
        if node_display_name in data_model.keys():
            attr_relationships = data_model[node_display_name]['Relationships']
        
        # Initialize node_dict
        node_dict = {}

        # Look through relationship types that represent values (i.e. do not define edges)
        for k, v in self.value_relationships.items():
            # Get key and defalt values current relationship type.
            rel_key, rel_default = self.get_rel_default_info(k)

            # If we have information to add about this particular node
            if attr_relationships and k in attr_relationships.keys():
                 # Check if the default specifies calling a function.
                if type(rel_default) == dict and 'default' in rel_default.keys() and isfunction(rel_default['default']):
                    # Add to node_dict The value comes from the standard function call. 
                    # TODO UPDATE TO USE FUNCTION FUNCTION
                    #breakpoint()
                    node_dict.update({rel_key: self.run_rel_functions(rel_default['standard'], node_display_name, attr_relationships)})
                    '''
                    try:
                        node_dict.update({rel_key: rel_default['standard'](node_display_name)})
                    except:
                        node_dict.update({rel_key: rel_default['standard'](node_display_name, self.namespaces)})
                    '''
                else:
                    # For standard entries, get information from attr_relationship dictionary
                    node_dict.update({rel_key: attr_relationships[k]})
            # else, add default values
            else: 
                # Check if the default specifies calling a function.
                if type(rel_default) == dict and 'default' in rel_default.keys() and isfunction(rel_default['default']):
                    #breakpoint()
                    node_dict.update({rel_key: self.run_rel_functions(rel_default['default'], node_display_name, attr_relationships)})

                    # Add to node_dict. The value comes from the standard function call. 
                    # TODO UPDATE TO USE FUNCTION FUNCTION
                    '''
                    try:
                        node_dict.update({rel_key: rel_default['default'](node_display_name)})
                    except:
                        node_dict.update({rel_key: rel_default['default'](node_display_name, self.namespaces)})
                    '''
                else:
                    # Set value to defaults.
                    node_dict.update({rel_key: rel_default})
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