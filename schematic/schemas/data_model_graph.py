import os
import string
import json
import logging

from typing import Any, Dict, Optional, Text, List

import inflection
import networkx as nx

from rdflib import Graph, Namespace, plugin, query
from networkx.algorithms.cycles import find_cycle
from networkx.readwrite import json_graph

from schematic.schemas.data_model_edges.DataModelEdges import generate_edge, edit_edge
from schematic.schemas.data_model_nodes.DataModelNodes import generate_node, edit_node

from schematic.utils.curie_utils import (
    expand_curies_in_schema,
    uri2label,
    extract_name_from_uri_or_curie,
)
from schematic.utils.general import find_duplicates
from schematic.utils.io_utils import load_default, load_json, load_schemaorg
from schematic.utils.schema_utils import (
    load_schema_into_networkx,
    node_attrs_cleanup,
    class_to_node,
    relationship_edges,
)
from schematic.utils.general import dict2list, unlist
from schematic.utils.viz_utils import visualize
from schematic.utils.validate_utils import (
    validate_class_schema,
    validate_property_schema,
    validate_schema,
)
from schematic.schemas.curie import uri2curie, curie2uri


namespaces = dict(rdf=Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))


logger = logging.getLogger(__name__)



class DataModelGraphMeta(object):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class DataModelGraph():
    '''
    Generate graph network (networkx) from the attributes and relationships returned
    fromt he data model parser.

    Create a singleton.
    '''
    __metaclass__ = DataModelGraphMeta
    def __init__(self, parsed_data_model):
        '''Load parsed data model.
        '''
        
        self.data_model = parsed_data_model

        if not self.data_model:
            raise ValueError(
                    "Something has gone wrong, a data model was not loaded into the DataModelGraph Class. Please check that your paths are correct"
                )


    def generate_data_model_graph(self):
        '''Generate NetworkX Graph from the Relationships/attributes dictionary
        
        '''

        G = nx.MultiDiGraph()
        for attribute, relationships in self.data_model:
            node = generate_node(G, attribute, relationship)






        data_model_graph = None
        breakpoint()
        return data_model_graph

class DataModelGraphExporer():
    def __init__():
        '''
        Load data model graph as a singleton.
        '''
        #self.data_model_graph = DataModelGraph.generate_data_model_graph(data_model)

    def get_adjacent_nodes_by_relationship():
        return

    def get_component_requirements():
        return

    def get_component_requirements_graph():
        return

    def get_descendants_by_edge_type():
        return

    def get_digraph_by_edge_type():
        return

    def get_edges_by_relationship():
        return

    def get_node_definition():
        return

    def get_node_dependencies():
        return

    def get_node_label():
        return

    def find_adjacent_child_classes():
        return

    def find_all_class_properties():
        return

    def find_class_specific_properties():
        return

    def find_class_usages():
        return

    def is_node_required():
        return

    