import os
import logging

import pandas as pd
import pytest
import networkx as nx
from copy import deepcopy

from schematic.schemas.data_model_edges import DataModelEdges
from schematic.schemas.data_model_nodes import DataModelNodes 
from schematic.schemas.data_model_relationships import (
    DataModelRelationships
    )

#from schematic.schemas import df_parser
from schematic.utils.df_utils import load_df
from schematic.schemas.data_model_graph import DataModelGraph
from schematic.schemas.data_model_nodes import DataModelNodes
from schematic.schemas.data_model_edges import DataModelEdges
from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.schemas.data_model_jsonld import DataModelJsonLD
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.schemas.data_model_parser import DataModelParser

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def generate_graph_data_model(helpers, data_model_name):
    """
    Simple helper function to generate a networkx graph data model from a CSV or JSONLD data model
    """
    
    # Instantiate Parser
    data_model_parser = helpers.get_data_model_parser(data_model_name=data_model_name)

    #Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Convert parsed model to graph
    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model)

    # Generate graph
    graph_data_model = data_model_grapher.generate_data_model_graph()

    return graph_data_model

@pytest.fixture
def DME(helpers, data_model_name='example.model.csv'):
    path_to_data_model = helpers.get_data_path("example.model.jsonld")

    graph_data_model = generate_graph_data_model(helpers, data_model_name=path_to_data_model)
    DME = DataModelGraphExplorer(graph_data_model)
    yield DME

@pytest.fixture(name="DMR")
def fixture_DMR():
    """Yields a data model relationships object for testing"""
    yield DataModelRelationships()

@pytest.fixture
def DMEdges():
    """
    Yields a Data Model Edges object for testing
    TODO: Update naming for DataModelGraphExplorer and fixture to avoid overlapping namespace
    """
    yield DataModelEdges()

class TestDataModelParser:
    def test_get_base_schema_path(self, helpers):
        return

    def test_get_model_type(self):
        return

    def test_parse_model(self):
        return

class TestDataModelCsvParser:
    def test_check_schema_definition(self):
        return
    def test_gather_csv_attributes_relationships(self):
        return
    def test_parse_csv_model(self ):
        return

class TestDataModelJsonLdParser:
    def test_gather_jsonld_attributes_relationships(self):
        return
    def test_parse_jsonld_model(self):
        return

class TestDataModelRelationships:
    """Tests for DataModelRelationships class"""
    def test_define_data_model_relationships(self, DMR: DataModelRelationships):
        """Tests relationships_dictionary created has correct keys"""
        required_keys = [
            'jsonld_key',
            'csv_header',
            'type',
            'edge_rel',
            'required_header'
        ]
        required_edge_keys = ['edge_key', 'edge_dir']
        required_node_keys = ['node_label', 'node_attr_dict']

        relationships = DMR.relationships_dictionary

        for relationship in relationships.values():
            for key in required_keys:
                assert key in relationship.keys()
            if relationship['edge_rel']:
                for key in required_edge_keys:
                    assert key in relationship.keys()
            else:
                for key in required_node_keys:
                    assert key in relationship.keys()

    def test_define_required_csv_headers(self, DMR: DataModelRelationships):
        """Tests method returns correct values"""
        assert DMR.define_required_csv_headers() == [
            'Attribute',
            'Description',
            'Valid Values',
            'DependsOn',
            'DependsOn Component',
            'Required', 'Parent',
            'Validation Rules',
            'Properties',
            'Source'
        ]

    def test_define_edge_relationships(self, DMR: DataModelRelationships):
        """Tests method returns correct values"""
        assert DMR.define_edge_relationships() == {
            'rangeIncludes': 'Valid Values',
            'requiresDependency': 'DependsOn',
            'requiresComponent': 'DependsOn Component',
            'subClassOf': 'Parent',
            'domainIncludes': 'Properties'
        }

    def test_define_value_relationships(self, DMR: DataModelRelationships):
        """Tests method returns correct values"""
        assert DMR.define_value_relationships() == {
            'displayName': 'Attribute',
            'label': None,
            'comment': 'Description',
            'required': 'Required',
            'validationRules': 'Validation Rules',
            'isPartOf': None,
            'id': 'Source'
        }


class TestDataModelGraph:
    @pytest.mark.parametrize("data_model", ['example.model.csv', 'example.model.jsonld'], ids=["csv", "jsonld"])
    def test_generate_data_model_graph(self, helpers, data_model):
        '''Check that data model graph is constructed properly, requires calling various classes.
        TODO: In another test, check conditional dependencies.
        '''
        graph = generate_graph_data_model(helpers=helpers, data_model_name=data_model)
        
        #Check that some edges are present as expected:
        assert True == (('FamilyHistory', 'Breast') in graph.edges('FamilyHistory'))
        assert True == (('BulkRNA-seqAssay', 'Biospecimen') in graph.edges('BulkRNA-seqAssay'))
        assert ['Ab', 'Cd', 'Ef', 'Gh'] == [k for k,v in graph['CheckList'].items() for vk, vv in v.items() if vk == 'rangeValue']

        # Check that all relationships recorded between 'CheckList' and 'Ab' are present
        assert True == ('rangeValue' and 'parentOf' in graph['CheckList']['Ab'])
        assert False == ('requiresDependency' in graph['CheckList']['Ab'])
        
        # Check nodes:
        assert True == ('Patient' in graph.nodes)
        assert True == ('GRCh38' in graph.nodes)


        # Check weights
        assert True == (graph['Sex']['Female']['rangeValue']['weight'] == 0)
        assert True == (graph['MockComponent']['CheckRegexFormat']['requiresDependency']['weight'] == 4)

        # Check Edge directions
        assert 4 == (len(graph.out_edges('TissueStatus')))
        assert 2 == (len(graph.in_edges('TissueStatus')))

class TestDataModelGraphExplorer:
    def test_find_properties(self):
        return

    def test_find_classes(self):
        return

    def test_find_node_range(self):
        return

    def test_get_adjacent_nodes_by_relationship(self):
        return

    def test_get_component_requirements(self):
        return

    def test_get_component_requirements_graph(self):
        return

    def get_descendants_by_edge_type(self):
        return

    def test_get_digraph_by_edge_type(self):
        return

    def test_get_edges_by_relationship(self):
        return

    def test_get_ordered_entry(self):
        return

    def test_get_nodes_ancestors(self):
        return

    def test_get_node_comment(self):
        return

    def test_get_node_dependencies(self):
        return

    def test_get_nodes_descendants(self):
        return

    def test_get_nodes_display_names(self):
        return

    def test_get_node_label(self):
        return

    def test_get_node_range(self):
        return

    def test_get_node_required(self):
        return

    def test_get_node_validation_rules(self):
        return

    def test_get_subgraph_by_edge_type(self):
        return

    def test_find_adjacent_child_classes(self):
        return

    def test_find_parent_classes(self):
        return

    def test_full_schema_graph(self):
        return

    @pytest.mark.parametrize("class_name, expected_in_schema", [("Patient",True), ("ptaient",False), ("Biospecimen",True), ("InvalidComponent",False)])
    def test_is_class_in_schema(self, DME, class_name, expected_in_schema):
        """
        Test to cover checking if a given class is in a schema.
        `is_class_in_schema` should return `True` if the class is in the schema
        and `False` if it is not.
        """

        # Check if class is in schema
        class_in_schema = DME.is_class_in_schema(class_name)

        # Assert value is as expected
        assert class_in_schema == expected_in_schema

    def test_sub_schema_graph(self):
        return

class TestDataModelNodes:
    def test_gather_nodes(self):
        return
    def test_gather_all_nodes(self):
        return
    def test_get_rel_node_dict_info(self):
        return
    def test_get_data_model_properties(self):
        return
    def test_get_entry_type(self):
        return
    def test_run_rel_functions(self):
        return
    def test_generate_node_dict(self):
        return
    def test_generate_node(self):
        return

class TestDataModelEdges:
    """
    Cases to test
        Where node == attribute_display_name
        Weights
            domain includes weights
            list weights
            single element weights
        Edges
            subClassOf/domainIncludes relationship edge
            any other relationship edge
            rangeIncludes relationship edge
        
    """
    def test_skip_edge(self, helpers, DMR, DMEdges):
        # Instantiate graph object and set node
        G = nx.MultiDiGraph()
        node = "Diagnosis"

        # Instantiate Parser
        data_model_parser = helpers.get_data_model_parser("validator_dag_test.model.csv")

        # Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate data model objects
        dmn = DataModelNodes(parsed_data_model)

        # Get edge relationships and all nodes from the parsed model
        edge_relationships = DMR.define_edge_relationships()
        all_nodes = dmn.gather_all_nodes(attr_rel_dict=parsed_data_model)

        # Sanity check to ensure that the node we intend to test exists in the data model
        assert node in all_nodes

        # Add a single node to the graph
        node_dict = {}
        node_dict = dmn.generate_node_dict(node, parsed_data_model)
        node_dict[node] = node_dict
        G = dmn.generate_node(G, node_dict)

        # Check the edges in the graph, there should be none
        before_edges = deepcopy(G.edges)

        # Generate an edge in the graph with one node and a subset of the parsed data model
        # We're attempting to add an edge for a node that is the only one in the graph, 
        # so `generate_edge` should skip adding edges and return the same graph
        G = DMEdges.generate_edge(G, node, node_dict, {node:parsed_data_model[node]}, edge_relationships)

        # Assert that no edges were added and that the current graph edges are the same as before the call to `generate_edge`
        assert before_edges == G.edges

        return
    
    @pytest.mark.parametrize("node_to_add, edge_relationship", 
                             [("DataType", "parentOf"), 
                                ("Female", "parentOf"),
                                ("Sex","requiresDependency")],
                              ids=["subClassOf",
                                   "Valid Value",
                                   "all others"
                                   ])
    def test_generate_edge(self, helpers, DMR, DMEdges, node_to_add, edge_relationship):
        # Instantiate graph object
        G = nx.MultiDiGraph()

        # Instantiate Parser
        data_model_parser = helpers.get_data_model_parser("validator_dag_test.model.csv")

        #Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate data model objects
        dmn = DataModelNodes(parsed_data_model)

        # Get edge relationships and all nodes from the parsed model
        edge_relationships = DMR.define_edge_relationships()
        all_nodes = dmn.gather_all_nodes(attr_rel_dict=parsed_data_model)

        # Sanity check to ensure that the node we intend to test exists in the data model
        assert node_to_add in all_nodes

        # Add all nodes to the graph 
        all_node_dict = {}
        for node in all_nodes:
            node_dict = dmn.generate_node_dict(node, parsed_data_model)
            all_node_dict[node] = node_dict
            G = dmn.generate_node(G, node_dict)

        # Check the edges in the graph, there should be none
        before_edges = deepcopy(G.edges)

        # Generate edges for whichever node we are testing
        G = DMEdges.generate_edge(G, node_to_add, all_node_dict, parsed_data_model, edge_relationships)

        # Assert that the current edges are different from the edges of the graph before
        assert G.edges != before_edges

        # Assert that somewhere in the current edges for the node we added, that the correct relationship exists
        relationship_df = pd.DataFrame(G.edges, columns= ['node1', 'node2', 'edge'])
        assert (relationship_df['edge'] == edge_relationship).any()
        
        return
    
    @pytest.mark.parametrize("node_to_add, other_node, expected_weight, data_model_path", 
                             [("Patient ID", "Biospecimen", 1, "validator_dag_test.model.csv"),
                              ("dataset_id", "cohorts", -1, "properties.test.model.csv")],
                              ids=["list", "domainIncludes"])
    def test_generate_weights(self, helpers, DMR, DMEdges, node_to_add, other_node, expected_weight, data_model_path):
        # Instantiate graph object
        G = nx.MultiDiGraph()

        # Instantiate Parser
        data_model_parser = helpers.get_data_model_parser(data_model_path)

        #Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate data model objects
        dmn = DataModelNodes(parsed_data_model)

        # Get edge relationships and all nodes from the parsed model
        edge_relationships = DMR.define_edge_relationships()
        all_nodes = dmn.gather_all_nodes(attr_rel_dict=parsed_data_model)


        # Sanity check to ensure that the node we intend to test exists in the data model
        assert node_to_add in all_nodes
        
        # Add all nodes to the graph 
        all_node_dict = {}
        for node in all_nodes:
            node_dict = dmn.generate_node_dict(node, parsed_data_model)
            all_node_dict[node] = node_dict
            G = dmn.generate_node(G, node_dict)

        # Check the edges in the graph, there should be none
        before_edges = deepcopy(G.edges)

        # Generate edges for whichever node we are testing
        G = DMEdges.generate_edge(G, node_to_add, all_node_dict, parsed_data_model, edge_relationships)

        # Assert that the current edges are different from the edges of the graph before
        assert G.edges != before_edges
        
        # Cast the edges and weights to a DataFrame for easier indexing
        edges_and_weights = pd.DataFrame(G.edges.data(), columns= ['node1', 'node2', 'weights']).set_index('node1')

        if expected_weight < 0:
            schema = helpers.get_data_frame(path=helpers.get_data_path(data_model_path), data_model=True)
            expected_weight = schema.index[schema['Attribute']==other_node][0]
            logger.debug(f"Expected weight for nodes {node_to_add} and {other_node} is {expected_weight}.")

        # Assert that the weight added is what is expected
        if node_to_add in ['Patient ID']:
            assert edges_and_weights.loc[other_node, 'weights']['weight'] == expected_weight
        elif node_to_add in ['cohorts']:
            assert edges_and_weights.loc[node_to_add, 'weights']['weight'] == expected_weight
        return




class TestDataModelJsonSchema:
    def test_get_array_schema(self):
        return
    def test_get_non_blank_schema(self):
        return
    def test_get_json_validation_schema(self):
        return

class TestDataModelJsonLd:
    def test_base_jsonld_template(self):
        return
    def test_create_object(self):
        return
    def test_add_contexts_to_entries(self):
        return
    def test_clean_template(self):
        return
    def test_strip_context(self):
        return
    def test_reorder_template_entries(self):
        return
    def test_property_template(self):
        return
    def test_class_template(self):
        return
    def test_generate_jsonld_object(self):
        return
    def test_convert_graph_to_jsonld(self):
        return
class TestSchemas:
    def test_convert_csv_to_graph(self, helpers):
        return
    def test_convert_jsonld_to_graph(self, helpers):
        return

