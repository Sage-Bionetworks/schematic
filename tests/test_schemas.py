import os
import logging

import pandas as pd
import pytest

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

def generate_graph_data_model(helpers, path_to_data_model):
    """
    Simple helper function to generate a networkx graph data model from a CSV or JSONLD data model
    """
    
    # Instantiate Parser
    data_model_parser = DataModelParser(path_to_data_model=path_to_data_model)

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

    graph_data_model = generate_graph_data_model(helpers, path_to_data_model=path_to_data_model)
    DME = DataModelGraphExplorer(graph_data_model)
    yield DME

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
    def test_define_data_model_relationships(self):
        return
    def test_define_required_csv_headers(self):
        return
    def test_define_edge_relationships(self):
        return
    def test_define_value_relationships(self):
        return

class TestDataModelGraph:
    def test_generate_data_model_graph(self):
        return

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
    def test_generate_edge(self,helpers):
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
    @pytest.mark.parametrize("data_model", ['tests/data/example.model.csv', 'tests/data/example.model.jsonld'], ids=["csv", "jsonld"])
    def test_convert_data_model_to_graph(self, helpers, data_model):
        '''Check that data model graph is constructed properly, requires calling various classes.
        
        '''
        graph = generate_graph_data_model(helpers=helpers, path_to_data_model=data_model)
        
        #Check that some edges are present as expected:
        assert True == (('FamilyHistory', 'Breast') in graph.edges('FamilyHistory'))
        assert True == (('BulkRNA-seqAssay', 'Biospecimen') in graph.edges('BulkRNA-seqAssay'))
        assert ['Ab', 'Cd', 'Ef', 'Gh'] == [k for k,v in graph['CheckList'].items() for vk, vv in v.items() if vk == 'rangeValue']

        # Check that all relationships recorded between 'CheckList' and 'Ab' are present
        assert True == ('rangeValue' and 'parentOf' in graph['CheckList']['Ab'])
        assert False == ('requiresDependency' in graph['CheckList']['Ab'])
        
        # Check nodes:
        # JSONLD will add 'Thing' to compare between CSV and JSONLD just compare all nodes except 'Thing'.
        assert 66 ==(len([node for node in graph.nodes if node != 'Thing']))
        assert True == ('Patient' in graph.nodes)
        assert True == ('GRCh38' in graph.nodes)


        # Check weights
        assert True == (graph['Sex']['Female']['rangeValue']['weight'] == 0)
        assert True == (graph['MockComponent']['CheckRegexFormat']['requiresDependency']['weight'] == 4)

        # Check Edge directions
        assert 4 == (len(graph.out_edges('TissueStatus')))
        assert 2 == (len(graph.in_edges('TissueStatus')))

