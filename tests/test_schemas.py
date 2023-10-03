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

DATA_MODEL_DICT = {
  'example.model.csv': "CSV",
  'example.model.jsonld': "JSONLD"
}

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

def generate_data_model_nodes(helpers, data_model_name):
    # Instantiate Parser
    data_model_parser = helpers.get_data_model_parser(data_model_name=data_model_name)
    # Parse Model
    parsed_data_model = data_model_parser.parse_model()
    # Instantiate DataModelNodes
    data_model_nodes = DataModelNodes(attribute_relationships_dict=parsed_data_model)
    return data_model_nodes

@pytest.fixture
def DME(helpers, data_model_name='example.model.csv'):
    path_to_data_model = helpers.get_data_path("example.model.jsonld")

    graph_data_model = generate_graph_data_model(helpers, data_model_name=path_to_data_model)
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
    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    def test_gather_nodes(self, helpers, data_model):
        # Instantiate Parser
        data_model_parser = helpers.get_data_model_parser(data_model_name=data_model)

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(helpers, data_model_name=data_model)

        attr_info = ('Patient', attr_rel_dictionary['Patient'])
        nodes = data_model_nodes.gather_nodes(attr_info=attr_info)

        # Make sure there are no repeat nodes
        assert len(nodes) == len(set(nodes))

        # Make sure the nodes returned conform to expectations (values and order)
        ## The parsing records display names for relationships for CSV and labels for JSONLD, so the expectations are different between the two.
        if DATA_MODEL_DICT[data_model]=='CSV':
            expected_nodes = ['Patient', 'Patient ID', 'Sex', 'Year of Birth', 'Diagnosis', 'Component', 'DataType']
        elif DATA_MODEL_DICT[data_model] == 'JSONLD':
            expected_nodes = ['Patient', 'PatientID', 'Sex', 'YearofBirth', 'Diagnosis', 'Component', 'DataType']

        assert nodes == expected_nodes

        # Ensure order is tested.
        reordered_nodes = nodes.copy()
        reordered_nodes.remove('Patient')
        reordered_nodes.append('Patient')
        assert reordered_nodes != expected_nodes

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    def test_gather_all_nodes(self, helpers, data_model):
        # Instantiate Parser
        data_model_parser = helpers.get_data_model_parser(data_model_name=data_model)

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(helpers, data_model_name=data_model)

        all_nodes = data_model_nodes.gather_all_nodes(attr_rel_dict=attr_rel_dictionary)

        # Make sure there are no repeat nodes
        assert len(all_nodes) == len(set(all_nodes))

        # Check that nodes from first entry, are recoreded in order in all_nodes
        first_attribute = list(attr_rel_dictionary.keys())[0]
        attr_info = (first_attribute, attr_rel_dictionary[first_attribute])
        expected_starter_nodes = data_model_nodes.gather_nodes(attr_info=attr_info)
        actual_starter_nodes = all_nodes[0:len(expected_starter_nodes)]

        assert actual_starter_nodes == expected_starter_nodes

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
    def test_convert_csv_to_graph(self, helpers):
        return
    def test_convert_jsonld_to_graph(self, helpers):
        return

