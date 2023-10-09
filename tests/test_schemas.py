import os
import logging

import pandas as pd
import pytest
import random

#from schematic.schemas import df_parser
from schematic.utils.df_utils import load_df
from schematic.utils.schema_utils import get_label_from_display_name, get_attribute_display_name_from_label, convert_bool_to_str, parse_validation_rules

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
def test_fake_func():
    return

REL_FUNC_DICT = {
    'get_attribute_display_name_from_label':get_attribute_display_name_from_label,
    'parse_validation_rules': parse_validation_rules,
    'get_label_from_display_name': get_label_from_display_name,
    'convert_bool_to_str': convert_bool_to_str,
    'test_fake_func': test_fake_func, 
}
TEST_DN_DICT = {'Bio Things': {'class': 'BioThings',
                               'property': 'bioThings'},
                'bio things': {'class': 'Biothings',
                               'property': 'biothings'},
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

@pytest.fixture(name='relationships')
def get_relationships(helpers):
    DMR = DataModelRelationships()
    relationships_dict = DMR.relationships_dictionary
    relationships = list(relationships_dict.keys())
    yield relationships

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
        # Only check first entry, bc subsequent ones might be in the same order as would be gathered with gather_nodes if it contained a node that was already recorded.
        first_attribute = list(attr_rel_dictionary.keys())[0]
        attr_info = (first_attribute, attr_rel_dictionary[first_attribute])
        expected_starter_nodes = data_model_nodes.gather_nodes(attr_info=attr_info)
        actual_starter_nodes = all_nodes[0:len(expected_starter_nodes)]

        assert actual_starter_nodes == expected_starter_nodes

    def test_get_rel_node_dict_info(self, helpers, relationships):
        # Instantiate Parser
        data_model_parser = helpers.get_data_model_parser(data_model_name='example.model.csv')

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(helpers, data_model_name='example.model.csv')

        for relationship in relationships:
            rel_dict_info = data_model_nodes.get_rel_node_dict_info(relationship)
            if rel_dict_info:
                assert type(rel_dict_info[0]) == str
                assert type(rel_dict_info[1]) == dict
                assert 'default' in rel_dict_info[1].keys()

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    def test_get_data_model_properties(self, helpers, data_model):
        # Instantiate Parser
        data_model_parser = helpers.get_data_model_parser(data_model_name=data_model)

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(helpers, data_model_name=data_model)

        # Get properties in the data model
        data_model_properties = data_model_nodes.get_data_model_properties(attr_rel_dictionary)
        
        # In the current example model, there are no properties, would need to update this section if properties are added.
        assert data_model_properties == []

        # Update the attr_rel_dictionary to add a property, then see if its found.
        # Get a random relationship key from the attr_rel_dictionary:
        all_keys = list(attr_rel_dictionary.keys())
        random_index = len(all_keys)-1
        rel_key = all_keys[random.randint(0, random_index)]

        # Modify the contents of that relationship
        attr_rel_dictionary[rel_key]['Relationships']['Properties'] = ['TestProperty']
        
        # Get properties in the modified data model
        data_model_properties = data_model_nodes.get_data_model_properties(attr_rel_dictionary)

        assert data_model_properties == ['TestProperty']

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    def test_get_entry_type(self, helpers, data_model):
        
        # Instantiate Parser
        data_model_parser = helpers.get_data_model_parser(data_model_name=data_model)

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Update the attr_rel_dictionary to add a property, then see if it is assigned the correct entry type.
        # Get a random relationship key from the attr_rel_dictionary:
        all_keys = list(attr_rel_dictionary.keys())
        random_index = len(all_keys)-1
        rel_key = all_keys[random.randint(0, random_index)]

        # Modify the contents of that relationship
        attr_rel_dictionary[rel_key]['Relationships']['Properties'] = ['TestProperty']

        # Instantiate DataModelNodes
        # Note: Get entry type uses self, so I will have to instantiate DataModelNodes outside of the generate_data_model_nodes function
        data_model_nodes = DataModelNodes(attribute_relationships_dict=attr_rel_dictionary)

        # In the example data model all attributes should be classes.
        for attr in attr_rel_dictionary.keys():
            entry_type = data_model_nodes.get_entry_type(attr)
            assert entry_type == 'class'

        # Check that the added property is properly loaded as a property
        assert data_model_nodes.get_entry_type('TestProperty') == 'property'

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    @pytest.mark.parametrize("rel_func", list(REL_FUNC_DICT.values()), ids=list(REL_FUNC_DICT.keys()))
    @pytest.mark.parametrize("test_dn", list(TEST_DN_DICT.keys()), ids=list(TEST_DN_DICT.keys()))
    @pytest.mark.parametrize("test_bool", ['True', 'False', True, False, 'kldjk'], ids=['True_str', 'False_str', 'True_bool', 'False_bool', 'Random_str'])
    def test_run_rel_functions(self, helpers, data_model, rel_func, test_dn, test_bool):
        # Call each relationship function to ensure that it is returning the desired result.
        # Note all the called functions will also be tested in other unit tests.
        # Instantiate Parser
        data_model_parser = helpers.get_data_model_parser(data_model_name=data_model)

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(helpers, data_model_name=data_model)

        # Run functions the same way they are called in run_rel_functions:
        if rel_func == get_attribute_display_name_from_label:
            expected_display_names = list(attr_rel_dictionary.keys())
            returned_display_names = [data_model_nodes.run_rel_functions(
                                            rel_func=get_attribute_display_name_from_label,
                                            node_display_name=ndn,
                                            attr_relationships=attr_rel_dictionary) 
                                            for ndn in expected_display_names]

            assert expected_display_names == returned_display_names

        elif rel_func == parse_validation_rules:
            # Find attributes with validation rules
            # Gather Validation Rules
            vrs = []
            for k, v in attr_rel_dictionary.items():
                if 'Validation Rules' in v['Relationships'].keys():
                    vrs.append(v['Relationships']['Validation Rules'])
            parsed_vrs= []
            for attr in attr_rel_dictionary.keys():
                attr_relationships = attr_rel_dictionary[attr]['Relationships']
                if 'Validation Rules' in attr_relationships:
                    parsed_vrs.append(data_model_nodes.run_rel_functions(
                                        rel_func=parse_validation_rules,
                                        attr_relationships=attr_relationships,
                                        csv_header='Validation Rules'))            

            assert len(vrs) == len(parsed_vrs)
            if DATA_MODEL_DICT[data_model]=='CSV':
                assert vrs != parsed_vrs
            elif DATA_MODEL_DICT[data_model]=='JSONLD':
                # JSONLDs already contain parsed validaiton rules so the raw vrs will match the parsed_vrs
                assert vrs == parsed_vrs

            # For all validation rules where there are multiple rules, make sure they have been split as expected.
            for i, pvr in enumerate(parsed_vrs):
                delim_count = vrs[i][0].count('::')
                if delim_count:
                    assert len(pvr) == delim_count+1

        elif rel_func == get_label_from_display_name:
            # For a limited set check label is returned as expected.
            for entry_type, expected_value in TEST_DN_DICT[test_dn].items():
                actual_value = data_model_nodes.run_rel_functions(
                    rel_func=get_label_from_display_name,
                    node_display_name=test_dn,
                    entry_type=entry_type,
                    )
                assert actual_value == expected_value
        elif rel_func == convert_bool_to_str:
            # return nothing if random string provided.
            csv_header='Required'
            attr_relationships = {csv_header:test_bool}
            actual_conversion = data_model_nodes.run_rel_functions(
                    rel_func=convert_bool_to_str,
                    csv_header=csv_header,
                    attr_relationships=attr_relationships,
                    )
            if 'true' in str(test_bool).lower():
                assert actual_conversion==True
            elif 'false' in str(test_bool).lower():
                assert actual_conversion==False
            else:
                assert actual_conversion==None
        else:
            # If the function passed is not currently supported, should hit an error.
            try:
                data_model_nodes.run_rel_functions(rel_func=test_fake_func)
                convert_worked = False
            except:
                convert_worked = True
            assert convert_worked==True
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

