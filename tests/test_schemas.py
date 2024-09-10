import json
import logging
import os
import random
from copy import deepcopy

import networkx as nx
import numpy as np
import pandas as pd
import pytest

from schematic.schemas.data_model_edges import DataModelEdges
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.schemas.data_model_jsonld import (
    BaseTemplate,
    ClassTemplate,
    DataModelJsonLD,
    PropertyTemplate,
    convert_graph_to_jsonld,
)
from schematic.schemas.data_model_nodes import DataModelNodes
from schematic.schemas.data_model_parser import (
    DataModelCSVParser,
    DataModelJSONLDParser,
    DataModelParser,
)
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.utils.df_utils import load_df
from schematic.utils.io_utils import load_json
from schematic.utils.schema_utils import (
    DisplayLabelType,
    convert_bool_to_str,
    get_attribute_display_name_from_label,
    get_json_schema_log_file_path,
    get_label_from_display_name,
    parse_validation_rules,
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DATA_MODEL_DICT = {"example.model.csv": "CSV", "example.model.jsonld": "JSONLD"}


def test_fake_func():
    return


REL_FUNC_DICT = {
    "get_attribute_display_name_from_label": get_attribute_display_name_from_label,
    "parse_validation_rules": parse_validation_rules,
    "get_label_from_display_name": get_label_from_display_name,
    "convert_bool_to_str": convert_bool_to_str,
    "test_fake_func": test_fake_func,
}
TEST_DN_DICT = {
    "Bio Things": {"class": "BioThings", "property": "bioThings"},
    "bio things": {"class": "Biothings", "property": "biothings"},
}
NODE_DISPLAY_NAME_DICT = {"Patient": False, "Sex": True, "MockRDB_id": True}


def get_data_model_parser(
    helpers,
    data_model_name: str = None,
):
    # Get path to data model
    fullpath = helpers.get_data_path(path=data_model_name)

    # Instantiate DataModelParser
    data_model_parser = DataModelParser(
        path_to_data_model=fullpath,
    )
    return data_model_parser


def generate_graph_data_model(
    helpers,
    data_model_name: str,
    data_model_labels: DisplayLabelType = "class_label",
) -> nx.MultiDiGraph:
    """
    Simple helper function to generate a networkx graph data model from a CSV or JSONLD data model
    """
    # Instantiate Parser
    data_model_parser = get_data_model_parser(
        helpers=helpers,
        data_model_name=data_model_name,
    )

    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Convert parsed model to graph
    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    return graph_data_model


def generate_data_model_nodes(
    helpers,
    data_model_name: str,
    data_model_labels: DisplayLabelType = "class_label",
) -> DataModelNodes:
    # Instantiate Parser
    data_model_parser = get_data_model_parser(
        helpers=helpers,
        data_model_name=data_model_name,
    )
    # Parse Model
    parsed_data_model = data_model_parser.parse_model()
    # Instantiate DataModelNodes
    data_model_nodes = DataModelNodes(attribute_relationships_dict=parsed_data_model)
    return data_model_nodes


def get_data_model_json_schema(helpers, data_model_name: str = None):
    # Get path to data model
    fullpath = helpers.get_data_path(path=data_model_name)

    # Get Data Model Graph
    graph_data_model = generate_graph_data_model(
        helpers, data_model_name=data_model_name
    )

    # Instantiate DataModelJsonSchema
    dmjs = DataModelJSONSchema(fullpath, graph=graph_data_model)
    return dmjs


@pytest.fixture(name="relationships")
def get_relationships(helpers):
    DMR = DataModelRelationships()
    relationships_dict = DMR.relationships_dictionary
    relationships = list(relationships_dict.keys())
    yield relationships


@pytest.fixture(name="DMR")
def fixture_dmr():
    """Yields a data model relationships object for testing"""
    yield DataModelRelationships()


@pytest.fixture(name="csv_parser")
def fixture_dm_csv_parser():
    yield DataModelCSVParser()


@pytest.fixture(name="jsonld_parser")
def fixture_dm_jsonld_parser():
    yield DataModelJSONLDParser()


@pytest.fixture
def data_model_edges():
    """
    Yields a Data Model Edges object for testing
    TODO: Update naming for DataModelGraphExplorer and fixture to avoid overlapping namespace
    """
    yield DataModelEdges()


class TestDataModelParser:
    def test_get_base_schema_path(self, helpers):
        """Test that base schema path is returned properly.
        Note:
            data model parser class does not currently accept an new path to a base schema,
            so just test that default BioThings data model path is returned.
        """
        # Instantiate Data model parser.
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name="example.model.csv"
        )

        # Get path to default biothings model.
        biothings_path = data_model_parser._get_base_schema_path(base_schema=None)

        assert os.path.basename(biothings_path) == "biothings.model.jsonld"

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    def test_get_model_type(self, helpers, data_model: str):
        # Instantiate Data model parser.
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name=data_model
        )

        # Check the data model type
        assert (data_model == "example.model.csv") == (
            data_model_parser.model_type == "CSV"
        )
        assert (data_model == "example.model.jsonld") == (
            data_model_parser.model_type == "JSONLD"
        )

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    def test_parse_model(self, helpers, data_model: str):
        """Test that the correct parser is called and that a dictionary is returned in the expected structure."""
        # Instantiate Data model parser.
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name=data_model
        )

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Get a key in the model
        attribute_key = list(attr_rel_dictionary.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(attr_rel_dictionary) == dict
        assert attribute_key in attr_rel_dictionary.keys()
        assert "Relationships" in attr_rel_dictionary[attribute_key]
        assert "Attribute" in attr_rel_dictionary[attribute_key]["Relationships"]


@pytest.mark.parametrize("data_model", ["example.model.csv"], ids=["csv"])
class TestDataModelCsvParser:
    def test_check_schema_definition(
        self, helpers, data_model: str, csv_parser: DataModelCSVParser
    ):
        """If the csv schema contains the required headers, then this function should not return anything. Check that this is so."""
        # path_to_data_model = helpers.get_data_path(path=data_model)
        model_df = helpers.get_data_frame(path=data_model, data_model=True)
        assert None == (csv_parser.check_schema_definition(model_df=model_df))

    def test_gather_csv_attributes_relationships(
        self, helpers, data_model: str, csv_parser: DataModelCSVParser
    ):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly."""
        path_to_data_model = helpers.get_data_path(path=data_model)
        model_df = load_df(path_to_data_model, data_model=True)

        # Get output of the function:
        attr_rel_dict = csv_parser.gather_csv_attributes_relationships(
            model_df=model_df
        )

        # Test the attr_rel_dict is formatted as expected:
        # Get a key in the model
        attribute_key = list(attr_rel_dict.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(attr_rel_dict) == dict
        assert attribute_key in attr_rel_dict.keys()
        assert "Relationships" in attr_rel_dict[attribute_key]
        assert "Attribute" in attr_rel_dict[attribute_key]["Relationships"]

    def test_parse_csv_model(
        self, helpers, data_model: str, csv_parser: DataModelCSVParser
    ):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly."""
        path_to_data_model = helpers.get_data_path(path=data_model)
        model_df = load_df(path_to_data_model, data_model=True)

        # Get output of the function:
        attr_rel_dictionary = csv_parser.parse_csv_model(
            path_to_data_model=path_to_data_model
        )

        # Test the attr_rel_dictionary is formatted as expected:
        # Get a key in the model
        attribute_key = list(attr_rel_dictionary.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(attr_rel_dictionary) == dict
        assert attribute_key in attr_rel_dictionary.keys()
        assert "Relationships" in attr_rel_dictionary[attribute_key]
        assert "Attribute" in attr_rel_dictionary[attribute_key]["Relationships"]


@pytest.mark.parametrize("data_model", ["example.model.jsonld"], ids=["jsonld"])
class TestDataModelJsonLdParser:
    def test_gather_jsonld_attributes_relationships(
        self,
        helpers,
        data_model: str,
        jsonld_parser: DataModelJSONLDParser,
    ):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly."""
        path_to_data_model = helpers.get_data_path(path=data_model)
        model_jsonld = load_json(path_to_data_model)

        # Get output of the function:
        attr_rel_dict = jsonld_parser.gather_jsonld_attributes_relationships(
            model_jsonld=model_jsonld["@graph"],
        )

        # Test the attr_rel_dict is formatted as expected:
        # Get a key in the model
        attribute_key = list(attr_rel_dict.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(attr_rel_dict) == dict
        assert attribute_key in attr_rel_dict.keys()
        assert "Relationships" in attr_rel_dict[attribute_key]
        assert "Attribute" in attr_rel_dict[attribute_key]["Relationships"]

    def test_parse_jsonld_model(
        self,
        helpers,
        data_model: str,
        jsonld_parser: DataModelJSONLDParser,
    ):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly."""
        path_to_data_model = helpers.get_data_path(path=data_model)
        model_jsonld = load_json(path_to_data_model)

        # Get output of the function:
        attr_rel_dictionary = jsonld_parser.parse_jsonld_model(
            path_to_data_model=path_to_data_model,
        )

        # Test the attr_rel_dictionary is formatted as expected:
        # Get a key in the model
        attribute_key = list(attr_rel_dictionary.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(attr_rel_dictionary) == dict
        assert attribute_key in attr_rel_dictionary.keys()
        assert "Relationships" in attr_rel_dictionary[attribute_key]
        assert "Attribute" in attr_rel_dictionary[attribute_key]["Relationships"]


class TestDataModelRelationships:
    """Tests for DataModelRelationships class"""

    def test_define_data_model_relationships(self, DMR: DataModelRelationships):
        """Tests relationships_dictionary created has correct keys"""
        required_keys = [
            "jsonld_key",
            "csv_header",
            "type",
            "edge_rel",
            "required_header",
        ]
        required_edge_keys = ["edge_key", "edge_dir"]
        required_node_keys = ["node_label", "node_attr_dict"]

        relationships = DMR.relationships_dictionary

        for relationship in relationships.values():
            for key in required_keys:
                assert key in relationship.keys()
            if relationship["edge_rel"]:
                for key in required_edge_keys:
                    assert key in relationship.keys()
            else:
                for key in required_node_keys:
                    assert key in relationship.keys()

    def test_define_required_csv_headers(self, DMR: DataModelRelationships):
        """Tests method returns correct values"""
        assert DMR.define_required_csv_headers() == [
            "Attribute",
            "Description",
            "Valid Values",
            "DependsOn",
            "DependsOn Component",
            "Required",
            "Parent",
            "Validation Rules",
            "Properties",
            "Source",
        ]

    @pytest.mark.parametrize("edge", [True, False], ids=["True", "False"])
    def test_retreive_rel_headers_dict(self, DMR: DataModelRelationships, edge: bool):
        """Tests method returns correct values"""
        if edge:
            assert DMR.retreive_rel_headers_dict(edge=edge) == {
                "rangeIncludes": "Valid Values",
                "requiresDependency": "DependsOn",
                "requiresComponent": "DependsOn Component",
                "subClassOf": "Parent",
                "domainIncludes": "Properties",
            }
        else:
            assert DMR.retreive_rel_headers_dict(edge=edge) == {
                "displayName": "Attribute",
                "label": None,
                "comment": "Description",
                "required": "Required",
                "validationRules": "Validation Rules",
                "isPartOf": None,
                "id": "Source",
            }


class TestDataModelGraph:
    @pytest.mark.parametrize(
        "data_model",
        ["example.model.csv", "example.model.jsonld"],
        ids=["csv", "jsonld"],
    )
    @pytest.mark.parametrize(
        "data_model_labels",
        ["display_label", "class_label"],
        ids=["data_model_labels-display_label", "data_model_labels-class_label"],
    )
    def test_generate_data_model_graph(self, helpers, data_model, data_model_labels):
        """Check that data model graph is constructed properly, requires calling various classes.
        TODO: In another test, check conditional dependencies.
        """
        graph = generate_graph_data_model(
            helpers=helpers,
            data_model_name=data_model,
            data_model_labels=data_model_labels,
        )

        # Check that some edges are present as expected:
        assert ("FamilyHistory", "Breast") in graph.edges("FamilyHistory")

        if data_model_labels == "display_label":
            expected_valid_values = ["ab", "cd", "ef", "gh"]
            mock_id_label = "MockRDB_id"
            assert ("BulkRNAseqAssay", "Biospecimen") in graph.edges("BulkRNAseqAssay")

        else:
            expected_valid_values = ["Ab", "Cd", "Ef", "Gh"]
            mock_id_label = "MockRDBId"
            assert ("BulkRNA-seqAssay", "Biospecimen") in graph.edges(
                "BulkRNA-seqAssay"
            )
        assert expected_valid_values == [
            k
            for k, v in graph["CheckListEnum"].items()
            for vk, vv in v.items()
            if vk == "rangeValue"
        ]

        assert mock_id_label in graph.nodes

        # Check that all relationships recorded between 'CheckList' and 'Ab' are present
        assert (
            "rangeValue"
            and "parentOf" in graph["CheckListEnum"][expected_valid_values[0]]
        )
        assert (
            "requiresDependency" not in graph["CheckListEnum"][expected_valid_values[0]]
        )

        # Check nodes:
        assert "Patient" in graph.nodes
        assert "GRCh38" in graph.nodes

        # Check weights
        assert graph["Sex"]["Female"]["rangeValue"]["weight"] == 0

        assert (
            graph["MockComponent"]["CheckRegexFormat"]["requiresDependency"]["weight"]
            == 11
        )

        # Check Edge directions
        assert 4 == (len(graph.out_edges("TissueStatus")))
        assert 2 == (len(graph.in_edges("TissueStatus")))


class TestDataModelGraphExplorer:
    def test_find_properties(self):
        return

    def test_find_classes(self):
        return

    def test_find_node_range(self):
        return

    def test_get_adjacent_nodes_by_relationship(self):
        return

    @pytest.mark.parametrize(
        "data_model",
        ["example_required_vr_test.model.csv", "example_required_vr_test.model.jsonld"],
        ids=["csv", "jsonld"],
    )
    @pytest.mark.parametrize(
        ["manifest_component", "node_required"],
        [
            ["Patient", False],
            ["Biospecimen", True],
        ],
        ids=["Patient_Component", "Biospecimen_Component"],
    )
    @pytest.mark.parametrize(
        ["node_label", "node_display_name"],
        [["PatientID", None], [None, "Patient ID"]],
        ids=["use_node_label", "use_node_display_name"],
    )
    @pytest.mark.parametrize(
        "provide_vrs",
        [True, False],
        ids=["provide_vrs_True", "provide_vrs_False"],
    )
    def test_get_component_node_required(
        self,
        helpers,
        data_model: str,
        manifest_component: str,
        node_required: bool,
        provide_vrs: bool,
        node_label: str,
        node_display_name: str,
    ):
        # Get graph explorer
        DMGE = helpers.get_data_model_graph_explorer(path=data_model)

        # Get validation rules, if indicated to do so
        node_validation_rules = None
        if provide_vrs:
            node_validation_rules = DMGE.get_component_node_validation_rules(
                manifest_component=manifest_component,
                node_label=node_label,
                node_display_name=node_display_name,
            )

        # Find if component node is required then compare to expectations
        component_node_required = DMGE.get_component_node_required(
            manifest_component=manifest_component,
            node_validation_rules=node_validation_rules,
            node_label=node_label,
            node_display_name=node_display_name,
        )

        assert component_node_required == node_required

    @pytest.mark.parametrize(
        "data_model",
        ["example_required_vr_test.model.csv", "example_required_vr_test.model.jsonld"],
        ids=["csv", "jsonld"],
    )
    @pytest.mark.parametrize(
        ["manifest_component", "node_vrs"],
        [
            ["Patient", "unique warning"],
            ["Biospecimen", "unique required error"],
        ],
        ids=["Patient_Component", "Biospecimen_Component"],
    )
    @pytest.mark.parametrize(
        ["node_label", "node_display_name"],
        [["PatientID", None], [None, "Patient ID"]],
        ids=["use_node_label", "use_node_display_name"],
    )
    def test_get_component_node_validation_rules(
        self,
        helpers,
        data_model: str,
        manifest_component: str,
        node_vrs: str,
        node_label: str,
        node_display_name: str,
    ):
        # Get graph explorer
        DMGE = helpers.get_data_model_graph_explorer(path=data_model)

        # Get component node validation rules and compare to expectations
        node_validation_rules = DMGE.get_component_node_validation_rules(
            manifest_component=manifest_component,
            node_label=node_label,
            node_display_name=node_display_name,
        )

        assert node_validation_rules == [node_vrs]

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

    def test_get_subgraph_by_edge_type(self):
        return

    def test_find_adjacent_child_classes(self):
        return

    def test_find_parent_classes(self):
        return

    def test_full_schema_graph(self):
        return

    @pytest.mark.parametrize(
        "data_model",
        ["example.model.csv", "example.model.jsonld"],
        ids=["csv", "jsonld"],
    )
    @pytest.mark.parametrize(
        "class_name, expected_in_schema",
        [
            ("Patient", True),
            ("ptaient", False),
            ("Biospecimen", True),
            ("InvalidComponent", False),
        ],
    )
    def test_is_class_in_schema(
        self, helpers, class_name, expected_in_schema, data_model
    ):
        """
        Test to cover checking if a given class is in a schema.
        `is_class_in_schema` should return `True` if the class is in the schema
        and `False` if it is not.
        """
        DMGE = helpers.get_data_model_graph_explorer(path=data_model)
        # Check if class is in schema
        class_in_schema = DMGE.is_class_in_schema(class_name)

        # Assert value is as expected
        assert class_in_schema == expected_in_schema

    def test_sub_schema_graph(self):
        return


@pytest.mark.parametrize(
    "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
)
class TestDataModelNodes:
    def test_gather_nodes(self, helpers, data_model):
        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name=data_model
        )

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(
            helpers,
            data_model_name=data_model,
        )

        attr_info = ("Patient", attr_rel_dictionary["Patient"])
        nodes = data_model_nodes.gather_nodes(attr_info=attr_info)

        # Make sure there are no repeat nodes
        assert len(nodes) == len(set(nodes))

        # Make sure the nodes returned conform to expectations (values and order)
        ## The parsing records display names for relationships for CSV and labels for JSONLD, so the expectations are different between the two.
        expected_nodes = [
            "Patient",
            "Patient ID",
            "Sex",
            "Year of Birth",
            "Diagnosis",
            "Component",
            "DataType",
        ]

        assert nodes == expected_nodes

        # Ensure order is tested.
        reordered_nodes = nodes.copy()
        reordered_nodes.remove("Patient")
        reordered_nodes.append("Patient")
        assert reordered_nodes != expected_nodes

    def test_gather_all_nodes(self, helpers, data_model):
        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name=data_model
        )

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(
            helpers, data_model_name=data_model
        )

        all_nodes = data_model_nodes.gather_all_nodes_in_model(
            attr_rel_dict=attr_rel_dictionary
        )

        # Make sure there are no repeat nodes
        assert len(all_nodes) == len(set(all_nodes))

        # Check that nodes from first entry, are recoreded in order in all_nodes
        # Only check first entry, bc subsequent ones might be in the same order as would be gathered with gather_nodes if it contained a node that was already recorded.
        first_attribute = list(attr_rel_dictionary.keys())[0]
        attr_info = (first_attribute, attr_rel_dictionary[first_attribute])
        expected_starter_nodes = data_model_nodes.gather_nodes(attr_info=attr_info)
        actual_starter_nodes = all_nodes[0 : len(expected_starter_nodes)]

        assert actual_starter_nodes == expected_starter_nodes

    def test_get_rel_node_dict_info(self, helpers, data_model, relationships):
        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name=data_model
        )

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(
            helpers, data_model_name=data_model
        )

        for relationship in relationships:
            rel_dict_info = data_model_nodes.get_rel_node_dict_info(relationship)
            if rel_dict_info:
                assert type(rel_dict_info[0]) == str
                assert type(rel_dict_info[1]) == dict
                assert "default" in rel_dict_info[1].keys()

    def test_get_data_model_properties(self, helpers, data_model):
        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name=data_model
        )

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(
            helpers, data_model_name=data_model
        )

        # Get properties in the data model
        data_model_properties = data_model_nodes.get_data_model_properties(
            attr_rel_dictionary
        )

        # In the current example model, there are no properties, would need to update this section if properties are added.
        assert data_model_properties == []

        # Update the attr_rel_dictionary to add a property, then see if its found.
        # Get a random relationship key from the attr_rel_dictionary:
        all_keys = list(attr_rel_dictionary.keys())
        random_index = len(all_keys) - 1
        rel_key = all_keys[random.randint(0, random_index)]

        # Modify the contents of that relationship
        attr_rel_dictionary[rel_key]["Relationships"]["Properties"] = ["TestProperty"]

        # Get properties in the modified data model
        data_model_properties = data_model_nodes.get_data_model_properties(
            attr_rel_dictionary
        )

        assert data_model_properties == ["TestProperty"]

    def test_get_entry_type(self, helpers, data_model):
        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name=data_model
        )

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Update the attr_rel_dictionary to add a property, then see if it is assigned the correct entry type.
        # Get a random relationship key from the attr_rel_dictionary:
        all_keys = list(attr_rel_dictionary.keys())
        random_index = len(all_keys) - 1
        rel_key = all_keys[random.randint(0, random_index)]

        # Modify the contents of that relationship
        attr_rel_dictionary[rel_key]["Relationships"]["Properties"] = ["TestProperty"]

        # Instantiate DataModelNodes
        # Note: Get entry type uses self, so I will have to instantiate DataModelNodes outside of the generate_data_model_nodes function
        data_model_nodes = DataModelNodes(
            attribute_relationships_dict=attr_rel_dictionary
        )

        # In the example data model all attributes should be classes.
        for attr in attr_rel_dictionary.keys():
            entry_type = data_model_nodes.get_entry_type(attr)
            assert entry_type == "class"

        # Check that the added property is properly loaded as a property
        assert data_model_nodes.get_entry_type("TestProperty") == "property"

    @pytest.mark.parametrize(
        "rel_func", list(REL_FUNC_DICT.values()), ids=list(REL_FUNC_DICT.keys())
    )
    @pytest.mark.parametrize(
        "test_dn", list(TEST_DN_DICT.keys()), ids=list(TEST_DN_DICT.keys())
    )
    @pytest.mark.parametrize(
        "test_bool",
        ["True", "False", True, False, "kldjk"],
        ids=["True_str", "False_str", "True_bool", "False_bool", "Random_str"],
    )
    def test_run_rel_functions(self, helpers, data_model, rel_func, test_dn, test_bool):
        # Call each relationship function to ensure that it is returning the desired result.
        # Note all the called functions will also be tested in other unit tests.
        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name=data_model
        )

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(
            helpers, data_model_name=data_model
        )

        # Run functions the same way they are called in run_rel_functions:
        if rel_func == get_attribute_display_name_from_label:
            expected_display_names = list(attr_rel_dictionary.keys())
            returned_display_names = [
                data_model_nodes.run_rel_functions(
                    rel_func=get_attribute_display_name_from_label,
                    node_display_name=ndn,
                    attr_relationships=attr_rel_dictionary,
                )
                for ndn in expected_display_names
            ]

            assert expected_display_names == returned_display_names

        elif rel_func == parse_validation_rules:
            # Find attributes with validation rules
            # Gather Validation Rules
            vrs = []
            for k, v in attr_rel_dictionary.items():
                if "Validation Rules" in v["Relationships"].keys():
                    vrs.append(v["Relationships"]["Validation Rules"])
            parsed_vrs = []
            for attr in attr_rel_dictionary.keys():
                attr_relationships = attr_rel_dictionary[attr]["Relationships"]
                if "Validation Rules" in attr_relationships:
                    parsed_vrs.append(
                        data_model_nodes.run_rel_functions(
                            rel_func=parse_validation_rules,
                            attr_relationships=attr_relationships,
                            csv_header="Validation Rules",
                        )
                    )

            assert len(vrs) == len(parsed_vrs)
            if DATA_MODEL_DICT[data_model] == "CSV":
                for ind, rule in enumerate(vrs):
                    if "::" in rule[0]:
                        assert parsed_vrs[ind] == rule[0].split("::")
                    elif "^^" in rule[0]:
                        component_with_specific_rules = []
                        component_rule_sets = rule[0].split("^^")
                        components = [
                            cr.split(" ")[0].replace("#", "")
                            for cr in component_rule_sets
                        ]
                        if "" in components:
                            components.remove("")
                        for parsed_rule in parsed_vrs:
                            if isinstance(parsed_rule, dict):
                                for k in parsed_rule.keys():
                                    component_with_specific_rules.append(k)
                        assert all(
                            [
                                component in component_with_specific_rules
                                for component in components
                            ]
                        )
                    else:
                        assert parsed_vrs[ind] == rule
            elif DATA_MODEL_DICT[data_model] == "JSONLD":
                # JSONLDs already contain parsed validaiton rules so the raw vrs will match the parsed_vrs
                assert vrs == parsed_vrs

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
            csv_header = "Required"
            attr_relationships = {csv_header: test_bool}
            actual_conversion = data_model_nodes.run_rel_functions(
                rel_func=convert_bool_to_str,
                csv_header=csv_header,
                attr_relationships=attr_relationships,
            )
            if "true" in str(test_bool).lower():
                assert actual_conversion == True
            elif "false" in str(test_bool).lower():
                assert actual_conversion == False
            else:
                assert actual_conversion == None
        else:
            # If the function passed is not currently supported, should hit an error.
            try:
                data_model_nodes.run_rel_functions(rel_func=test_fake_func)
                convert_worked = False
            except:
                convert_worked = True
            assert convert_worked == True
        return

    @pytest.mark.parametrize(
        "node_display_name",
        list(NODE_DISPLAY_NAME_DICT.keys()),
        ids=["Node_required-" + str(v) for v in NODE_DISPLAY_NAME_DICT.values()],
    )
    @pytest.mark.parametrize(
        "data_model_labels",
        ["display_label", "class_label"],
        ids=["data_model_labels-display_label", "data_model_labels-class_label"],
    )
    def test_generate_node_dict(
        self, helpers, data_model, node_display_name, data_model_labels
    ):
        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers,
            data_model_name=data_model,
        )

        # Parse Model
        attr_rel_dictionary = data_model_parser.parse_model()

        # Change SourceManifest to sockComponent so we can check the data_model_labels is working as expected

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(
            helpers,
            data_model_name=data_model,
            data_model_labels=data_model_labels,
        )

        node_dict = data_model_nodes.generate_node_dict(
            node_display_name=node_display_name,
            attr_rel_dict=attr_rel_dictionary,
            data_model_labels=data_model_labels,
        )

        # Check that the output is as expected for the required key.
        if NODE_DISPLAY_NAME_DICT[node_display_name]:
            assert node_dict["required"] == True
        else:
            # Looking up this way, in case we add empty defaults back to JSONLD it wont fail, but will only be absent in JSONLD not CSV.
            if not node_dict["required"] == False:
                assert DATA_MODEL_DICT[data_model] == "JSONLD"

        # Check that the display name matches the label
        if data_model_labels == "display_label":
            assert node_display_name == node_dict["label"]

    def test_generate_node(self, helpers, data_model):
        # Test adding a dummy node
        node_dict = {"label": "test_label"}

        # Get Graph
        graph_data_model = generate_graph_data_model(
            helpers, data_model_name=data_model
        )

        # Instantiate DataModelNodes
        data_model_nodes = generate_data_model_nodes(
            helpers, data_model_name=data_model
        )

        # Assert the test node is not already in the graph
        assert node_dict["label"] not in graph_data_model.nodes

        # Add test node
        data_model_nodes.generate_node(graph_data_model, node_dict)

        # Check that the test node has been added
        assert node_dict["label"] in graph_data_model.nodes


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

    def test_skip_edge(self, helpers, DMR, data_model_edges):
        # Instantiate graph object and set node
        G = nx.MultiDiGraph()
        node = "Diagnosis"

        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name="validator_dag_test.model.csv"
        )

        # Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate data model Nodes object
        DMN = DataModelNodes(parsed_data_model)

        # Get edge relationships and all nodes from the parsed model
        edge_relationships = DMR.retreive_rel_headers_dict(edge=True)
        all_nodes = DMN.gather_all_nodes_in_model(attr_rel_dict=parsed_data_model)

        # Sanity check to ensure that the node we intend to test exists in the data model
        assert node in all_nodes

        # Add a single node to the graph
        node_dict = {}
        node_dict = DMN.generate_node_dict(node, parsed_data_model)
        node_dict[node] = node_dict
        G = DMN.generate_node(G, node_dict)

        # Check the edges in the graph, there should be none
        before_edges = deepcopy(G.edges)

        edge_list = []
        # Generate an edge in the graph with one node and a subset of the parsed data model
        # We're attempting to add an edge for a node that is the only one in the graph,
        # so `generate_edge` should skip adding edges and return the same graph
        edge_list_2 = data_model_edges.generate_edge(
            node,
            node_dict,
            {node: parsed_data_model[node]},
            edge_relationships,
            edge_list,
        )

        for node_1, node_2, edge_dict in edge_list_2:
            G.add_edge(node_1, node_2, key=edge_dict["key"], weight=edge_dict["weight"])

        # Assert that no edges were added and that the current graph edges are the same as before the call to `generate_edge`
        assert before_edges == G.edges

    @pytest.mark.parametrize(
        "node_to_add, edge_relationship",
        [
            ("DataType", "parentOf"),
            ("Female", "parentOf"),
            ("Sex", "requiresDependency"),
        ],
        ids=["subClassOf", "Valid Value", "all others"],
    )
    def test_generate_edge(
        self, helpers, DMR, data_model_edges, node_to_add, edge_relationship
    ):
        # Instantiate graph object
        G = nx.MultiDiGraph()

        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name="validator_dag_test.model.csv"
        )

        # Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate data model Nodes object
        DMN = DataModelNodes(parsed_data_model)

        # Get edge relationships and all nodes from the parsed model
        edge_relationships = DMR.retreive_rel_headers_dict(edge=True)
        all_nodes = DMN.gather_all_nodes_in_model(attr_rel_dict=parsed_data_model)

        # Sanity check to ensure that the node we intend to test exists in the data model
        assert node_to_add in all_nodes

        # Add all nodes to the graph
        all_node_dict = {}
        for node in all_nodes:
            node_dict = DMN.generate_node_dict(node, parsed_data_model)
            all_node_dict[node] = node_dict
            G = DMN.generate_node(G, node_dict)

        # Check the edges in the graph, there should be none
        before_edges = deepcopy(G.edges)

        edge_list = []

        # Generate edges for whichever node we are testing
        edge_list_2 = data_model_edges.generate_edge(
            node_to_add,
            all_node_dict,
            parsed_data_model,
            edge_relationships,
            edge_list,
        )

        for node_1, node_2, edge_dict in edge_list_2:
            G.add_edge(node_1, node_2, key=edge_dict["key"], weight=edge_dict["weight"])

        # Assert that the current edges are different from the edges of the graph before
        assert G.edges > before_edges

        # Assert that somewhere in the current edges for the node we added, that the correct relationship exists
        relationship_df = pd.DataFrame(G.edges, columns=["node1", "node2", "edge"])
        assert (relationship_df["edge"] == edge_relationship).any()

    @pytest.mark.parametrize(
        "node_to_add, other_node, expected_weight, data_model_path",
        [
            ("Patient ID", "Biospecimen", 1, "validator_dag_test.model.csv"),
            ("dataset_id", "cohorts", -1, "properties.test.model.csv"),
        ],
        ids=["list", "domainIncludes"],
    )
    def test_generate_weights(
        self,
        helpers,
        DMR,
        data_model_edges,
        node_to_add,
        other_node,
        expected_weight,
        data_model_path,
    ):
        # Instantiate graph object
        G = nx.MultiDiGraph()

        # Instantiate Parser
        data_model_parser = get_data_model_parser(
            helpers=helpers, data_model_name=data_model_path
        )

        # Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate data model Nodes object
        DMN = DataModelNodes(parsed_data_model)

        # Get edge relationships and all nodes from the parsed model
        edge_relationships = DMR.retreive_rel_headers_dict(edge=True)
        all_nodes = DMN.gather_all_nodes_in_model(attr_rel_dict=parsed_data_model)

        # Sanity check to ensure that the node we intend to test exists in the data model
        assert node_to_add in all_nodes

        # Add all nodes to the graph
        all_node_dict = {}
        for node in all_nodes:
            node_dict = DMN.generate_node_dict(node, parsed_data_model)
            all_node_dict[node] = node_dict
            G = DMN.generate_node(G, node_dict)

        # Check the edges in the graph, there should be none
        before_edges = deepcopy(G.edges)

        edge_list = []

        # Generate edges for whichever node we are testing
        edge_list_2 = data_model_edges.generate_edge(
            node_to_add,
            all_node_dict,
            parsed_data_model,
            edge_relationships,
            edge_list,
        )

        for node_1, node_2, edge_dict in edge_list_2:
            G.add_edge(node_1, node_2, key=edge_dict["key"], weight=edge_dict["weight"])

        # Assert that the current edges are different from the edges of the graph before
        assert G.edges > before_edges

        # Cast the edges and weights to a DataFrame for easier indexing
        edges_and_weights = pd.DataFrame(
            G.edges.data(), columns=["node1", "node2", "weights"]
        ).set_index("node1")

        # Weights are set to a negative nubmer to indicate that the weight cannot be known reliably beforehand and must be determined by reading the schema
        # Get the index of the property in the schema
        # Weights for properties are determined by their order in the schema.
        # This would allow the tests to continue to function correctly in the case were other attributes were added to the schema
        if expected_weight < 0:
            schema = helpers.get_data_frame(
                path=helpers.get_data_path(data_model_path), data_model=True
            )
            expected_weight = schema.index[schema["Attribute"] == other_node][0]
            logger.debug(
                f"Expected weight for the edge of nodes {node_to_add} and {other_node} is {expected_weight}."
            )

        # Assert that the weight added is what is expected
        if node_to_add in ["Patient ID"]:
            assert (
                edges_and_weights.loc[other_node, "weights"]["weight"]
                == expected_weight
            )
        elif node_to_add in ["cohorts"]:
            assert (
                edges_and_weights.loc[node_to_add, "weights"]["weight"]
                == expected_weight
            )


@pytest.mark.parametrize(
    "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
)
class TestDataModelJsonSchema:
    @pytest.mark.parametrize(
        "node_range",
        [[], ["healthy"], ["healthy", "cancer"]],
        ids=["empty_range", "single_range", "multi_range"],
    )
    @pytest.mark.parametrize(
        "node_name", ["", "Diagnosis"], ids=["empty_node_name", "Diagnosis_node_name"]
    )
    @pytest.mark.parametrize("blank", [True, False], ids=["True_blank", "False_blank"])
    def test_get_array_schema(self, helpers, data_model, node_range, node_name, blank):
        dmjs = get_data_model_json_schema(helpers=helpers, data_model_name=data_model)
        array_schema = dmjs.get_array_schema(
            node_range=node_range, node_name=node_name, blank=blank
        )

        # check node_name is recoreded as the key to the array schema
        assert node_name in array_schema

        # Check maxItems is the lenghth of node_range
        assert len(node_range) == array_schema[node_name]["maxItems"]

        # Check that blank value is added at the end of node_range, if true
        if blank:
            assert array_schema[node_name]["items"]["enum"][-1] == ""
            assert len(array_schema[node_name]["items"]["enum"]) == len(node_range) + 1
        else:
            assert array_schema[node_name]["items"]["enum"] == node_range
            assert len(array_schema[node_name]["items"]["enum"]) == len(node_range)

    @pytest.mark.parametrize(
        "node_name", ["", "Diagnosis"], ids=["empty_node_name", "Diagnosis_node_name"]
    )
    def test_get_non_blank_schema(self, helpers, data_model, node_name):
        dmjs = get_data_model_json_schema(helpers=helpers, data_model_name=data_model)
        non_blank_schema = dmjs.get_non_blank_schema(node_name=node_name)
        # check node_name is recoreded as the key to the array schema
        assert node_name in non_blank_schema
        assert non_blank_schema[node_name] == {"not": {"type": "null"}, "minLength": 1}

    @pytest.mark.parametrize(
        "node_range",
        [[], ["healthy"], ["healthy", "cancer"]],
        ids=["empty_range", "single_range", "multi_range"],
    )
    @pytest.mark.parametrize(
        "node_name", ["", "Diagnosis"], ids=["empty_node_name", "Diagnosis_node_name"]
    )
    @pytest.mark.parametrize("blank", [True, False], ids=["True_blank", "False_blank"])
    def test_get_range_schema(self, helpers, data_model, node_range, node_name, blank):
        dmjs = get_data_model_json_schema(helpers=helpers, data_model_name=data_model)

        range_schema = dmjs.get_range_schema(
            node_range=node_range, node_name=node_name, blank=blank
        )

        # check node_name is recoreded as the key to the array schema
        assert node_name in range_schema

        # Check that blank value is added at the end of node_range, if true
        if blank:
            assert range_schema[node_name]["enum"][-1] == ""
            assert len(range_schema[node_name]["enum"]) == len(node_range) + 1
        else:
            assert range_schema[node_name]["enum"] == node_range
            assert len(range_schema[node_name]["enum"]) == len(node_range)

    @pytest.mark.parametrize(
        "source_node", ["", "Patient"], ids=["empty_node_name", "patient_source"]
    )
    @pytest.mark.parametrize(
        "schema_name",
        ["", "Test_Schema_Name"],
        ids=["empty_schema_name", "schema_name"],
    )
    def test_get_json_validation_schema(
        self, helpers, data_model, source_node, schema_name
    ):
        dmjs = get_data_model_json_schema(helpers=helpers, data_model_name=data_model)

        data_model_path = helpers.get_data_path(path=data_model)
        json_schema_log_file_path = get_json_schema_log_file_path(
            data_model_path=data_model_path, source_node=source_node
        )

        # Remove json schema log file if it already exists.
        if os.path.exists(json_schema_log_file_path):
            os.remove(json_schema_log_file_path)
        assert os.path.exists(json_schema_log_file_path) == False

        try:
            # Get validation schema
            json_validation_schema = dmjs.get_json_validation_schema(
                source_node=source_node, schema_name=schema_name
            )

            # Check Keys in Schema
            expected_jvs_keys = [
                "$schema",
                "$id",
                "title",
                "type",
                "properties",
                "required",
                "allOf",
            ]
            actual_jvs_keys = list(json_validation_schema.keys())
            assert expected_jvs_keys == actual_jvs_keys

            # Check title
            assert schema_name == json_validation_schema["title"]

            # Check contents of validation schema
            assert "Diagnosis" in json_validation_schema["properties"]
            assert "Cancer" in json_validation_schema["properties"]["Diagnosis"]["enum"]

            # Check that log file is saved
            assert os.path.exists(json_schema_log_file_path) == True

            # Remove the log file that was created.
            os.remove(json_schema_log_file_path)

        except:
            # Should only fail if no source node is provided.
            assert source_node == ""


class TestDataModelJsonLd:
    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    def test_init(self, helpers, data_model):
        # Test that __init__ is being set up properly
        # Get Graph
        graph_data_model = generate_graph_data_model(
            helpers, data_model_name=data_model
        )

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(graph=graph_data_model)

        # Test that __init__ is being set up properly
        assert type(data_model_jsonld.graph) == nx.MultiDiGraph
        assert type(data_model_jsonld.rel_dict) == dict
        assert "required" in data_model_jsonld.rel_dict
        assert type(data_model_jsonld.dmge) == DataModelGraphExplorer
        assert data_model_jsonld.output_path == ""

    def test_base_jsonld_template(self, helpers):
        # Gather the templates
        base_template = BaseTemplate()
        base_jsonld_template = json.loads(base_template.to_json())

        # Test base template is constructed as expected
        assert "@context" in base_jsonld_template
        assert "@graph" in base_jsonld_template
        assert "@id" in base_jsonld_template

    def test_property_template(self, helpers):
        # Get Property Template
        empty_template = PropertyTemplate()
        property_template = json.loads(empty_template.to_json())

        expected_property_template = {
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
        assert property_template == expected_property_template

    def test_class_template(self, helpers):
        # Get Class Template
        empty_template = ClassTemplate()
        class_template = json.loads(empty_template.to_json())

        expected_class_template = {
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
        assert class_template == expected_class_template

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    @pytest.mark.parametrize(
        "template_type", ["property", "class"], ids=["property", "class"]
    )
    @pytest.mark.parametrize("node", ["", "Patient"], ids=["no node", "Patient"])
    @pytest.mark.parametrize(
        "data_model_labels",
        ["display_label", "class_label"],
        ids=["data_model_labels-display_label", "data_model_labels-class_label"],
    )
    def test_fill_entry_template(
        self, helpers, data_model, template_type, node, data_model_labels
    ):
        # Get Graph
        graph_data_model = generate_graph_data_model(
            helpers,
            data_model_name=data_model,
            data_model_labels=data_model_labels,
        )

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(graph=graph_data_model)

        # Get empty template
        if template_type == "property":
            property_template = PropertyTemplate()
            template = json.loads(property_template.to_json())
        elif template_type == "class":
            class_template = ClassTemplate()
            template = json.loads(class_template.to_json())

        # Make a copy of the template, since template is mutable
        template_copy = deepcopy(template)

        try:
            # Fill out template for given node.
            object_template = data_model_jsonld.fill_entry_template(
                template=template_copy, node=node
            )
            # Ensure template keys are present (not all original keys will be present due to cleaning empty values):
        except:
            # Should only fail if no node is given
            assert node == ""

        if "object_template" in locals():
            # Check that object template keys match the expected keys
            actual_keys = list(object_template.keys())
            if template_type == "property":
                expected_keys = [
                    "@id",
                    "@type",
                    "rdfs:comment",
                    "rdfs:label",
                    "schema:isPartOf",
                    "sms:displayName",
                    "sms:required",
                    "sms:validationRules",
                ]
            elif template_type == "class":
                expected_keys = [
                    "@id",
                    "@type",
                    "rdfs:comment",
                    "rdfs:label",
                    "rdfs:subClassOf",
                    "schema:isPartOf",
                    "sms:displayName",
                    "sms:required",
                    "sms:requiresDependency",
                    "sms:validationRules",
                ]

            assert (set(actual_keys) - set(expected_keys)) == (
                set(expected_keys) - set(actual_keys)
            )
            if data_model_labels == "display_label":
                assert (
                    object_template["rdfs:label"] == object_template["sms:displayName"]
                )

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    @pytest.mark.parametrize(
        "template_type", ["property", "class"], ids=["property", "class"]
    )
    def test_add_contexts_to_entries(self, helpers, data_model, template_type):
        # Will likely need to change when contexts added to model.
        # Get Graph
        graph_data_model = generate_graph_data_model(
            helpers, data_model_name=data_model
        )

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(graph=graph_data_model)

        # Get empty template
        if template_type == "property":
            property_template = PropertyTemplate()
            template = json.loads(property_template.to_json())
        elif template_type == "class":
            class_template = ClassTemplate()
            template = json.loads(class_template.to_json())

        # Make a copy of the template, since template is mutable
        template_copy = deepcopy(template)

        # Fill out template for given node.
        object_template = data_model_jsonld.fill_entry_template(
            template=template_copy, node="Patient"
        )

        if "sms:required" in object_template:
            assert "sms" in object_template["sms:required"]
        if "@id" in object_template:
            assert "bts" in object_template["@id"]

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    def test_clean_template(
        self, helpers, data_model: str, DMR: DataModelRelationships
    ):
        # TODO: This will need to change with contexts bc they are hard coded here.
        # Get Graph
        graph_data_model = generate_graph_data_model(
            helpers, data_model_name=data_model
        )

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(graph=graph_data_model)

        # Get empty template
        class_template = ClassTemplate()
        template = json.loads(class_template.to_json())

        # Make a copy of the template, since template is mutable
        template_copy = deepcopy(template)

        assert "sms:requiresDependency" in template_copy

        # Fill out some mock entries in the template:
        template_copy["@id"] == "bts:CheckURL"
        template_copy["rdfs:label"] == "CheckURL"
        data_model_relationships = DMR.relationships_dictionary

        # Clean template
        data_model_jsonld.clean_template(
            template=template_copy, data_model_relationships=data_model_relationships
        )

        # Look for expected changes after cleaning
        # Check that expected JSONLD default is added
        assert template_copy["sms:required"] == "sms:false"
        assert template_copy["sms:validationRules"] == []

        # Check that non-required JSONLD keys are removed.
        assert "sms:requiresDependency" not in template_copy

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    @pytest.mark.parametrize(
        "valid_values",
        [[], ["Other", "Female", "Male"], ["A", "Bad", "Entry"]],
        ids=["Empty List", "Disordered List", "Incorrect List"],
    )
    def test_reorder_template_entries(self, helpers, data_model, valid_values):
        # Note the way test_reorder_template_entries works, is that as long as an entry has recordings in the template
        # even if they are incorrect, they will be corrected within this function.
        # Get Graph
        graph_data_model = generate_graph_data_model(
            helpers, data_model_name=data_model
        )

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(graph=graph_data_model)

        # Get empty template
        class_template = ClassTemplate()
        template = json.loads(class_template.to_json())

        # Make a copy of the template, since template is mutable
        template_copy = deepcopy(template)

        # Fill out template with 'Sex' attribute from example model
        template_copy["@id"] = "Sex"
        template_copy["rdfs:label"] = "Sex"
        template_copy["sms:required"] = "sms:false"
        template_copy["schema:rangeIncludes"] = valid_values

        # Now reorder:
        data_model_jsonld.reorder_template_entries(template=template_copy)
        if valid_values:
            assert template_copy["schema:rangeIncludes"] == [
                {"@id": "bts:Female"},
                {"@id": "bts:Male"},
                {"@id": "bts:Other"},
            ]
        else:
            assert template_copy["schema:rangeIncludes"] == []

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    def test_generate_jsonld_object(self, helpers, data_model):
        # Check that JSONLD object is being made, and has some populated entries.

        # Get Graph
        graph_data_model = generate_graph_data_model(
            helpers,
            data_model_name=data_model,
        )

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(graph=graph_data_model)
        jsonld_dm = data_model_jsonld.generate_jsonld_object()

        assert list(jsonld_dm.keys()) == ["@context", "@graph", "@id"]
        assert len(jsonld_dm["@graph"]) > 1

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    def test_convert_graph_to_jsonld(self, helpers, data_model):
        # Get Graph
        graph_data_model = generate_graph_data_model(
            helpers, data_model_name=data_model
        )

        # Generate JSONLD
        jsonld_dm = convert_graph_to_jsonld(graph=graph_data_model)
        assert list(jsonld_dm.keys()) == ["@context", "@graph", "@id"]
        assert len(jsonld_dm["@graph"]) > 1
