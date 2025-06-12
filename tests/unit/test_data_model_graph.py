from typing import Optional, Union

import pytest

from schematic.schemas.data_model_relationships import JSONSchemaType
from schematic.schemas.data_model_graph import (
    DataModelGraph,
    DataModelGraphExplorer,
    create_data_model_graph_explorer,
)
from schematic.schemas.data_model_parser import DataModelParser
from tests.conftest import Helpers

DATA_MODEL_DICT = {"example.model.csv": "CSV", "example.model.jsonld": "JSONLD"}


@pytest.fixture(name="column_type_dmge_jsonld", scope="module")
def fixture_column_type_dmge_jsonld() -> DataModelGraphExplorer:
    """Returns a DataModelGraphExplorer using the example data model with columnType attributes"""
    data_model_parser = DataModelParser(
        path_to_data_model="tests/data/example.model_column_type.jsonld"
    )
    parsed_data_model = data_model_parser.parse_model()
    data_model_grapher = DataModelGraph(parsed_data_model)
    graph_data_model = data_model_grapher.graph
    return DataModelGraphExplorer(graph_data_model)


@pytest.fixture(name="column_type_dmge_csv", scope="module")
def fixture_column_type_dmge_csv() -> DataModelGraphExplorer:
    """Returns a DataModelGraphExplorer using the example data model with columnType attributes"""
    data_model_parser = DataModelParser(
        path_to_data_model="tests/data/example.model.column_type_component.csv"
    )
    parsed_data_model = data_model_parser.parse_model()
    data_model_grapher = DataModelGraph(parsed_data_model)
    graph_data_model = data_model_grapher.graph
    return DataModelGraphExplorer(graph_data_model)


class TestDataModelGraphExplorer:
    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    @pytest.mark.parametrize(
        "node_label, node_display_name, expected_validation_rule",
        [
            # Test case 1: node label is provided
            (
                "PatientID",
                None,
                {"Biospecimen": "unique error", "Patient": "unique warning"},
            ),
            (
                "CheckRegexListStrict",
                None,
                ["list strict", "regex match [a-f]"],
            ),
            # Test case 2: node label is not provided and display label is not part of the schema
            (
                None,
                "invalid display label",
                [],
            ),
            # Test case 3: node label is not provided but a valid display label is provided
            (
                None,
                "Patient ID",
                {"Biospecimen": "unique error", "Patient": "unique warning"},
            ),
        ],
    )
    def test_get_node_validation_rules_valid(
        self,
        helpers: Helpers,
        data_model: str,
        node_label: Optional[str],
        node_display_name: Optional[str],
        expected_validation_rule: Union[list[str], dict[str, str]],
    ):
        DMGE = helpers.get_data_model_graph_explorer(path=data_model)

        node_validation_rules = DMGE.get_node_validation_rules(
            node_label=node_label, node_display_name=node_display_name
        )
        assert node_validation_rules == expected_validation_rule

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    @pytest.mark.parametrize(
        "node_label, node_display_name",
        [
            # Test case 1: node label and node display name are not provided
            (
                None,
                None,
            ),
            # Test case 2: node label is not valid and display name is not provided
            (
                "invalid node",
                None,
            ),
        ],
    )
    def test_get_node_validation_rules_invalid(
        self,
        helpers,
        data_model,
        node_label,
        node_display_name,
    ):
        DMGE = helpers.get_data_model_graph_explorer(path=data_model)
        with pytest.raises(ValueError):
            DMGE.get_node_validation_rules(
                node_label=node_label, node_display_name=node_display_name
            )

    @pytest.mark.parametrize(
        "node_label, expected_type",
        [
            ("CheckString", "string"),
            ("CheckNum", "number"),
            ("CheckURL", None),
        ],
        ids=["CheckString", "CheckNum", "CheckURL"],
    )
    def test_get_node_column_type_with_node_labels_jsonld(
        self,
        node_label: str,
        expected_type: Optional[JSONSchemaType],
        column_type_dmge_jsonld: DataModelGraphExplorer,
    ) -> None:
        """Tests for DataModelGraphExplorer.get_node_column_type using node label"""
        assert (
            column_type_dmge_jsonld.get_node_column_type(node_label=node_label)
            == expected_type
        )

    @pytest.mark.parametrize(
        "node_label, expected_type",
        [
            ("Stringtype", "string"),
            ("Numtype", "number"),
            ("CheckURL", None),
        ],
        ids=["Stringtype", "Numtype", "CheckURL"],
    )
    def test_get_node_column_type_with_node_labels_csv(
        self,
        node_label: str,
        expected_type: Optional[JSONSchemaType],
        column_type_dmge_csv: DataModelGraphExplorer,
    ) -> None:
        """Tests for DataModelGraphExplorer.get_node_column_type using node label"""
        assert (
            column_type_dmge_csv.get_node_column_type(node_label=node_label)
            == expected_type
        )

    @pytest.mark.parametrize(
        "node_display_name, expected_type",
        [
            ("Check String", "string"),
            ("Check Num", "number"),
            ("Check URL", None),
        ],
        ids=["Check String", "Check Num", "Check URL"],
    )
    def test_get_node_column_type_with_display_name(
        self,
        node_display_name: str,
        expected_type: Optional[JSONSchemaType],
        column_type_dmge_jsonld: DataModelGraphExplorer,
    ) -> None:
        """Tests for DataModelGraphExplorer.get_node_column_type using node display name"""
        assert (
            column_type_dmge_jsonld.get_node_column_type(
                node_display_name=node_display_name
            )
            == expected_type
        )


@pytest.mark.parametrize(
    "data_model_path",
    ["tests/data/example.model.csv", "tests/data/example.model.jsonld"],
)
def test_create_data_model_graph_explorer(data_model_path: str) -> None:
    """
    Tests for create_data_model_graph_explorer
    Tests that the dmge is created
    """
    dmge = create_data_model_graph_explorer(data_model_path)
    assert dmge
