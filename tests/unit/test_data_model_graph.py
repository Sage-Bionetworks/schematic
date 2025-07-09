from typing import Optional, Union

import pytest

from schematic.schemas.data_model_relationships import JSONSchemaType
from tests.conftest import Helpers

DATA_MODEL_DICT = {"example.model.csv": "CSV", "example.model.jsonld": "JSONLD"}
COLUMN_TYPE_DATA_MODEL_DICT = {
    "example.model.column_type_component.csv": "CSV",
    "example.model.column_type_component.jsonld": "JSONLD",
}


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
            ("Stringtype", "string"),
            ("Numtype", "number"),
            ("Missingtype", None),
        ],
        ids=["Stringtype", "Numtpye", "Missingtype"],
    )
    @pytest.mark.parametrize(
        "data_model",
        list(COLUMN_TYPE_DATA_MODEL_DICT.keys()),
        ids=list(COLUMN_TYPE_DATA_MODEL_DICT.values()),
    )
    def test_get_node_column_type_with_node_labels(
        self,
        node_label: str,
        expected_type: Optional[str],
        data_model: str,
        helpers: Helpers,
    ) -> None:
        """Tests for DataModelGraphExplorer.get_node_column_type using node label"""
        dmge = helpers.get_data_model_graph_explorer(path=data_model)
        assert dmge.get_node_column_type(node_label=node_label) == expected_type

    @pytest.mark.parametrize(
        "node_display_name, expected_type",
        [
            ("String type", "string"),
            ("Num type", "number"),
            ("Missing type", None),
        ],
        ids=["String type", "Num type", "Missing type"],
    )
    @pytest.mark.parametrize(
        "data_model",
        list(COLUMN_TYPE_DATA_MODEL_DICT.keys()),
        ids=list(COLUMN_TYPE_DATA_MODEL_DICT.values()),
    )
    def test_get_node_column_type_with_node_display_names(
        self,
        node_display_name: str,
        expected_type: Optional[str],
        data_model: str,
        helpers: Helpers,
    ) -> None:
        """Tests for DataModelGraphExplorer.get_node_column_type using node label"""
        dmge = helpers.get_data_model_graph_explorer(path=data_model)
        assert (
            dmge.get_node_column_type(node_display_name=node_display_name)
            == expected_type
        )
