from typing import Optional, Union

import pytest

from tests.conftest import Helpers
from schematic.schemas.data_model_graph import create_dmge

DATA_MODEL_DICT = {"example.model.csv": "CSV", "example.model.jsonld": "JSONLD"}


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
    "data_model_path",
    ["tests/data/example.model.csv", "tests/data/example.model.jsonld"],
)
def test_create_dmge(data_model_path: str) -> None:
    """
    Tests for create_dmge
    Tests that the dmge is created
    """
    dmge = create_dmge(data_model_path)
    assert dmge
