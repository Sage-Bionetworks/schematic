from unittest.mock import MagicMock

import numpy as np
import pytest

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.store.synapse import SynapseStorage
from schematic.utils.validate_utils import comma_separated_list_regex
from tests.conftest import Helpers


@pytest.fixture(name="dmge", scope="function")
def DMGE(helpers: Helpers) -> DataModelGraphExplorer:
    """Fixture to instantiate a DataModelGraphExplorer object."""
    dmge = helpers.get_data_model_graph_explorer(path="example.model.jsonld")
    return dmge


class TestStoreSynapse:
    @pytest.mark.parametrize("hideBlanks", [True, False])
    @pytest.mark.parametrize(
        "label_options",
        ["display_label", "class_label"],
        ids=["display_label", "class_label"],
    )
    def test_process_row_annotations_hide_blanks(
        self,
        dmge: DataModelGraphExplorer,
        synapse_store: SynapseStorage,
        hideBlanks: bool,
        label_options: str,
    ) -> None:
        """ensure that blank values are not added to the annotations dictionary if hideBlanks is True"""

        metadata_syn_with_blanks = {
            "PatientID": "value1",
            "Sex": "value2",
            "Diagnosis": "",  # Blank value (empty string)
            "FamilyHistory": 3,  # Non-string value
            "YearofBirth": np.nan,  # Blank value (NaN)
            "CancerType": "   ",  # Blank value (whitespace string)
        }
        annos = {
            "PatientID": "old_value1",
            "Sex": "old_value2",
            "Diagnosis": "old_value3",
            "FamilyHistory": "old_value4",
            "YearofBirth": "old_value5",
            "CancerType": "old_value6",
        }
        comma_separated_list = comma_separated_list_regex()
        processed_annos = synapse_store.process_row_annotations(
            dmge=dmge,
            metadata_syn=metadata_syn_with_blanks,
            csv_list_regex=comma_separated_list,
            hide_blanks=hideBlanks,
            annos=annos,
            annotation_keys=label_options,
        )
        # make sure that empty keys are removed if hideBlanks is True
        if hideBlanks:
            assert (
                "Diagnosis"
                and "YearofBirth"
                and "CancerType" not in processed_annos.keys()
            )
        else:
            # make sure that empty keys are added if hideBlanks is False
            # make sure that nan values are converted to empty strings
            assert processed_annos["Diagnosis"] == ""
            assert processed_annos["YearofBirth"] == ""
            assert processed_annos["CancerType"] == "   "

        # make sure that annotations already in the dictionary are not overwritten
        assert processed_annos["PatientID"] == "value1"
        assert processed_annos["Sex"] == "value2"
        assert processed_annos["FamilyHistory"] == 3

    @pytest.mark.parametrize(
        "label_options",
        ["display_label", "class_label"],
        ids=["display_label", "class_label"],
    )
    @pytest.mark.parametrize("hideBlanks", [True, False])
    def test_process_row_annotations_get_validation(
        self,
        dmge: DataModelGraphExplorer,
        synapse_store: SynapseStorage,
        hideBlanks: bool,
        label_options: str,
    ) -> None:
        """ensure that get_node_validation_rules is called with the correct arguments"""
        comma_separated_list = comma_separated_list_regex()
        metadata_syn = {
            "FamilyHistory": "value1,value2,value3",
        }
        annos = {"FamilyHistory": "old_value"}

        dmge.get_node_validation_rules = MagicMock()

        # pretend that "FamilyHistory" has a list of validation rules
        dmge.get_node_validation_rules.return_value = ["list", "regex"]

        processed_annos = synapse_store.process_row_annotations(
            dmge=dmge,
            metadata_syn=metadata_syn,
            csv_list_regex=comma_separated_list,
            hide_blanks=hideBlanks,
            annos=annos,
            annotation_keys=label_options,
        )

        if label_options == "display_label":
            # get_node_validation_rules was called with node_display_name
            dmge.get_node_validation_rules.assert_any_call(
                node_display_name="FamilyHistory"
            )
            dmge.get_node_validation_rules.assert_any_call(
                node_display_name="FamilyHistory"
            )
        else:
            # get_node_validation_rules was called with node_label
            dmge.get_node_validation_rules.assert_any_call(node_label="FamilyHistory")
        # ensure that the value is split into a list
        assert processed_annos["FamilyHistory"] == ["value1", "value2", "value3"]
