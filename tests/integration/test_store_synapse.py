from unittest.mock import MagicMock

import numpy as np
import pytest

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.store.synapse import SynapseStorage
from schematic.utils.validate_utils import comma_separated_list_regex
from tests.conftest import Helpers


@pytest.fixture
def metadataSyn():
    return {
        "key1": "value1",
        "key2": np.nan,
        "key3": "val1,val2,val3",  # Simulate a CSV-like string
        "key4": "another_value",
    }


@pytest.fixture
def annos():
    return {"key1": "old_value1", "key2": "old_value2", "key3": "old_value3"}


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
        annos: dict,
        hideBlanks: bool,
        label_options: str,
    ):
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
            "PatientID": "value1",
            "Sex": "value2",
            "Diagnosis": "value3",
            "FamilyHistory": "value4",
            "YearofBirth": "value5",
            "CancerType": "value6",
        }
        comma_separated_list = comma_separated_list_regex()
        process_row_annos = synapse_store.process_row_annotations(
            dmge=dmge,
            metadataSyn=metadata_syn_with_blanks,
            csv_list_regex=comma_separated_list,
            hideBlanks=hideBlanks,
            annos=annos,
            annotation_keys=label_options,
        )
        # make sure that empty keys are not added if hideBlanks is True
        if hideBlanks:
            assert (
                "Diagnosis"
                and "YearofBirth"
                and "CancerType" not in process_row_annos.keys()
            )
        assert (
            "Diagnosis"
            and "YearofBirth"
            and "CancerType"
            and "PatientID"
            and "Sex"
            and "FamilyHistory" in process_row_annos.keys()
        )
        # make sure that annotations already in the dictionary are not overwritten
        assert "PatientID" and "Sex" in process_row_annos.keys()

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
        label_options: str,
    ):
        """ensure that get_node_validation_rules is called with the correct arguments"""
        comma_separated_list = comma_separated_list_regex()
        metadata_syn = {
            "PatientID": "value1",
        }
        annos = {"PatientID": "old_value"}

        dmge.get_node_validation_rules = MagicMock()
        process_row_annos = synapse_store.process_row_annotations(
            dmge=dmge,
            metadataSyn=metadata_syn,
            csv_list_regex=comma_separated_list,
            hideBlanks=True,
            annos=annos,
            annotation_keys=label_options,
        )

        if label_options == "display_label":
            # get_node_validation_rules was called with node_display_name="PatientID" at least once
            dmge.get_node_validation_rules.assert_any_call(
                node_display_name="PatientID"
            )
            # get_node_validation_rules was called with node_display_name="Sex" at least once
            dmge.get_node_validation_rules.assert_any_call(node_display_name="Sex")
        else:
            # get_node_validation_rules was called with node_label="PatientID" at least once
            dmge.get_node_validation_rules.assert_any_call(node_label="PatientID")
            # get_node_validation_rules was called with node_label="Sex" at least once
            dmge.get_node_validation_rules.assert_any_call(node_label="Sex")
