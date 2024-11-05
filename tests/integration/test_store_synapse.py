from re import compile as re_compile
from unittest.mock import MagicMock

import numpy as np
import pytest

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.store.synapse import SynapseStorage
from schematic.utils.general import syn_id_regex
from schematic.utils.validate_utils import comma_separated_list_regex


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
            "annotations": {
                "annotations": {
                    "PatientID": "old_value1",
                    "Sex": "old_value2",
                    "Diagnosis": "old_value3",
                    "FamilyHistory": "old_value4",
                    "YearofBirth": "old_value5",
                    "CancerType": "old_value6",
                }
            }
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
        processed_annos = processed_annos["annotations"]["annotations"]

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
        annos = {"annotations": {"annotations": {"FamilyHistory": "old_value"}}}

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
        assert processed_annos["annotations"]["annotations"]["FamilyHistory"] == [
            "value1",
            "value2",
            "value3",
        ]

    @pytest.mark.parametrize(
        "asset_view, dataset_id, expected_files",
        [
            (
                "syn23643253",
                "syn61374924",
                [
                    ("syn61374926", "schematic - main/BulkRNASeq and files/txt1.txt"),
                    ("syn61374930", "schematic - main/BulkRNASeq and files/txt2.txt"),
                    ("syn62282720", "schematic - main/BulkRNASeq and files/txt4.txt"),
                    ("syn62282794", "schematic - main/BulkRNASeq and files/txt3.txt"),
                ],
            ),
            (
                "syn23643253",
                "syn25614635",
                [
                    (
                        "syn25614636",
                        "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_A.txt",
                    ),
                    (
                        "syn25614637",
                        "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_B.txt",
                    ),
                    (
                        "syn25614638",
                        "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_C.txt",
                    ),
                ],
            ),
            (
                "syn63917487",
                "syn63917494",
                [
                    (
                        "syn63917518",
                        "schematic - main/Test files and dataset annotations/Test BulkRNAseq w annotation/txt4.txt",
                    ),
                    (
                        "syn63917520",
                        "schematic - main/Test files and dataset annotations/Test BulkRNAseq w annotation/txt1.txt",
                    ),
                    (
                        "syn63917521",
                        "schematic - main/Test files and dataset annotations/Test BulkRNAseq w annotation/txt2.txt",
                    ),
                    (
                        "syn63917522",
                        "schematic - main/Test files and dataset annotations/Test BulkRNAseq w annotation/txt3.txt",
                    ),
                ],
            ),
            (
                "syn23643253",
                "syn63927665",
                [
                    (
                        "syn63927670",
                        "schematic - main/BulkRNAseq nested files/data/txt4.txt",
                    ),
                    (
                        "syn63927671",
                        "schematic - main/BulkRNAseq nested files/data/txt1.txt",
                    ),
                    (
                        "syn63927672",
                        "schematic - main/BulkRNAseq nested files/data/txt2.txt",
                    ),
                    (
                        "syn63927673",
                        "schematic - main/BulkRNAseq nested files/data/txt3.txt",
                    ),
                ],
            ),
            (
                "syn23643253",
                "syn63987067",
                [
                    (
                        "syn63987071",
                        "schematic - main/BulkRNAseq and double nested files/dataset/folder 1/data/txt4.txt",
                    ),
                    (
                        "syn63987072",
                        "schematic - main/BulkRNAseq and double nested files/dataset/folder 1/data/txt1.txt",
                    ),
                    (
                        "syn63987073",
                        "schematic - main/BulkRNAseq and double nested files/dataset/folder 1/data/txt2.txt",
                    ),
                    (
                        "syn63987074",
                        "schematic - main/BulkRNAseq and double nested files/dataset/folder 1/data/txt3.txt",
                    ),
                ],
            ),
        ],
    )
    def test_getFilesInStorageDataset(self, asset_view, dataset_id, expected_files):
        # GIVEN a SynapseStorage object with the appropriate asset view
        syn = SynapseStorage()
        syn.storageFileView = asset_view
        # WHEN getFilesInStorageDataset is called for the given dataset
        dataset_files = syn.getFilesInStorageDataset(dataset_id)
        # THEN the expected files are returned
        # AND there are no unexpected files
        assert dataset_files == expected_files
        # AND the (synId, path) order is correct
        synapse_id_regex = re_compile(syn_id_regex())
        assert synapse_id_regex.fullmatch(dataset_files[0][0])
