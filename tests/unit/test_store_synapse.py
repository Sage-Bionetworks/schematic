import pandas as pd
import pytest

from schematic.store.synapse import SynapseStorage
from schematic.schemas.data_model_graph import DataModelGraphExplorer


class TestStoreSynapse:
    synapse_store = SynapseStorage()
    test_manifest_good = pd.DataFrame(
        {"Filename": ["test_file.txt"], "entityId": ["syn1"]}
    )
    test_manifest_no_entityid = pd.DataFrame({"Filename": ["test_file.txt"]})
    test_dataset_files = [
        ("syn2", "test_file_1.txt"),
        ("syn3", "test_file_2.txt"),
    ]

    DATA_MODEL_DICT = {"example.model.csv": "CSV", "example.model.jsonld": "JSONLD"}

    def test_get_file_entityIds_only_new_files_manifest_is_none(self) -> None:
        with pytest.raises(UnboundLocalError, match="No manifest was passed in"):
            self.synapse_store._get_file_entityIds(
                dataset_files=[], only_new_files=True, manifest=None
            )

    def test_get_file_entityIds_only_new_files_manifest_no_entityId_column(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="The manifest in your dataset"):
            self.synapse_store._get_file_entityIds(
                dataset_files=[],
                only_new_files=True,
                manifest=self.test_manifest_no_entityid,
            )

    def test_get_file_entityIds_only_new_files_manifest_good(self) -> None:
        assert self.synapse_store._get_file_entityIds(
            dataset_files=self.test_dataset_files,
            only_new_files=True,
            manifest=self.test_manifest_good,
        ) == {
            "Filename": ["test_file_1.txt", "test_file_2.txt"],
            "entityId": ["syn2", "syn3"],
        }

    def test_get_file_entityIds_only_new_files_manifest_good_no_new_files(self) -> None:
        assert self.synapse_store._get_file_entityIds(
            dataset_files=[("syn1", "test_file.txt")],
            only_new_files=True,
            manifest=self.test_manifest_good,
        ) == {"Filename": [], "entityId": []}

    def test_get_file_entityIds_all_files(self) -> None:
        assert self.synapse_store._get_file_entityIds(
            dataset_files=self.test_dataset_files,
            only_new_files=False,
            manifest=self.test_manifest_good,
        ) == {
            "Filename": ["test_file_1.txt", "test_file_2.txt"],
            "entityId": ["syn2", "syn3"],
        }

    @pytest.mark.parametrize(
        "manifest_dataframe, expected",
        [
            # Case 1a: 'id' in lowercase — should be normalized to 'Id'
            (
                pd.DataFrame({"id": ["test_value"]}),
                pd.DataFrame({"Id": ["test_value"], "entityId": [""]}),
            ),
            # Case 1b: 'ID' in uppercase — should be normalized to 'Id'
            (
                pd.DataFrame({"ID": ["test_value"]}),
                pd.DataFrame({"Id": ["test_value"], "entityId": [""]}),
            ),
            # Case 1c: 'iD' mixed case — should be normalized to 'Id'
            (
                pd.DataFrame({"iD": ["test_value"]}),
                pd.DataFrame({"Id": ["test_value"], "entityId": [""]}),
            ),
            # Case 2: 'Uuid' present — should be renamed to 'Id'
            (
                pd.DataFrame({"Uuid": ["test_value"]}),
                pd.DataFrame({"Id": ["test_value"], "entityId": [""]}),
            ),
            # Case 3: 'entityID' in wrong mixed case — should be renamed to 'entityId'
            (
                pd.DataFrame({"Id": ["test_value"], "entityId": ["test_value"]}),
                pd.DataFrame({"Id": ["test_value"], "entityId": ["test_value"]}),
            ),
            # Case 4: 'EntityId' in wrong mixed case — should be renamed to 'entityId'
            (
                pd.DataFrame({"Id": ["test_value"], "EntityId": ["test_value"]}),
                pd.DataFrame({"Id": ["test_value"], "entityId": ["test_value"]}),
            ),
        ],
    )
    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    def test_add_id_columns_to_manifest(
        self, data_model, manifest_dataframe, expected, helpers
    ) -> None:
        dmge = helpers.get_data_model_graph_explorer(path=data_model)
        pd.testing.assert_frame_equal(
            self.synapse_store._add_id_columns_to_manifest(manifest_dataframe, dmge),
            expected,
        )
