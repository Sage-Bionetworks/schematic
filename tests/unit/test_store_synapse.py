import pandas as pd
import pytest

from schematic.store.synapse import SynapseStorage


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
