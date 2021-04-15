import os
import math
import logging
import pytest

import pandas as pd
from synapseclient import EntityViewSchema

from schematic.store.base import BaseStorage
from schematic.store.synapse import SynapseStorage, DatasetFileView


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DATASET_ID = "syn25057021"


@pytest.fixture
def synapse_store():
    access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")
    if access_token:
        synapse_store = SynapseStorage(access_token=access_token)
    else:
        synapse_store = SynapseStorage()
    yield synapse_store


@pytest.fixture
def dataset_fileview(synapse_store):
    dataset_fileview = DatasetFileView(DATASET_ID, synapse_store.syn)
    yield dataset_fileview
    dataset_fileview.delete()


@pytest.fixture
def dataset_fileview_table(dataset_fileview):
    table = dataset_fileview.query(tidy=False, force=True)
    yield table


@pytest.fixture
def dataset_fileview_table_tidy(dataset_fileview, dataset_fileview_table):
    table = dataset_fileview.tidy_table()
    yield table


class TestBaseStorage:
    def test_init(self):

        with pytest.raises(NotImplementedError):
            BaseStorage()


class TestSynapseStorage:
    def test_init(self, synapse_store):
        assert synapse_store.storageFileview == "syn23643253"
        assert isinstance(synapse_store.storageFileviewTable, pd.DataFrame)

    def test_getFileAnnotations(self, synapse_store):
        expected_dict = {
            "author": "bruno, milen, sujay",
            "impact": "42.9",
            "confidence": "high",
            "YearofBirth": "1980",
            "entityId": "syn25057024",
            "FileFormat": "txt",
        }
        actual_dict = synapse_store.getFileAnnotations("syn25057024")

        # For simplicity, just checking if eTag is present since
        # it changes anytime the files on Synapse change
        assert "eTag" in actual_dict
        del actual_dict["eTag"]

        assert expected_dict == actual_dict

    @pytest.mark.parametrize("force_batch", [True, False], ids=["batch", "non_batch"])
    def test_getDatasetAnnotations(self, synapse_store, force_batch):
        expected_df = pd.DataFrame.from_records(
            [
                {
                    "Filename": "TestDataset-Annotations-v2/Sample_A.txt",
                    "author": "bruno, milen, sujay",
                    "impact": "42.9",
                    "confidence": "high",
                    "FileFormat": "txt",
                    "YearofBirth": "1980",
                },
                {
                    "Filename": "TestDataset-Annotations-v2/Sample_B.txt",
                    "confidence": "low",
                    "FileFormat": "csv",
                    "date": "2020-02-01",
                },
                {
                    "Filename": "TestDataset-Annotations-v2/Sample_C.txt",
                    "FileFormat": "fastq",
                },
            ]
        ).fillna("")
        actual_df = synapse_store.getDatasetAnnotations(
            DATASET_ID, force_batch=force_batch
        )

        # For simplicity, just checking if eTag and entityId are present
        # since they change anytime the files on Synapse change
        assert "eTag" in actual_df
        assert "entityId" in actual_df
        actual_df.drop(columns=["eTag", "entityId"], inplace=True)

        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)

    def test_getDatasetProject(self, synapse_store):

        assert synapse_store.getDatasetProject(DATASET_ID) == "syn23643250"
        assert synapse_store.getDatasetProject("syn23643250") == "syn23643250"

        assert synapse_store.getDatasetProject("syn24992812") == "syn24992754"
        assert synapse_store.getDatasetProject("syn24992754") == "syn24992754"

        with pytest.raises(ValueError):
            synapse_store.getDatasetProject("syn12345678")


class TestDatasetFileView:
    def test_init(self, dataset_fileview, synapse_store):

        assert dataset_fileview.datasetId == DATASET_ID
        assert dataset_fileview.synapse is synapse_store.syn
        assert dataset_fileview.parentId == DATASET_ID
        assert isinstance(dataset_fileview.view_schema, EntityViewSchema)

    def test_enter_exit(self, synapse_store):

        # Within the 'with' statement, the file view should be available
        with DatasetFileView(DATASET_ID, synapse_store.syn) as fileview:
            assert isinstance(fileview.view_schema, EntityViewSchema)
            assert synapse_store.syn.get(fileview.view_schema) is not None

        # Outside the 'with' statement, the file view should be unavailable
        assert fileview.view_schema is None

    def test_query(self, dataset_fileview_table):

        table = dataset_fileview_table

        # The content is tested in test_getDatasetAnnotations()
        # These tests are intentionally superficial
        # Some of these confirm the untidyness
        assert isinstance(table, pd.DataFrame)

        # Check for untidy default columns
        assert "ROW_ID" in table
        assert "ROW_ETAG" in table

        # Check for untidy list-columns
        author_row = table["ROW_ID"] == 25057024
        assert author_row.any()
        assert "author" in table
        author_value = table.loc[author_row, "author"].values[0]
        assert author_value == ["bruno", "milen", "sujay"]

        # Check for untidy integer-columns
        assert "YearofBirth" in table
        year_value = table.loc[author_row, "YearofBirth"].values[0]
        assert isinstance(year_value, float)
        assert math.isclose(year_value, 1980.0)

    def test_tidy_table(self, dataset_fileview_table_tidy):

        table = dataset_fileview_table_tidy

        # The content is tested in test_getDatasetAnnotations()
        # These tests are intentionally superficial
        # Some of these confirm the untidyness
        assert isinstance(table, pd.DataFrame)

        # Check for untidy default columns
        assert "entityId" in table
        assert "eTag" in table
        assert table.index.name == "entityId"

        # Check for untidy list-columns
        selected_row = table["entityId"] == "syn25057024"
        assert selected_row.any()
        assert "author" in table
        author_value = table.loc[selected_row, "author"].values[0]
        assert author_value == "bruno, milen, sujay"

        # Check for untidy integer-columns
        assert "YearofBirth" in table
        year_value = table.loc[selected_row, "YearofBirth"].values[0]
        assert isinstance(year_value, str)
        assert year_value == "1980"
