from __future__ import annotations
import os
import math
import logging
import pytest
from time import sleep 
from tenacity import Retrying, RetryError, stop_after_attempt, wait_random_exponential

import pandas as pd
from synapseclient import EntityViewSchema, Folder

from schematic.models.metadata import MetadataModel
from schematic.store.base import BaseStorage
from schematic.store.synapse import SynapseStorage, DatasetFileView
from schematic.utils.cli_utils import get_from_config
from schematic.schemas.generator import SchemaGenerator
from synapseclient.core.exceptions import SynapseHTTPError

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture
def synapse_store():
    access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")
    if access_token:
        synapse_store = SynapseStorage(access_token=access_token)
    else:
        synapse_store = SynapseStorage()
    yield synapse_store


@pytest.fixture
def dataset_fileview(dataset_id, synapse_store):
    dataset_fileview = DatasetFileView(dataset_id, synapse_store.syn)
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

@pytest.fixture
def projectId(synapse_store, helpers):
    projectId = helpers.get_python_project(helpers)
    yield projectId

@pytest.fixture
def datasetId(synapse_store, projectId):
    dataset = Folder(
        name = 'Test  Dataset',
        parent = projectId,
        )

    datasetId = synapse_store.syn.store(dataset).id
    yield datasetId
    synapse_store.syn.delete(datasetId)


def raise_final_error(retry_state):
    return retry_state.outcome.result()

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
            "FileFormat": "txt",
            "IsImportantBool": "True",
            "IsImportantText": "TRUE",
        }
        actual_dict = synapse_store.getFileAnnotations("syn25614636")

        # For simplicity, just checking if eTag and entityId are present
        # since they change anytime the files on Synapse change
        assert "eTag" in actual_dict
        del actual_dict["eTag"]
        assert "entityId" in actual_dict
        del actual_dict["entityId"]

        assert expected_dict == actual_dict

    def test_annotation_submission(self, synapse_store, helpers, config):
        manifest_path = "mock_manifests/annotations_test_manifest.csv"

        # Upload dataset annotations
        inputModelLocaiton = helpers.get_data_path(get_from_config(config.DATA, ("model", "input", "location")))
        sg = SchemaGenerator(inputModelLocaiton)

        try:        
            for attempt in Retrying(
                stop = stop_after_attempt(15),
                wait = wait_random_exponential(multiplier=1,min=10,max=120),
                retry_error_callback = raise_final_error
                ):
                with attempt:         
                    manifest_id = synapse_store.associateMetadataWithFiles(
                        schemaGenerator = sg,
                        metadataManifestPath = helpers.get_data_path(manifest_path),
                        datasetId = 'syn34295552',
                        manifest_record_type = 'entity',
                        useSchemaLabel = True,
                        hideBlanks = True,
                        restrict_manifest = False,
                    )
        except RetryError:
            pass

        # Retrive annotations
        entity_id, entity_id_spare = helpers.get_data_frame(manifest_path)["entityId"][0:2]
        annotations = synapse_store.getFileAnnotations(entity_id)

        # Check annotations of interest
        assert annotations['CheckInt'] == '7'
        assert annotations['CheckList'] == 'valid, list, values'
        assert 'CheckRecommended' not in annotations.keys()





    @pytest.mark.parametrize("force_batch", [True, False], ids=["batch", "non_batch"])
    def test_getDatasetAnnotations(self, dataset_id, synapse_store, force_batch):
        expected_df = pd.DataFrame.from_records(
            [
                {
                    "Filename": "TestDataset-Annotations-v3/Sample_A.txt",
                    "author": "bruno, milen, sujay",
                    "impact": "42.9",
                    "confidence": "high",
                    "FileFormat": "txt",
                    "YearofBirth": "1980",
                    "IsImportantBool": "True",
                    "IsImportantText": "TRUE",
                },
                {
                    "Filename": "TestDataset-Annotations-v3/Sample_B.txt",
                    "confidence": "low",
                    "FileFormat": "csv",
                    "date": "2020-02-01",
                },
                {
                    "Filename": "TestDataset-Annotations-v3/Sample_C.txt",
                    "FileFormat": "fastq",
                    "IsImportantBool": "False",
                    "IsImportantText": "FALSE",
                },
            ]
        ).fillna("")
        actual_df = synapse_store.getDatasetAnnotations(
            dataset_id, force_batch=force_batch
        )

        # For simplicity, just checking if eTag and entityId are present
        # since they change anytime the files on Synapse change
        assert "eTag" in actual_df
        assert "entityId" in actual_df
        actual_df.drop(columns=["eTag", "entityId"], inplace=True)

        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)

    def test_getDatasetProject(self, dataset_id, synapse_store):

        assert synapse_store.getDatasetProject(dataset_id) == "syn23643250"
        assert synapse_store.getDatasetProject("syn23643250") == "syn23643250"

        assert synapse_store.getDatasetProject("syn24992812") == "syn24992754"
        assert synapse_store.getDatasetProject("syn24992754") == "syn24992754"

        with pytest.raises(PermissionError):
            synapse_store.getDatasetProject("syn12345678")


class TestDatasetFileView:
    def test_init(self, dataset_id, dataset_fileview, synapse_store):

        assert dataset_fileview.datasetId == dataset_id
        assert dataset_fileview.synapse is synapse_store.syn
        assert dataset_fileview.parentId == dataset_id
        assert isinstance(dataset_fileview.view_schema, EntityViewSchema)

    def test_enter_exit(self, dataset_id, synapse_store):

        # Within the 'with' statement, the file view should be available
        with DatasetFileView(dataset_id, synapse_store.syn) as fileview:
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
        sample_a_row = [True, False, False]
        assert "author" in table
        author_value = table.loc[sample_a_row, "author"].values[0]
        assert author_value == ["bruno", "milen", "sujay"]

        # Check for untidy integer-columns
        assert "YearofBirth" in table
        year_value = table.loc[sample_a_row, "YearofBirth"].values[0]
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
        sample_a_row = [True, False, False]
        assert "author" in table
        author_value = table.loc[sample_a_row, "author"][0]
        assert author_value == "bruno, milen, sujay"

        # Check for untidy integer-columns
        assert "YearofBirth" in table
        year_value = table.loc[sample_a_row, "YearofBirth"][0]
        assert isinstance(year_value, str)
        assert year_value == "1980"

class TestTableOperations:

    def test_createTable(self, helpers, synapse_store, config, projectId, datasetId):

        # Check if FollowUp table exists if so delete
        existing_tables = synapse_store.get_table_info(projectId = projectId)
        
        if "followup_synapse_storage_manifest_table" in existing_tables.keys():
            synapse_store.syn.delete(existing_tables["followup_synapse_storage_manifest_table"])
            # assert no table
            assert "followup_synapse_storage_manifest_table" not in synapse_store.get_table_info(projectId = projectId).keys()

        # associate metadata with files
        manifest_path = "mock_manifests/local_manifest.csv"
        inputModelLocaiton = helpers.get_data_path(get_from_config(config.DATA, ("model", "input", "location")))
        sg = SchemaGenerator(inputModelLocaiton)

        # updating file view on synapse takes a long time
        manifestId = synapse_store.associateMetadataWithFiles(
            schemaGenerator = sg,
            metadataManifestPath = helpers.get_data_path(manifest_path),
            datasetId = datasetId,
            manifest_record_type = 'table',
            useSchemaLabel = True,
            hideBlanks = True,
            restrict_manifest = False,
        )
        existing_tables = synapse_store.get_table_info(projectId = projectId)
        
        # clean Up
        synapse_store.syn.delete(manifestId)
        # assert table exists
        assert "followup_synapse_storage_manifest_table" in existing_tables.keys()


    def test_replaceTable(self, helpers):

        # Check if FollowUp table exists
            # if so delete

        # assert no table
        # Associate barebones FollowUp manifest with files
        # Query table for certain column
        # assert empty/no results
        # import filled FollowUp Manifest
        # Associate filled manifest with files
        # query table
        # assert results exist
        # delete table        

        assert True

    def test_updateTable(self, helpers):
        assert True
