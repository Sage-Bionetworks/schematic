"""Tests for store module"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import shutil
import tempfile
import uuid
from contextlib import nullcontext as does_not_raise
from typing import Any, Callable, Generator
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from numpy import nan
from pandas.testing import assert_frame_equal
from synapseclient import EntityViewSchema, Folder
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.entity import File, Project
from synapseclient.models import Annotations
from synapseclient.models import Folder as FolderModel

from schematic.configuration.configuration import CONFIG, Configuration
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_parser import DataModelParser
from schematic.store.base import BaseStorage
from schematic.store.synapse import DatasetFileView, ManifestDownload, SynapseStorage
from schematic.utils.general import check_synapse_cache_size, create_temp_folder
from tests.conftest import Helpers
from tests.utils import CleanupItem

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture
def test_download_manifest_id():
    yield "syn51203973"


@pytest.fixture
def mock_manifest_download(synapse_store, test_download_manifest_id):
    md = ManifestDownload(synapse_store.syn, test_download_manifest_id)
    yield md


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
def version(synapse_store, helpers):
    yield helpers.get_python_version()


@pytest.fixture
def projectId(synapse_store, helpers):
    projectId = helpers.get_python_project(helpers)
    yield projectId


@pytest.fixture
def datasetId(
    synapse_store: SynapseStorage, projectId: str, helpers, schedule_for_cleanup
):
    dataset = Folder(
        name="Table Test Dataset "
        + helpers.get_python_version()
        + f" integration_test_{str(uuid.uuid4()).replace('-', '_')}",
        parent=projectId,
    )

    datasetId = synapse_store.syn.store(dataset).id
    schedule_for_cleanup(CleanupItem(synapse_id=datasetId))
    yield datasetId


@pytest.fixture
def dmge(
    helpers: Helpers, config: Configuration
) -> Generator[DataModelGraphExplorer, None, None]:
    """initiate data model explorer

    Args:
        helpers (pytest fixture): fixture
        config (Configuration): configuration class

    Yields:
        DataModelGraphExplorer
    """
    # associate org FollowUp metadata with files
    input_model_location = helpers.get_data_path(
        os.path.basename(config.model_location)
    )
    data_model_parser = DataModelParser(path_to_data_model=input_model_location)
    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    # Instantiate DataModelGraphExplorer
    dmge = DataModelGraphExplorer(graph_data_model)
    yield dmge


@pytest.fixture
def mock_file() -> File:
    return File(parentId="syn123", id="syn456", name="mock_file")


@pytest.fixture(scope="module")
def synapse_store_special_scope():
    yield SynapseStorage(perform_query=False)


def raise_final_error(retry_state):
    return retry_state.outcome.result()


class TestBaseStorage:
    def test_init(self):
        with pytest.raises(NotImplementedError):
            BaseStorage()


class TestSynapseStorage:
    "Tests the SynapseStorage class"

    def test_init(self, synapse_store: SynapseStorage) -> None:
        """Tests SynapseStorage.__init__"""
        assert synapse_store.storageFileview == "syn23643253"
        assert isinstance(synapse_store.storageFileviewTable, pd.DataFrame)
        assert synapse_store.root_synapse_cache.endswith(".synapseCache")

    def test__purge_synapse_cache(self) -> None:
        """Tests SynapseStorage._purge_synapse_cache"""
        synapse_store = SynapseStorage(synapse_cache_path="test_cache_dir")
        size_before_purge = check_synapse_cache_size(synapse_store.root_synapse_cache)
        synapse_store._purge_synapse_cache(
            maximum_storage_allowed_cache_gb=0.000001, minute_buffer=0
        )
        size_after_purge = check_synapse_cache_size(synapse_store.root_synapse_cache)
        assert size_before_purge > size_after_purge
        shutil.rmtree("test_cache_dir")

    def test_login(self) -> None:
        """Tests SynapseStorage.login"""
        synapse_client = SynapseStorage.login()
        assert synapse_client.cache.cache_root_dir.endswith(".synapseCache")
        synapse_client = SynapseStorage.login("test_cache_dir")
        assert synapse_client.cache.cache_root_dir == "test_cache_dir"
        shutil.rmtree("test_cache_dir")

    @pytest.mark.parametrize(
        "project_scope,columns,where_clauses,expected,expected_new_query",
        [
            (None, None, None, "SELECT * FROM syn23643253 ;", True),
            (
                ["syn23643250"],
                None,
                None,
                "SELECT * FROM syn23643253 WHERE projectId IN ('syn23643250', '') ;",
                True,
            ),
            (
                None,
                None,
                ["projectId IN ('syn23643250')"],
                "SELECT * FROM syn23643253 WHERE projectId IN ('syn23643250') ;",
                True,
            ),
            (
                ["syn23643250"],
                ["name", "id", "path"],
                None,
                "SELECT name,id,path FROM syn23643253 WHERE projectId IN ('syn23643250', '') ;",
                True,
            ),
            (
                None,
                ["name", "id", "path"],
                ["parentId='syn61682648'", "type='file'"],
                "SELECT name,id,path FROM syn23643253 WHERE parentId='syn61682648' AND type='file' ;",
                True,
            ),
            (
                ["syn23643250"],
                None,
                ["parentId='syn61682648'", "type='file'"],
                "SELECT * FROM syn23643253 WHERE parentId='syn61682648' AND type='file' AND projectId IN ('syn23643250', '') ;",
                True,
            ),
            (
                ["syn23643250"],
                ["name", "id", "path"],
                ["parentId='syn61682648'", "type='file'"],
                "SELECT name,id,path FROM syn23643253 WHERE parentId='syn61682648' AND type='file' AND projectId IN ('syn23643250', '') ;",
                True,
            ),
            (
                ["syn23643250"],
                ["name", "id", "path"],
                ["parentId='syn61682648'", "type='file'"],
                "SELECT name,id,path FROM syn23643253 WHERE parentId='syn61682648' AND type='file' AND projectId IN ('syn23643250', '') ;",
                False,
            ),
        ],
    )
    def test_view_query(
        self,
        synapse_store_special_scope: SynapseStorage,
        project_scope: list,
        columns: list,
        where_clauses: list,
        expected: str,
        expected_new_query: bool,
    ) -> None:
        # GIVEN a the correct fileview
        assert synapse_store_special_scope.storageFileview == "syn23643253"

        # AND the approrpiate project scope
        synapse_store_special_scope.project_scope = project_scope

        # WHEN the query is built and run
        # THEN it should complete without raising an exception
        with does_not_raise():
            synapse_store_special_scope.query_fileview(columns, where_clauses)
            # AND the query string should be as expected
            assert synapse_store_special_scope.fileview_query == expected
            # AND query should have recieved a non-empty table
            assert synapse_store_special_scope.storageFileviewTable.empty is False
            # AND the query should be new if expected
            assert synapse_store_special_scope.new_query_different == expected_new_query

    @pytest.mark.parametrize(
        "asset_view,columns,message",
        [
            (
                "syn62339865",
                ["path"],
                r"The path column has not been added to the fileview. .*",
            ),
            (
                "syn62340177",
                ["id"],
                r"The columns id specified in the query do not exist in the fileview. .*",
            ),
        ],
    )
    def test_view_query_exception(
        self,
        asset_view: str,
        columns: list[str],
        message: str,
    ) -> None:
        # GIVEN a project scope
        project_scope = ["syn23643250"]

        # AND a test configuration
        TEST_CONFIG = Configuration()
        with patch(
            "schematic.store.synapse.CONFIG", return_value=TEST_CONFIG
        ) as mock_config:
            # AND the appropriate test file view
            mock_config.synapse_master_fileview_id = asset_view
            # AND a real path to the synapse config file
            mock_config.synapse_configuration_path = CONFIG.synapse_configuration_path
            # AND a unique synapse storage object that uses the values modified in the test config
            synapse_store = SynapseStorage(perform_query=False)
            # AND the given project scope
            synapse_store.project_scope = project_scope

            # WHEN the query is built and run
            # THEN it should raise a ValueError with the appropriate message
            with pytest.raises(ValueError, match=message):
                synapse_store.query_fileview(columns)

    def test_getFileAnnotations(self, synapse_store: SynapseStorage) -> None:
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

    @pytest.mark.parametrize("only_new_files", [True, False])
    def test_get_file_entityIds(self, helpers, synapse_store, only_new_files):
        # TODO: Automatically reset manifest at path specified below after each test
        # so that subsequent runs do not affect each other
        manifest_path = "mock_manifests/test_BulkRNAseq.csv"
        dataset_files = synapse_store.getFilesInStorageDataset("syn39241199")

        if only_new_files:
            # Prepare manifest is getting Ids for new files only
            manifest = helpers.get_data_frame(manifest_path)
            entityIds = pd.DataFrame({"entityId": ["syn39242580", "syn51900502"]})

            # If this line errors out then the changes on the manifest file need to be discarded
            manifest = manifest.join(entityIds)

            # get entityIds for new files
            files_and_Ids = synapse_store._get_file_entityIds(
                dataset_files=dataset_files,
                only_new_files=only_new_files,
                manifest=manifest,
            )

            # Assert that there are no new files
            for value in files_and_Ids.values():
                assert value == []

        else:
            # get entityIds for all files
            files_and_Ids = synapse_store._get_file_entityIds(
                dataset_files=dataset_files, only_new_files=only_new_files
            )

            # assert that the correct number of files were found
            assert len(files_and_Ids["entityId"]) == 2

    @pytest.mark.parametrize(
        "test_annotations, dataset_id, manifest_record_type, temporary_file_copy",
        [
            (
                {"CheckInt": "7", "CheckList": "valid, list, values"},
                "syn34295552",
                "file_and_entities",
                "annotations_test_manifest.csv",
            ),
            (
                {"FileFormat": "BAM", "GenomeBuild": "GRCh38"},
                "syn39241199",
                "table_and_file",
                "test_BulkRNAseq.csv",
            ),
        ],
        ids=["non file-based", "file-based"],
        indirect=["temporary_file_copy"],
    )
    def test_annotation_submission(
        self,
        synapse_store: SynapseStorage,
        helpers,
        test_annotations: dict[str, str],
        dataset_id: str,
        manifest_record_type: str,
        dmge: DataModelGraphExplorer,
        temporary_file_copy: Generator[str, None, None],
    ):
        """Test annotation submission"""
        synapse_store.associateMetadataWithFiles(
            dmge=dmge,
            metadataManifestPath=temporary_file_copy,
            datasetId=dataset_id,
            manifest_record_type=manifest_record_type,
            hideBlanks=True,
            restrict_manifest=False,
        )

        # Retrive annotations
        entity_id = helpers.get_data_frame(temporary_file_copy)["entityId"][0]
        annotations = synapse_store.getFileAnnotations(entity_id)

        # Check annotations of interest
        for key in test_annotations.keys():
            assert key in annotations.keys()
            assert annotations[key] == test_annotations[key]

        if temporary_file_copy.endswith("annotations_test_manifest_copy.csv"):
            assert "CheckRecommended" not in annotations.keys()
        elif temporary_file_copy.endswith("test_BulkRNAseq_copy.csv"):
            entity = synapse_store.syn.get(entity_id)
            assert isinstance(entity, File)

    @pytest.mark.parametrize("force_batch", [True, False], ids=["batch", "non_batch"])
    def test_getDatasetAnnotations(self, dataset_id, synapse_store, force_batch):
        expected_df = pd.DataFrame.from_records(
            [
                {
                    "Filename": "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_A.txt",
                    "author": "bruno, milen, sujay",
                    "impact": "42.9",
                    "confidence": "high",
                    "FileFormat": "txt",
                    "YearofBirth": "1980",
                    "IsImportantBool": "True",
                    "IsImportantText": "TRUE",
                },
                {
                    "Filename": "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_B.txt",
                    "confidence": "low",
                    "FileFormat": "csv",
                    "date": "2020-02-01",
                },
                {
                    "Filename": "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_C.txt",
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

    @pytest.mark.parametrize(
        "full_path,expected",
        [
            (
                True,
                [
                    ("syn126", "schematic - main/parent_folder/test_file"),
                    (
                        "syn125",
                        "schematic - main/parent_folder/test_folder/test_file_2",
                    ),
                ],
            ),
            (False, [("syn126", "test_file"), ("syn125", "test_file_2")]),
        ],
    )
    def test_getFilesInStorageDataset(self, synapse_store, full_path, expected):
        mock_return = [
            (
                ("parent_folder", "syn123"),
                [("test_folder", "syn124")],
                [("test_file", "syn126")],
            ),
            (
                (
                    os.path.join("schematic - main", "parent_folder", "test_folder"),
                    "syn124",
                ),
                [],
                [("test_file_2", "syn125")],
            ),
        ]
        with patch(
            "synapseutils.walk_functions._help_walk", return_value=mock_return
        ) as mock_walk_patch, patch(
            "schematic.store.synapse.SynapseStorage.getDatasetProject",
            return_value="syn23643250",
        ) as mock_project_id_patch, patch(
            "synapseclient.entity.Entity.__getattr__", return_value="schematic - main"
        ) as mock_project_name_patch, patch.object(
            synapse_store.syn, "get", return_value=Project(name="schematic - main")
        ):
            file_list = synapse_store.getFilesInStorageDataset(
                datasetId="syn_mock", fileNames=None, fullpath=full_path
            )
        assert file_list == expected

    @pytest.mark.parametrize("downloadFile", [True, False])
    def test_getDatasetManifest(self, synapse_store, downloadFile):
        # get a test manifest
        manifest_data = synapse_store.getDatasetManifest("syn51204502", downloadFile)

        # make sure the file gets downloaded
        if downloadFile:
            assert manifest_data["name"] == "synapse_storage_manifest_censored.csv"
            assert os.path.exists(manifest_data["path"])
            # clean up
            os.remove(manifest_data["path"])
        else:
            # return manifest id
            assert manifest_data == "syn51204513"

    @pytest.mark.parametrize(
        "existing_manifest_df,fill_in_return_value,expected_df",
        [
            (
                pd.DataFrame(),
                [
                    {
                        "Filename": ["new_mock_file_path"],
                        "entityId": ["new_mock_entity_id"],
                    },
                    {
                        "Filename": ["new_mock_file_path"],
                        "entityId": ["new_mock_entity_id"],
                    },
                ],
                pd.DataFrame(
                    {
                        "Filename": ["new_mock_file_path"],
                        "entityId": ["new_mock_entity_id"],
                    }
                ),
            ),
            (
                pd.DataFrame(
                    {
                        "Filename": ["existing_mock_file_path"],
                        "entityId": ["existing_mock_entity_id"],
                    }
                ),
                [
                    {
                        "Filename": ["existing_mock_file_path", "new_mock_file_path"],
                        "entityId": ["existing_mock_entity_id", "new_mock_entity_id"],
                    },
                    {
                        "Filename": ["new_mock_file_path"],
                        "entityId": ["new_mock_entity_id"],
                    },
                ],
                pd.DataFrame(
                    {
                        "Filename": ["existing_mock_file_path", "new_mock_file_path"],
                        "entityId": ["existing_mock_entity_id", "new_mock_entity_id"],
                    }
                ),
            ),
        ],
    )
    def test_fill_in_entity_id_filename(
        self, synapse_store, existing_manifest_df, fill_in_return_value, expected_df
    ):
        with patch(
            "schematic.store.synapse.SynapseStorage.getFilesInStorageDataset",
            return_value=["syn123", "syn124", "syn125"],
        ) as mock_get_file_storage, patch(
            "schematic.store.synapse.SynapseStorage._get_file_entityIds",
            side_effect=fill_in_return_value,
        ) as mock_get_file_entity_id:
            dataset_files, new_manifest = synapse_store.fill_in_entity_id_filename(
                datasetId="test_syn_id", manifest=existing_manifest_df
            )

            assert_frame_equal(new_manifest, expected_df)
            assert dataset_files == ["syn123", "syn124", "syn125"]

    # Test case: make sure that Filename and entityId column get filled and component column has the same length as filename column
    def test_add_entity_id_and_filename_with_component_col(self, synapse_store):
        with patch(
            "schematic.store.synapse.SynapseStorage._get_files_metadata_from_dataset",
            return_value={
                "Filename": ["test_file1", "test_file2"],
                "entityId": ["syn123", "syn124"],
            },
        ):
            mock_manifest = pd.DataFrame.from_dict(
                {"Filename": [""], "Component": ["MockComponent"], "Sample ID": [""]}
            ).reset_index(drop=True)
            manifest_to_return = synapse_store.add_entity_id_and_filename(
                datasetId="mock_syn_id", manifest=mock_manifest
            )
            expected_df = pd.DataFrame.from_dict(
                {
                    "Filename": ["test_file1", "test_file2"],
                    "Component": ["MockComponent", "MockComponent"],
                    "Sample ID": ["", ""],
                    "entityId": ["syn123", "syn124"],
                }
            )
            assert_frame_equal(manifest_to_return, expected_df)

    # Test case: make sure that Filename and entityId column get filled when component column does not exist
    def test_add_entity_id_and_filename_without_component_col(self, synapse_store):
        with patch(
            "schematic.store.synapse.SynapseStorage._get_files_metadata_from_dataset",
            return_value={
                "Filename": ["test_file1", "test_file2"],
                "entityId": ["syn123", "syn124"],
            },
        ):
            mock_manifest = pd.DataFrame.from_dict(
                {"Filename": [""], "Sample ID": [""]}
            ).reset_index(drop=True)
            manifest_to_return = synapse_store.add_entity_id_and_filename(
                datasetId="mock_syn_id", manifest=mock_manifest
            )
            expected_df = pd.DataFrame.from_dict(
                {
                    "Filename": ["test_file1", "test_file2"],
                    "Sample ID": ["", ""],
                    "entityId": ["syn123", "syn124"],
                }
            )
            assert_frame_equal(manifest_to_return, expected_df)

    @pytest.mark.parametrize(
        "hideBlanks, annotation_keys",
        [
            (True, "display_label"),
            (False, "display_label"),
            (True, "class_label"),
            (False, "class_label"),
        ],
    )
    async def test_format_row_annotations_entity_id_trash_can(
        self,
        caplog: pytest.LogCaptureFixture,
        dmge: DataModelGraph,
        synapse_store: SynapseStorage,
        hideBlanks: bool,
        annotation_keys: str,
    ) -> None:
        """make sure that missing_entity_handler gets triggered when entity is in the trash can"""
        with patch(
            "schematic.store.synapse.SynapseStorage.get_async_annotation",
            side_effect=SynapseHTTPError("entity syn123 is in the trash can"),
            new_callable=AsyncMock,
        ):
            mock_row_dict = {
                "Component": "MockComponent",
                "Mock_id": 1,
                "Id": "Mock_id",
                "entityId": "mock_syn_id",
            }
            mock_row = pd.Series(mock_row_dict)
            with caplog.at_level(logging.WARNING):
                formatted_annotations = await synapse_store.format_row_annotations(
                    dmge,
                    mock_row,
                    entityId="mock_syn_id",
                    hideBlanks=hideBlanks,
                    annotation_keys=annotation_keys,
                )
                assert "entity syn123 is in the trash can" in caplog.text
                assert formatted_annotations == None

    def test_get_files_metadata_from_dataset(self, synapse_store):
        patch_get_children = [
            ("syn123", "parent_folder/test_A.txt"),
            ("syn456", "parent_folder/test_B.txt"),
        ]
        mock_file_entityId = {
            "Filename": ["parent_folder/test_A.txt", "parent_folder/test_B.txt"],
            "entityId": ["syn123", "syn456"],
        }
        with patch(
            "schematic.store.synapse.SynapseStorage.getFilesInStorageDataset",
            return_value=patch_get_children,
        ):
            with patch(
                "schematic.store.synapse.SynapseStorage._get_file_entityIds",
                return_value=mock_file_entityId,
            ):
                dataset_file_names_id_dict = (
                    synapse_store._get_files_metadata_from_dataset(
                        "mock dataset id", only_new_files=True
                    )
                )
                assert dataset_file_names_id_dict == {
                    "Filename": [
                        "parent_folder/test_A.txt",
                        "parent_folder/test_B.txt",
                    ],
                    "entityId": ["syn123", "syn456"],
                }

    async def test_get_async_annotation(self, synapse_store: SynapseStorage) -> None:
        """test get annotation async function"""
        mock_syn_id = "syn1234"

        with patch(
            "schematic.store.synapse.get_entity_id_bundle2",
            new_callable=AsyncMock,
            return_value="mock",
        ) as mock_get_entity_id_bundle2:
            mock_get_entity_id_bundle2.return_value = "mock"
            result = await synapse_store.get_async_annotation(synapse_id=mock_syn_id)

            mock_get_entity_id_bundle2.assert_called_once_with(
                entity_id=mock_syn_id,
                request={"includeAnnotations": True},
                synapse_client=synapse_store.syn,
            )
            assert result == "mock"

    async def test_store_async_annotation(self, synapse_store: SynapseStorage) -> None:
        """test store annotations async function"""
        annos_dict = {
            "annotations": {
                "id": "mock_syn_id",
                "etag": "mock etag",
                "annotations": {
                    "Id": {"type": "STRING", "value": ["mock value"]},
                    "EntityId": {"type": "STRING", "value": ["mock_syn_id"]},
                    "SampleID": {"type": "STRING", "value": [""]},
                    "Component": {"type": "STRING", "value": ["mock value"]},
                },
            },
            "FileFormat": "mock format",
            "Component": "mock component",
            "Id": "mock_string",
            "EntityId": "mock_id",
        }
        expected_dict = Annotations(
            annotations={
                "Id": ["mock_string"],
                "EntityId": ["mock_syn_id"],
                "SampleID": [""],
                "Component": ["mock value"],
                "FileFormat": ["mock_format"],
            },
            etag="mock etag",
            id="mock syn_id",
        )

        with patch(
            "schematic.store.synapse.Annotations.store_async",
            new_callable=AsyncMock,
            return_value=expected_dict,
        ) as mock_store_async:
            result = await synapse_store.store_async_annotation(annos_dict)

            mock_store_async.assert_called_once_with(synapse_client=synapse_store.syn)
            assert result == expected_dict
            assert isinstance(result, Annotations)

    async def test_process_store_annos_failure(
        self, synapse_store: SynapseStorage
    ) -> None:
        """test _process_store_annos function when there's an error either getting or storing annotations"""

        async def mock_failure_coro():
            raise ValueError("sample error")

        # create tasks that will fail
        tasks = set()
        tasks.add(asyncio.create_task(mock_failure_coro()))

        synapse_store._process_store_annos
        # make sure error message can be raised
        with pytest.raises(RuntimeError, match="failed with"):
            await synapse_store._process_store_annos(tasks)

    async def test_process_store_annos_success_store(
        self, synapse_store: SynapseStorage
    ) -> None:
        """test _process_store_annos function and make sure that annotations can be stored after successfully getting annotations."""
        # mock annotation obtained after async_store
        stored_annos = Annotations(
            annotations={
                "Id": ["mock_string"],
                "EntityId": ["mock_syn_id"],
                "SampleID": [""],
                "Component": ["mock value"],
                "FileFormat": ["mock_format"],
            },
            etag="mock etag",
            id="mock_syn_id",
        )

        async def mock_success_coro():
            return stored_annos

        with patch(
            "schematic.store.synapse.SynapseStorage.store_async_annotation",
            new_callable=AsyncMock,
        ) as mock_store_async1:
            tasks = set()
            tasks.add(asyncio.create_task(mock_success_coro()))
            await synapse_store._process_store_annos(tasks)
            # make sure that the if statement is working
            mock_store_async1.assert_not_called()

    async def test_process_store_annos_success_get(
        self, synapse_store: SynapseStorage
    ) -> None:
        """test _process_store_annos function and make sure that task of storing annotations can be triggered"""
        # mock annotation obtained after get_async
        mock_annos_dict = {
            "annotations": {
                "id": "mock_syn_id",
                "etag": "mock etag",
                "annotations": {
                    "Id": {"type": "STRING", "value": ["mock value"]},
                    "EntityId": {"type": "STRING", "value": ["mock_syn_id"]},
                    "SampleID": {"type": "STRING", "value": [""]},
                    "Component": {"type": "STRING", "value": ["mock value"]},
                },
            },
            "FileFormat": "mock format",
            "Component": "mock component",
            "Id": "mock_string",
            "EntityId": "mock_id",
        }

        mock_stored_annos = Annotations(
            annotations={
                "Id": ["mock_string"],
                "EntityId": ["mock_syn_id"],
            },
            etag="mock etag",
            id="mock_syn_id",
        )

        async def mock_success_coro():
            return mock_annos_dict

        # make sure that the else statement is working
        new_tasks = set()
        with patch(
            "schematic.store.synapse.SynapseStorage.store_async_annotation",
            new_callable=AsyncMock,
            return_value=mock_stored_annos,
        ) as mock_store_async2:
            new_tasks.add(asyncio.create_task(mock_success_coro()))
            await synapse_store._process_store_annos(new_tasks)
            mock_store_async2.assert_called_once()

    async def test_process_store_annos_success_get_entity_id_variants(
        self, synapse_store: SynapseStorage
    ) -> None:
        "mock annotations obtained after gettinng annotations have different annotations and formatting"
        annotations_variants = [
            {"EntityId": ["mock_syn_id"], "Id": ["mock_string"]},
            {"entityId": ["mock_syn_id"], "id": ["mock_string"]},
            {"entityid": ["mock_syn_id"], "id": ["mock_string"]},
            {"ENTITYID": ["mock_syn_id"], "ID": ["mock_string"]},
        ]
        for anno_variant in annotations_variants:
            mock_annos_dict = {
                "annotations": {
                    "id": "mock_syn_id",
                    "etag": "mock etag",
                    "annotations": {
                        "Id": {"type": "STRING", "value": ["mock value"]},
                        "EntityId": {"type": "STRING", "value": ["mock_syn_id"]},
                        "SampleID": {"type": "STRING", "value": [""]},
                        "Component": {"type": "STRING", "value": ["mock value"]},
                    },
                },
                "FileFormat": "mock format",
                "Component": "mock component",
                **anno_variant,
            }
            mock_stored_annos = Annotations(
                annotations={
                    **anno_variant,
                    "SampleID": [""],
                    "Component": ["mock value"],
                    "FileFormat": ["mock_format"],
                },
                etag="mock etag",
                id="mock syn_id",
            )

            async def mock_success_coro() -> dict[str, Any]:
                return mock_annos_dict

            # make sure that the else statement is working
            new_tasks = set()
            with patch(
                "schematic.store.synapse.SynapseStorage.store_async_annotation",
                new_callable=AsyncMock,
                return_value=mock_stored_annos,
            ) as mock_store_async2:
                new_tasks.add(asyncio.create_task(mock_success_coro()))
                await synapse_store._process_store_annos(new_tasks)
                mock_store_async2.assert_called_once()

    async def test_process_store_annos_get_annos_empty(
        self, synapse_store: SynapseStorage
    ) -> None:
        """ "test _process_store_annos function and make sure that task of storing annotations wont be triggered when annotations are empty"""

        # make sure that the else statement is working
        # and that the task of storing annotations is not triggered when annotations are empty
        async def mock_success_coro() -> None:
            return None

        with patch(
            "schematic.store.synapse.SynapseStorage.store_async_annotation",
            new_callable=AsyncMock,
        ) as mock_store_async:
            new_tasks = set()
            new_tasks.add(asyncio.create_task(mock_success_coro()))
            await synapse_store._process_store_annos(new_tasks)
            mock_store_async.assert_not_called()


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

    def test_tidy_table_no_manifest_uploaded(self, synapse_store):
        """
        Test to ensure that the table can be tidied without issue when a DatasetFileView object is instantiated
        based on a dataset that has files annotated but no manifest uploaded.
        Covers the case where a user validates a manifest with schematic, and annotates the files with a non-schematic tool (ie the R client),
        and then tries to generate a manifest for the dataset with schematic.
        """
        # GIVEN a dataset that has files annotated (including the eTag annotation) but no manifest uplodaded
        dataset_id = "syn64019998"

        # AND the expected metadata from the files in the dataset
        expected_metadata = pd.DataFrame(
            {
                "Component": {
                    0: nan,
                    1: "BulkRNA-seqAssay",
                    2: "BulkRNA-seqAssay",
                    3: "BulkRNA-seqAssay",
                    4: "BulkRNA-seqAssay",
                },
                "FileFormat": {0: nan, 1: "BAM", 2: "BAM", 3: "BAM", 4: "BAM"},
                "GenomeBuild": {
                    0: nan,
                    1: "GRCh37",
                    2: "GRCh37",
                    3: "GRCh37",
                    4: "GRCh37",
                },
                "entityId": {
                    0: "syn64019999",
                    1: "syn64020000",
                    2: "syn64020001",
                    3: "syn64020002",
                    4: "syn64020003",
                },
            },
        ).set_index("entityId", drop=False)

        # WHEN a DatasetFileView object is instantiated based on the dataset
        dataset_fileview = DatasetFileView(dataset_id, synapse_store.syn)

        # AND the fileview is queried without being tidied
        table = dataset_fileview.query(tidy=False, force=True)

        # THEN a table should be present
        assert isinstance(table, pd.DataFrame)

        # AND the table should not be empty
        assert not table.empty

        # AND the table should already include the eTag column that will be removed and saved for comparison later
        assert "eTag" in table.columns
        original_etag_colum = table.pop("eTag")

        # AND when the table is tidied no exception should be raised
        with does_not_raise():
            table = dataset_fileview.tidy_table()

        # AND the eTag column should be different from the original eTag column
        new_etag_column = table.pop("eTag").reset_index(drop=True)
        assert (new_etag_column != original_etag_colum).all()

        # AND the expected metadata should be present in the table
        assert_frame_equal(table, expected_metadata)


@pytest.mark.table_operations
class TestTableOperations:
    @pytest.mark.slow_test
    @pytest.mark.parametrize(
        "table_column_names",
        ["display_name", "display_label", "class_label"],
        ids=["tcn_display_name", "tcn_display_label", "tcn_class_label"],
    )
    @pytest.mark.parametrize(
        "annotation_keys",
        ["display_label", "class_label"],
        ids=["aks_display_label", "aks_class_label"],
    )
    async def test_create_table(
        self,
        helpers: Helpers,
        synapse_store: SynapseStorage,
        projectId: str,
        datasetId: str,
        table_column_names: str,
        annotation_keys: str,
        dmge: DataModelGraphExplorer,
        schedule_for_cleanup: Callable[[CleanupItem], None],
    ) -> None:
        # GIVEN a table to create
        table_manipulation = None
        table_name = f"followup_synapse_storage_manifest_table_integration_test_{str(uuid.uuid4()).replace('-', '_')}"
        schedule_for_cleanup(CleanupItem(name=table_name, parent_id=projectId))

        # AND a manifest to associate metadata with files
        manifest_path = "mock_manifests/table_manifest.csv"

        # AND a copy of all the folders in the manifest. Added to the dataset directory for easy cleanup
        manifest = helpers.get_data_frame(manifest_path)

        async def copy_folder_and_update_manifest(
            row: pd.Series,
            index: int,
            datasetId: str,
            synapse_store: SynapseStorage,
            manifest: pd.DataFrame,
            schedule_for_cleanup: Callable[[CleanupItem], None],
        ) -> None:
            """Internal function to copy a folder and update the manifest."""
            folder_id = row["entityId"]
            folder_copy = await FolderModel(id=folder_id).copy_async(
                parent_id=datasetId, synapse_client=synapse_store.syn
            )
            schedule_for_cleanup(CleanupItem(synapse_id=folder_copy.id))
            manifest.at[index, "entityId"] = folder_copy.id

        tasks = []

        for index, row in manifest.iterrows():
            tasks.append(
                copy_folder_and_update_manifest(
                    row, index, datasetId, synapse_store, manifest, schedule_for_cleanup
                )
            )
        await asyncio.gather(*tasks)

        with patch.object(
            synapse_store, "_generate_table_name", return_value=(table_name, "followup")
        ), patch.object(
            synapse_store, "getDatasetProject", return_value=projectId
        ), tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            # Write the DF to a temporary file to prevent modifying the original
            manifest.to_csv(tmp_file.name, index=False)

            # WHEN I associate metadata with files
            manifest_id = synapse_store.associateMetadataWithFiles(
                dmge=dmge,
                metadataManifestPath=tmp_file.name,
                datasetId=datasetId,
                manifest_record_type="table_and_file",
                hideBlanks=True,
                restrict_manifest=False,
                table_manipulation=table_manipulation,
                table_column_names=table_column_names,
                annotation_keys=annotation_keys,
            )
            schedule_for_cleanup(CleanupItem(synapse_id=manifest_id))

        # THEN the table should exist
        existing_table_id = synapse_store.syn.findEntityId(
            name=table_name, parent=projectId
        )

        # assert table exists
        assert existing_table_id is not None

    @pytest.mark.slow_test
    @pytest.mark.parametrize(
        "table_column_names",
        ["display_label", "class_label"],
        ids=["tcn_display_label", "tcn_class_label"],
    )
    @pytest.mark.parametrize(
        "annotation_keys",
        ["display_label", "class_label"],
        ids=["aks_display_label", "aks_class_label"],
    )
    async def test_replace_table(
        self,
        helpers: Helpers,
        synapse_store: SynapseStorage,
        projectId: str,
        datasetId: str,
        table_column_names: str,
        annotation_keys: str,
        dmge: DataModelGraphExplorer,
        schedule_for_cleanup: Callable[[str], None],
    ) -> None:
        table_manipulation = "replace"

        table_name = f"followup_synapse_storage_manifest_table_integration_test_{str(uuid.uuid4()).replace('-', '_')}"
        schedule_for_cleanup(CleanupItem(name=table_name, parent_id=projectId))
        manifest_path = "mock_manifests/table_manifest.csv"
        replacement_manifest_path = "mock_manifests/table_manifest_replacement.csv"
        column_of_interest = "DaystoFollowUp"

        # AND a copy of all the folders in the manifest. Added to the dataset directory for easy cleanup
        manifest = helpers.get_data_frame(manifest_path)
        replacement_manifest = helpers.get_data_frame(replacement_manifest_path)

        async def copy_folder_and_update_manifest(
            row: pd.Series,
            index: int,
            datasetId: str,
            synapse_store: SynapseStorage,
            manifest: pd.DataFrame,
            schedule_for_cleanup: Callable[[CleanupItem], None],
        ) -> None:
            """Internal function to copy a folder and update the manifest."""
            folder_id = row["entityId"]
            folder_copy = await FolderModel(id=folder_id).copy_async(
                parent_id=datasetId, synapse_client=synapse_store.syn
            )
            schedule_for_cleanup(CleanupItem(synapse_id=folder_copy.id))
            manifest.at[index, "entityId"] = folder_copy.id
            replacement_manifest.at[index, "entityId"] = folder_copy.id

        tasks = []
        for index, row in manifest.iterrows():
            tasks.append(
                copy_folder_and_update_manifest(
                    row, index, datasetId, synapse_store, manifest, schedule_for_cleanup
                )
            )

        await asyncio.gather(*tasks)

        with patch.object(
            synapse_store, "_generate_table_name", return_value=(table_name, "followup")
        ), patch.object(
            synapse_store, "getDatasetProject", return_value=projectId
        ), tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            # Write the DF to a temporary file to prevent modifying the original
            manifest.to_csv(tmp_file.name, index=False)

            # updating file view on synapse takes a long time
            manifest_id = synapse_store.associateMetadataWithFiles(
                dmge=dmge,
                metadataManifestPath=tmp_file.name,
                datasetId=datasetId,
                manifest_record_type="table_and_file",
                hideBlanks=True,
                restrict_manifest=False,
                table_manipulation=table_manipulation,
                table_column_names=table_column_names,
                annotation_keys=annotation_keys,
            )
            schedule_for_cleanup(CleanupItem(synapse_id=manifest_id))

        # Query table for DaystoFollowUp column
        table_id = synapse_store.syn.findEntityId(name=table_name, parent=projectId)
        days_to_follow_up = (
            synapse_store.syn.tableQuery(f"SELECT {column_of_interest} FROM {table_id}")
            .asDataFrame()
            .squeeze()
        )

        # assert Days to FollowUp == 73
        assert (days_to_follow_up == 73).all()

        with patch.object(
            synapse_store, "_generate_table_name", return_value=(table_name, "followup")
        ), patch.object(
            synapse_store, "getDatasetProject", return_value=projectId
        ), tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            # Write the DF to a temporary file to prevent modifying the original
            replacement_manifest.to_csv(tmp_file.name, index=False)

            # Associate replacement manifest with files
            manifest_id = synapse_store.associateMetadataWithFiles(
                dmge=dmge,
                metadataManifestPath=tmp_file.name,
                datasetId=datasetId,
                manifest_record_type="table_and_file",
                hideBlanks=True,
                restrict_manifest=False,
                table_manipulation=table_manipulation,
                table_column_names=table_column_names,
                annotation_keys=annotation_keys,
            )
            schedule_for_cleanup(CleanupItem(synapse_id=manifest_id))

        # Query table for DaystoFollowUp column
        table_id = synapse_store.syn.findEntityId(name=table_name, parent=projectId)
        days_to_follow_up = (
            synapse_store.syn.tableQuery(f"SELECT {column_of_interest} FROM {table_id}")
            .asDataFrame()
            .squeeze()
        )

        # assert Days to FollowUp == 89 now and not 73
        assert (days_to_follow_up == 89).all()

    @pytest.mark.parametrize(
        "annotation_keys",
        ["display_label", "class_label"],
        ids=["aks_display_label", "aks_class_label"],
    )
    async def test_upsert_table(
        self,
        helpers: Helpers,
        synapse_store: SynapseStorage,
        projectId: str,
        datasetId: str,
        annotation_keys: str,
        dmge: DataModelGraphExplorer,
        schedule_for_cleanup: Callable[[str], None],
    ):
        table_manipulation = "upsert"

        table_name = f"MockRDB_synapse_storage_manifest_table_integration_test_{str(uuid.uuid4()).replace('-', '_')}".lower()
        schedule_for_cleanup(CleanupItem(name=table_name, parent_id=projectId))
        manifest_path = "mock_manifests/rdb_table_manifest.csv"
        replacement_manifest_path = "mock_manifests/rdb_table_manifest_upsert.csv"
        column_of_interest = "MockRDB_id,SourceManifest"

        with patch.object(
            synapse_store, "_generate_table_name", return_value=(table_name, "mockrdb")
        ), patch.object(
            synapse_store, "getDatasetProject", return_value=projectId
        ), tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            # Copy to a temporary file to prevent modifying the original
            shutil.copyfile(helpers.get_data_path(manifest_path), tmp_file.name)

            # updating file view on synapse takes a long time
            manifest_id = synapse_store.associateMetadataWithFiles(
                dmge=dmge,
                metadataManifestPath=tmp_file.name,
                datasetId=datasetId,
                manifest_record_type="table_and_file",
                hideBlanks=True,
                restrict_manifest=False,
                table_manipulation=table_manipulation,
                table_column_names="display_name",
                annotation_keys=annotation_keys,
            )
            schedule_for_cleanup(CleanupItem(synapse_id=manifest_id))

        # set primary key annotation for uploaded table
        table_id = synapse_store.syn.findEntityId(name=table_name, parent=projectId)

        # Query table for DaystoFollowUp column
        table_query = (
            synapse_store.syn.tableQuery(f"SELECT {column_of_interest} FROM {table_id}")
            .asDataFrame()
            .squeeze()
        )

        # assert max ID is '4' and that there are 4 entries
        assert table_query.MockRDB_id.max() == 4
        assert table_query.MockRDB_id.size == 4
        assert table_query["SourceManifest"][3] == "Manifest1"

        with patch.object(
            synapse_store, "_generate_table_name", return_value=(table_name, "mockrdb")
        ), patch.object(
            synapse_store, "getDatasetProject", return_value=projectId
        ), tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            # Copy to a temporary file to prevent modifying the original
            shutil.copyfile(
                helpers.get_data_path(replacement_manifest_path), tmp_file.name
            )

            # Associate new manifest with files
            manifest_id = synapse_store.associateMetadataWithFiles(
                dmge=dmge,
                metadataManifestPath=tmp_file.name,
                datasetId=datasetId,
                manifest_record_type="table_and_file",
                hideBlanks=True,
                restrict_manifest=False,
                table_manipulation=table_manipulation,
                table_column_names="display_name",
                annotation_keys=annotation_keys,
            )
            schedule_for_cleanup(CleanupItem(synapse_id=manifest_id))

        # Query table for DaystoFollowUp column
        table_id = synapse_store.syn.findEntityId(name=table_name, parent=projectId)
        table_query = (
            synapse_store.syn.tableQuery(f"SELECT {column_of_interest} FROM {table_id}")
            .asDataFrame()
            .squeeze()
        )

        # assert max ID is '4' and that there are 4 entries
        assert table_query.MockRDB_id.max() == 8
        assert table_query.MockRDB_id.size == 8
        assert table_query["SourceManifest"][3] == "Manifest2"


class TestDownloadManifest:
    @pytest.mark.parametrize(
        "datasetFileView",
        [
            {
                "id": ["syn51203973", "syn51203943"],
                "name": [
                    "synapse_storage_manifest.csv",
                    "synapse_storage_manifest_censored.csv",
                ],
            },
            {"id": ["syn51203973"], "name": ["synapse_storage_manifest.csv"]},
            {"id": ["syn51203943"], "name": ["synapse_storage_manifest_censored.csv"]},
        ],
    )
    def test_get_manifest_id(self, synapse_store, datasetFileView):
        # rows that contain the censored manifest
        datasetFileViewDataFrame = pd.DataFrame(datasetFileView)
        row_censored = datasetFileViewDataFrame.loc[
            datasetFileViewDataFrame["name"] == "synapse_storage_manifest_censored.csv"
        ]
        if not row_censored.empty > 0:
            censored_manifest_id = row_censored["id"].values[0]
        # rows that contain the uncensored manifest
        row_uncensored = datasetFileViewDataFrame.loc[
            datasetFileViewDataFrame["name"] == "synapse_storage_manifest.csv"
        ]
        if not row_uncensored.empty > 0:
            uncensored_manifest_id = row_uncensored["id"].values[0]

        # get id of the uncensored manifest
        manifest_syn_id = synapse_store._get_manifest_id(datasetFileViewDataFrame)

        # if there are both censored and uncensored manifests, return only id of uncensored manifest
        if not row_uncensored.empty > 0:
            assert manifest_syn_id == uncensored_manifest_id
        # if only censored manifests are present, return only id of censored manifest
        elif row_uncensored.empty and not row_censored.empty:
            assert manifest_syn_id == censored_manifest_id

    @pytest.mark.parametrize("newManifestName", ["", "Example"])
    def test_download_manifest(self, mock_manifest_download, newManifestName):
        # test the download function by downloading a manifest
        manifest_data = mock_manifest_download.download_manifest(newManifestName)
        assert os.path.exists(manifest_data["path"])

        if not newManifestName:
            assert manifest_data["name"] == "synapse_storage_manifest.csv"
        else:
            assert manifest_data["name"] == "Example.csv"

        # clean up
        os.remove(manifest_data["path"])

    def test_download_access_restricted_manifest(self, synapse_store):
        # attempt to download an uncensored manifest that has access restriction.
        # if the code works correctly, the censored manifest that does not have access restriction would get downloaded (see: syn29862066)
        md = ManifestDownload(synapse_store.syn, "syn29862066")
        manifest_data = md.download_manifest()

        assert os.path.exists(manifest_data["path"])

        # clean up
        os.remove(manifest_data["path"])

    def test_download_manifest_on_aws(self, mock_manifest_download, monkeypatch):
        # mock AWS environment by providing SECRETS_MANAGER_SECRETS environment variable and attempt to download a manifest
        monkeypatch.setenv("SECRETS_MANAGER_SECRETS", "mock_value")
        manifest_data = mock_manifest_download.download_manifest()

        assert os.path.exists(manifest_data["path"])
        # clean up
        os.remove(manifest_data["path"])

    @pytest.mark.parametrize("entity_id", ["syn27600053", "syn29862078"])
    def test_entity_type_checking(self, synapse_store, entity_id, caplog):
        md = ManifestDownload(synapse_store.syn, entity_id)
        md._entity_type_checking()
        if entity_id == "syn27600053":
            assert (
                "You are using entity type: folder. Please provide a file ID"
                in caplog.text
            )


class TestManifestUpload:
    """Test manifest upload"""

    @pytest.mark.parametrize(
        "original_manifest, files_in_dataset, expected_entity_ids, expected_filenames",
        [
            # there are new files in dataset folders after a manifest gets generated
            # but the expected behavior is to add entity ID to existing "filename" column
            (
                {
                    "Filename": {0: "Test sub folder/sample_file_one.txt"},
                    "Sample ID": {0: 1},
                    "File Format": {0: "BAM"},
                    "Component": {0: "BulkRNA-seqAssay"},
                    "Genome Build": {0: "GRCh37"},
                    "Genome FASTA": {0: ""},
                    "entityId": {0: ""},
                    "Id": {0: "mock_id_0"},
                },
                [
                    ("syn1224", "Test sub folder/sample_file_one.txt"),
                    ("syn1225", "Test sub folder/sample_file_two.txt"),
                ],
                ["syn1224"],
                ["Test sub folder/sample_file_one.txt"],
            ),
            # there's no new files in dataset folder after a manifest gets generated
            (
                {
                    "Filename": {
                        0: "Test sub folder/sample_file_one.txt",
                        1: "Test sub folder/sample_file_two.txt",
                    },
                    "Sample ID": {0: 1, 1: 2},
                    "File Format": {0: "BAM", 1: "BAM"},
                    "Component": {0: "BulkRNA-seqAssay", 1: "BulkRNA-seqAssay"},
                    "Genome Build": {0: "GRCh37", 1: "GRCh37"},
                    "Genome FASTA": {0: "", 1: ""},
                    "entityId": {0: "syn1224", 1: "syn1225"},
                    "Id": {0: "mock_id_0", 1: "mock_id_1"},
                },
                [
                    ("syn1224", "Test sub folder/sample_file_one.txt"),
                    ("syn1225", "Test sub folder/sample_file_two.txt"),
                ],
                ["syn1224", "syn1225"],
                [
                    "Test sub folder/sample_file_one.txt",
                    "Test sub folder/sample_file_two.txt",
                ],
            ),
        ],
    )
    async def test_add_annotations_to_entities_files(
        self,
        synapse_store: SynapseStorage,
        dmge: DataModelGraphExplorer,
        original_manifest: dict[str, Any],
        files_in_dataset: str,
        expected_filenames: list[str],
        expected_entity_ids: list[str],
        mock_file: File,
    ) -> None:
        """test adding annotations to entities files

        Args:
            helpers (fixture): a pytest fixture
            synapse_store (SynapseStorage): mock synapse store
            dmge (DataModelGraphExplorer): data model grpah explorer object
            original_manifest (Dictionary): the dataframe of manifest that you want to submit
            files_in_dataset (str): mock entityid and file name returned by getFilesInStorageDataset function
            expected_filenames (list(str)): expected list of file names
            expected_entity_ids (list(str)): expected list of entity ids
        """

        async def mock_format_row_annos():
            return

        async def mock_process_store_annos(requests):
            return

        with patch(
            "schematic.store.synapse.SynapseStorage.getFilesInStorageDataset",
            return_value=files_in_dataset,
        ), patch(
            "schematic.store.synapse.SynapseStorage.format_row_annotations",
            return_value=mock_format_row_annos,
            new_callable=AsyncMock,
        ) as mock_format_row, patch(
            "schematic.store.synapse.SynapseStorage._process_store_annos",
            return_value=mock_process_store_annos,
            new_callable=AsyncMock,
        ) as mock_process_store, patch.object(
            synapse_store.synapse_entity_tracker, "get", return_value=mock_file
        ):
            manifest_df = pd.DataFrame(original_manifest)

            new_df = await synapse_store.add_annotations_to_entities_files(
                dmge,
                manifest_df,
                manifest_record_type="entity",
                datasetId="mock id",
                hideBlanks=True,
            )

            file_names_lst = new_df["Filename"].tolist()
            entity_ids_lst = new_df["entityId"].tolist()

            # test entityId and Id columns get added
            assert "entityId" in new_df.columns
            assert "Id" in new_df.columns
            assert file_names_lst == expected_filenames
            assert entity_ids_lst == expected_entity_ids

            # make sure async function gets called as expected
            assert mock_format_row.call_count == len(expected_entity_ids)
            assert mock_process_store.call_count == 1

    @pytest.mark.parametrize(
        "mock_manifest_file_path",
        [
            "mock_manifests/test_mock_manifest.csv",
            "mock_manifests/test_mock_manifest_censored.csv",
        ],
    )
    def test_upload_manifest_file(
        self,
        helpers: Helpers,
        synapse_store: SynapseStorage,
        mock_manifest_file_path: str,
        mock_file: File,
    ) -> None:
        """test upload manifest file function

        Args:
            helpers (fixture): a pytest fixture
            synapse_store (SynapseStorage): mock synapse store
            dmge (DataModelGraphExplorer): data model grpah explorer object
        """
        test_df = pd.DataFrame(
            {
                "Filename": {
                    0: "Test sub folder/sample_file_one.txt",
                    1: "Test sub folder/sample_file_three.txt",
                    2: "Test sub folder/sample_file_two.txt",
                },
                "Sample ID": {0: 1, 1: 2, 2: 3},
                "File Format": {0: "BAM", 1: "BAM", 2: "BAM"},
                "Component": {
                    0: "BulkRNA-seqAssay",
                    1: "BulkRNA-seqAssay",
                    2: "BulkRNA-seqAssay",
                },
                "Genome Build": {0: "GRCh37", 1: "GRCh37", 2: "GRCh37"},
                "Genome FASTA": {0: "", 1: "", 2: ""},
                "Id": {0: "mock1", 1: "mock2", 2: "mock3"},
                "entityId": {0: "syn1224", 1: "syn1225", 2: "syn1226"},
            }
        )
        with patch("synapseclient.Synapse.store") as syn_store_mock, patch.object(
            synapse_store.synapse_entity_tracker, "get", return_value=mock_file
        ):
            syn_store_mock.return_value.id = "mock manifest id"
            mock_component_name = "BulkRNA-seqAssay"
            mock_file_path = helpers.get_data_path(mock_manifest_file_path)
            mock_manifest_synapse_file_id = synapse_store.upload_manifest_file(
                manifest=test_df,
                metadataManifestPath=mock_file_path,
                datasetId="mock dataset id",
                restrict_manifest=True,
                component_name=mock_component_name,
            )

            assert mock_manifest_synapse_file_id == "mock manifest id"

    @pytest.mark.parametrize("file_annotations_upload", [True, False])
    @pytest.mark.parametrize("hide_blanks", [True, False])
    @pytest.mark.parametrize("restrict", [True, False])
    @pytest.mark.parametrize("manifest_record_type", ["entity", "table", "both"])
    def test_upload_manifest_as_csv(
        self,
        helpers: Helpers,
        dmge: DataModelGraphExplorer,
        synapse_store: SynapseStorage,
        file_annotations_upload: bool,
        manifest_record_type: str,
        hide_blanks: bool,
        restrict: bool,
        mock_file: File,
    ) -> None:
        async def mock_add_annotations_to_entities_files():
            return

        with (
            patch(
                "schematic.store.synapse.SynapseStorage.add_annotations_to_entities_files",
                return_value=mock_add_annotations_to_entities_files,
                new_callable=AsyncMock,
            ) as add_anno_mock,
            patch(
                "schematic.store.synapse.SynapseStorage.upload_manifest_file",
                return_value="mock manifest id",
            ) as upload_manifest_mock,
            patch(
                "schematic.store.synapse.SynapseStorage.format_manifest_annotations"
            ) as format_manifest_anno_mock,
            patch.object(synapse_store.syn, "set_annotations"),
            patch.object(
                synapse_store.synapse_entity_tracker, "get", return_value=mock_file
            ),
        ):
            manifest_path = helpers.get_data_path("mock_manifests/test_BulkRNAseq.csv")
            manifest_df = helpers.get_data_frame(manifest_path)
            synapse_store.upload_manifest_as_csv(
                dmge,
                manifest=manifest_df,
                metadataManifestPath=manifest_path,
                datasetId="mock synapse id",
                restrict=restrict,
                manifest_record_type=manifest_record_type,
                file_annotations_upload=file_annotations_upload,
                hideBlanks=hide_blanks,
                component_name="BulkRNA-seqAssay",
                annotation_keys="class_label",
            )
            if file_annotations_upload:
                add_anno_mock.assert_called_once()
            else:
                add_anno_mock.assert_not_called()

            upload_manifest_mock.assert_called_once()
            format_manifest_anno_mock.assert_called_once()

    @pytest.mark.parametrize("file_annotations_upload", [True, False])
    @pytest.mark.parametrize("hide_blanks", [True, False])
    @pytest.mark.parametrize("restrict", [True, False])
    @pytest.mark.parametrize("manifest_record_type", ["entity", "table", "both"])
    def test_upload_manifest_as_table(
        self,
        helpers: Helpers,
        synapse_store: SynapseStorage,
        dmge: DataModelGraphExplorer,
        file_annotations_upload: bool,
        hide_blanks: bool,
        restrict: bool,
        manifest_record_type: str,
        mock_file: File,
    ) -> None:
        mock_df = pd.DataFrame()

        async def mock_add_annotations_to_entities_files():
            return

        with (
            patch(
                "schematic.store.synapse.SynapseStorage.uploadDB",
                return_value=["mock_table_id", mock_df, "mock_table_manifest"],
            ) as update_db_mock,
            patch(
                "schematic.store.synapse.SynapseStorage.add_annotations_to_entities_files",
                return_value=mock_add_annotations_to_entities_files,
                new_callable=AsyncMock,
            ) as add_anno_mock,
            patch(
                "schematic.store.synapse.SynapseStorage.upload_manifest_file",
                return_value="mock manifest id",
            ),
            patch.object(synapse_store.syn, "set_annotations") as set_anno_mock,
            patch(
                "schematic.store.synapse.SynapseStorage.format_manifest_annotations"
            ) as format_manifest_anno_mock,
            patch.object(
                synapse_store.synapse_entity_tracker, "get", return_value=mock_file
            ),
        ):
            manifest_path = helpers.get_data_path("mock_manifests/test_BulkRNAseq.csv")
            manifest_df = helpers.get_data_frame(manifest_path)
            synapse_store.upload_manifest_as_table(
                dmge,
                manifest=manifest_df,
                metadataManifestPath=manifest_path,
                datasetId="mock synapse id",
                table_name="new table name",
                component_name="BulkRNA-seqAssay",
                restrict=restrict,
                manifest_record_type=manifest_record_type,
                hideBlanks=hide_blanks,
                table_manipulation="replace",
                table_column_names="class_label",
                annotation_keys="class_label",
                file_annotations_upload=file_annotations_upload,
            )
            if file_annotations_upload:
                add_anno_mock.assert_called_once()
            else:
                add_anno_mock.assert_not_called()
            # need to set annotations for both table and files
            assert format_manifest_anno_mock.call_count == 2
            assert set_anno_mock.call_count == 2
            assert update_db_mock.call_count == 2

    @pytest.mark.parametrize("file_annotations_upload", [True, False])
    @pytest.mark.parametrize("hide_blanks", [True, False])
    @pytest.mark.parametrize("restrict", [True, False])
    @pytest.mark.parametrize("manifest_record_type", ["entity", "table", "both"])
    def test_upload_manifest_combo(
        self,
        helpers: Helpers,
        synapse_store: SynapseStorage,
        dmge: DataModelGraphExplorer,
        file_annotations_upload: bool,
        hide_blanks: bool,
        restrict: bool,
        manifest_record_type: str,
        mock_file: File,
    ) -> None:
        mock_df = pd.DataFrame()
        manifest_path = helpers.get_data_path("mock_manifests/test_BulkRNAseq.csv")
        manifest_df = helpers.get_data_frame(manifest_path)

        async def mock_add_annotations_to_entities_files():
            return

        with (
            patch(
                "schematic.store.synapse.SynapseStorage.uploadDB",
                return_value=["mock_table_id", mock_df, "mock_table_manifest"],
            ) as update_db_mock,
            patch(
                "schematic.store.synapse.SynapseStorage.add_annotations_to_entities_files",
                return_value=mock_add_annotations_to_entities_files,
                new_callable=AsyncMock,
            ) as add_anno_mock,
            patch(
                "schematic.store.synapse.SynapseStorage.upload_manifest_file",
                return_value="mock manifest id",
            ),
            patch.object(synapse_store.syn, "set_annotations") as set_anno_mock,
            patch(
                "schematic.store.synapse.SynapseStorage.format_manifest_annotations"
            ) as format_manifest_anno_mock,
            patch.object(
                synapse_store.synapse_entity_tracker, "get", return_value=mock_file
            ),
        ):
            synapse_store.upload_manifest_combo(
                dmge,
                manifest=manifest_df,
                metadataManifestPath=manifest_path,
                datasetId="mock synapse id",
                table_name="new table name",
                component_name="BulkRNA-seqAssay",
                restrict=restrict,
                manifest_record_type=manifest_record_type,
                hideBlanks=hide_blanks,
                table_manipulation="replace",
                table_column_names="class_label",
                annotation_keys="class_label",
                file_annotations_upload=file_annotations_upload,
            )

            if file_annotations_upload:
                add_anno_mock.assert_called_once()
            else:
                add_anno_mock.assert_not_called()
            # need to set annotations for both table and files
            assert format_manifest_anno_mock.call_count == 2
            assert set_anno_mock.call_count == 2
            assert update_db_mock.call_count == 2

    @pytest.mark.parametrize(
        "manifest_record_type,expected",
        [
            ("file_only", "mock_id_csv"),
            ("table_and_file", "mock_id_table"),
            ("file_and_entities", "mock_id_csv"),
            ("table_file_and_entities", "mock_id_entities"),
        ],
    )
    @pytest.mark.parametrize("restrict_rules", [True, False])
    @pytest.mark.parametrize("hide_blanks", [True, False])
    @pytest.mark.parametrize("file_annotations_upload", [True, False])
    def test_associate_metadata_with_files(
        self,
        helpers: Helpers,
        restrict_rules: bool,
        hide_blanks: bool,
        synapse_store: SynapseStorage,
        manifest_record_type: str,
        expected: str,
        file_annotations_upload: bool,
        dmge: DataModelGraphExplorer,
        mock_file: File,
    ) -> None:
        with (
            patch(
                "schematic.store.synapse.SynapseStorage.upload_manifest_as_csv",
                return_value="mock_id_csv",
            ),
            patch(
                "schematic.store.synapse.SynapseStorage.upload_manifest_as_table",
                return_value="mock_id_table",
            ),
            patch(
                "schematic.store.synapse.SynapseStorage.upload_manifest_combo",
                return_value="mock_id_entities",
            ),
            patch.object(
                synapse_store.synapse_entity_tracker, "get", return_value=mock_file
            ),
        ):
            manifest_path = "mock_manifests/test_BulkRNAseq.csv"
            manifest_id = synapse_store.associateMetadataWithFiles(
                dmge=dmge,
                metadataManifestPath=helpers.get_data_path(manifest_path),
                datasetId="mock_dataset_id",
                hideBlanks=hide_blanks,
                restrict_manifest=restrict_rules,
                manifest_record_type=manifest_record_type,
                file_annotations_upload=file_annotations_upload,
            )
        assert manifest_id == expected
