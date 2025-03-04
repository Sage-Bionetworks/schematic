"""
This script contains a test suite for verifying the validation, submission and annotation of
file-based manifests using the `TestMetadataModel` class to communicate with Synapse
and verify the expected behavior of uploading annotation manifest CSVs using the
metadata model.

It utilizes the `pytest` framework along with `pytest-mock` to mock and spy on methods
of the `SynapseStorage` class, which is responsible for handling file uploads and
annotations in Synapse.
"""
import asyncio
import logging
import tempfile
import uuid
from contextlib import nullcontext as does_not_raise
from typing import Any, Callable, Optional

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from synapseclient import Annotations
from synapseclient.core import utils
from synapseclient.models import File, Folder

from schematic.store.synapse import SynapseStorage
from schematic.utils.df_utils import STR_NA_VALUES_FILTERED
from schematic.utils.general import create_temp_folder
from tests.conftest import Helpers, metadata_model
from tests.utils import CleanupItem

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DESCRIPTION = "This is an example file."
CONTENT_TYPE = "text/plain"
VERSION_COMMENT = "My version comment"


def file_instance() -> File:
    """Creates a file instance with random content, used for manifests to be able to
    point to real Synapse entities during each test run. The parent folder these are
    created in is cleaned up post test run."""
    filename = utils.make_bogus_uuid_file()
    return File(
        path=filename,
        description=DESCRIPTION,
        content_type=CONTENT_TYPE,
        version_comment=VERSION_COMMENT,
        version_label=str(uuid.uuid4()),
    )


class TestMetadataModel:
    """Test suite for verifying the submission and annotation of file-based manifests."""

    def validate_manifest_annotations(
        self,
        manifest_annotations: Annotations,
        manifest_entity_type: str,
        expected_entity_id: str,
        manifest_file_contents: pd.DataFrame = None,
    ) -> None:
        """
        Validates that the annotations on a manifest entity (file or table) were correctly updated
        by comparing the annotations on the manifest entity with the contents of the manifest file itself,
        and ensuring the eTag annotation is not empty.

        This method is wrapped by ``_submit_and_verify_manifest()``

        Arguments:
            manifest_annotations (pd.DataFrame): manifest annotations
            manifest_entity_type (str): type of manifest (file or table)
            expected_entity_id (str): expected entity ID of the manifest
            manifest_file_contents (pd.DataFrame): manifest file contents

        Returns:
            None
        """
        # Check that the eTag annotation is not empty
        assert len(manifest_annotations["eTag"][0]) > 0

        # Check that entityId is expected
        assert manifest_annotations["entityId"][0] == expected_entity_id

        # For manifest files only: Check that all other annotations from the manifest match the annotations in the manifest file itself
        if manifest_entity_type.lower() != "file":
            return
        for annotation in manifest_annotations.keys():
            if annotation in ["eTag", "entityId"]:
                continue
            else:
                assert (
                    manifest_annotations[annotation][0]
                    == manifest_file_contents[annotation].unique()
                )

    @pytest.mark.single_process_execution
    async def test_submit_filebased_manifest_file_and_entities_valid_manifest_submitted(
        self,
        helpers: Helpers,
        mocker: MockerFixture,
        synapse_store: SynapseStorage,
        schedule_for_cleanup: Callable[[CleanupItem], None],
    ):
        # GIVEN a project that exists in Synapse
        project_id = "syn23643250"

        # AND a dataset/files that exist in Synapse
        dataset_folder = await Folder(
            name=f"test_submit_filebased_manifest_file_and_entities_valid_manifest_submitted_{uuid.uuid4()}",
            files=[file_instance(), file_instance()],
            parent_id=project_id,
        ).store_async(synapse_client=synapse_store.syn)
        schedule_for_cleanup(CleanupItem(synapse_id=dataset_folder.id))
        # Wait for the fileview to be updated
        await asyncio.sleep(10)

        # AND a CSV file on disk
        filenames = [
            f"schematic - main/{dataset_folder.name}/{file.name}"
            for file in dataset_folder.files
        ]
        entity_ids = [file.id for file in dataset_folder.files]
        random_uuids = [str(uuid.uuid4()) for _ in range(len(filenames))]
        data = {
            "Filename": filenames,
            "Sample ID": random_uuids,
            "File Format": ["" for _ in range(len(filenames))],
            "Component": ["BulkRNA-seqAssay" for _ in range(len(filenames))],
            "Genome Build": ["" for _ in range(len(filenames))],
            "Genome FASTA": ["" for _ in range(len(filenames))],
            "Id": random_uuids,
            "entityId": entity_ids,
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            df.to_csv(tmp_file.name, index=False)
            # WHEN the manifest is submitted (Assertions are handled in the helper method)
            self._submit_and_verify_manifest(
                helpers=helpers,
                mocker=mocker,
                synapse_store=synapse_store,
                manifest_path=tmp_file.name,
                dataset_id=dataset_folder.id,
                manifest_record_type="file_and_entities",
                validate_component=None,
                dataset_scope=None,
                expected_table_id=None,
                expected_table_name="bulkrna-seqassay_synapse_storage_manifest_table",
                project_id=project_id,
                expected_manifest_id=None,
                expected_manifest_name="synapse_storage_manifest_bulkrna-seqassay.csv",
            )

        # AND when the annotatsions are updated and the manifest is resubmitted
        with tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            random_uuids = [str(uuid.uuid4()) for _ in range(len(filenames))]
            df["Sample ID"] = random_uuids
            df["Id"] = random_uuids
            df.to_csv(tmp_file.name, index=False)

            # THEN the annotations are updated
            self._submit_and_verify_manifest(
                helpers=helpers,
                mocker=mocker,
                synapse_store=synapse_store,
                manifest_path=tmp_file.name,
                dataset_id=dataset_folder.id,
                manifest_record_type="file_and_entities",
                validate_component=None,
                dataset_scope=None,
                expected_table_id=None,
                expected_table_name="bulkrna-seqassay_synapse_storage_manifest_table",
                project_id=project_id,
                expected_manifest_id=None,
                expected_manifest_name="synapse_storage_manifest_bulkrna-seqassay.csv",
                already_spied=True,
            )

    @pytest.mark.single_process_execution
    async def test_submit_filebased_manifest_file_and_entities_mock_filename(
        self,
        helpers: Helpers,
        mocker: MockerFixture,
        synapse_store: SynapseStorage,
        schedule_for_cleanup: Callable[[CleanupItem], None],
    ):
        # GIVEN a project that exists in Synapse
        project_id = "syn23643250"

        # AND a dataset/files that exist in Synapse
        dataset_folder = await Folder(
            name=f"test_submit_filebased_manifest_file_and_entities_mock_filename_{uuid.uuid4()}",
            files=[file_instance(), file_instance()],
            parent_id=project_id,
        ).store_async(synapse_client=synapse_store.syn)
        schedule_for_cleanup(CleanupItem(synapse_id=dataset_folder.id))
        # Wait for the fileview to be updated
        await asyncio.sleep(10)

        # AND a CSV file on disk
        filenames = [
            f"schematic - main/{dataset_folder.name}/{file.name}"
            for file in dataset_folder.files
        ]
        entity_ids = [file.id for file in dataset_folder.files]
        random_uuids = [str(uuid.uuid4()) for _ in range(len(filenames))]
        data = {
            "Filename": filenames,
            "Sample ID": random_uuids,
            "Id": random_uuids,
            "Component": ["MockFilename" for _ in range(len(filenames))],
            "entityId": entity_ids,
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            df.to_csv(tmp_file.name, index=False)
            # WHEN the manifest is submitted (Assertions are handled in the helper method)
            self._submit_and_verify_manifest(
                helpers=helpers,
                mocker=mocker,
                synapse_store=synapse_store,
                manifest_path=tmp_file.name,
                dataset_id=dataset_folder.id,
                manifest_record_type="file_and_entities",
                validate_component="MockFilename",
                dataset_scope=dataset_folder.id,
                expected_table_id=None,
                expected_table_name="mockfilename_synapse_storage_manifest_table",
                project_id=project_id,
                expected_manifest_id=None,
                expected_manifest_name="synapse_storage_manifest_mockfilename.csv",
            )

    @pytest.mark.single_process_execution
    async def test_submit_filebased_manifest_table_and_file_valid_manifest_submitted(
        self,
        helpers: Helpers,
        mocker: MockerFixture,
        synapse_store: SynapseStorage,
        schedule_for_cleanup: Callable[[CleanupItem], None],
    ) -> None:
        # GIVEN a project that exists in Synapse
        project_id = "syn23643250"

        # AND a dataset/files that exist in Synapse
        dataset_folder = await Folder(
            name=f"test_submit_filebased_manifest_table_and_file_valid_manifest_submitted_{uuid.uuid4()}",
            files=[file_instance(), file_instance()],
            parent_id=project_id,
        ).store_async(synapse_client=synapse_store.syn)
        schedule_for_cleanup(CleanupItem(synapse_id=dataset_folder.id))
        # Wait for the fileview to be updated
        await asyncio.sleep(10)

        # AND a CSV file on disk
        filenames = [
            f"schematic - main/{dataset_folder.name}/{file.name}"
            for file in dataset_folder.files
        ]
        entity_ids = [file.id for file in dataset_folder.files]
        random_uuids = [str(uuid.uuid4()) for _ in range(len(filenames))]
        data = {
            "Filename": filenames,
            "Sample ID": random_uuids,
            "File Format": ["" for _ in range(len(filenames))],
            "Component": ["BulkRNA-seqAssay" for _ in range(len(filenames))],
            "Genome Build": ["" for _ in range(len(filenames))],
            "Genome FASTA": ["" for _ in range(len(filenames))],
            "Id": random_uuids,
            "entityId": entity_ids,
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            df.to_csv(tmp_file.name, index=False)

            # WHEN the manifest is submitted (Assertions are handled in the helper method)
            self._submit_and_verify_manifest(
                helpers=helpers,
                mocker=mocker,
                synapse_store=synapse_store,
                manifest_path=tmp_file.name,
                dataset_id=dataset_folder.id,
                manifest_record_type="table_and_file",
                validate_component=None,
                dataset_scope=None,
                # Find by name instead of ID
                expected_table_id=None,
                expected_table_name="bulkrna-seqassay_synapse_storage_manifest_table",
                project_id=project_id,
                # Find by name instead of ID
                expected_manifest_id=None,
                expected_manifest_name="synapse_storage_manifest_bulkrna-seqassay.csv",
            )

        # AND when the annotations are updated and the manifest is resubmitted
        with tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            random_uuids = [str(uuid.uuid4()) for _ in range(len(filenames))]
            df["Sample ID"] = random_uuids
            df["Id"] = random_uuids
            df.to_csv(tmp_file.name, index=False)

            # THEN the annotations are updated
            self._submit_and_verify_manifest(
                helpers=helpers,
                mocker=mocker,
                synapse_store=synapse_store,
                manifest_path=tmp_file.name,
                dataset_id=dataset_folder.id,
                manifest_record_type="table_and_file",
                validate_component=None,
                dataset_scope=None,
                # Find by name instead of ID
                expected_table_id=None,
                expected_table_name="bulkrna-seqassay_synapse_storage_manifest_table",
                project_id=project_id,
                # Find by name instead of ID
                expected_manifest_id=None,
                expected_manifest_name="synapse_storage_manifest_bulkrna-seqassay.csv",
                already_spied=True,
            )

    @pytest.mark.single_process_execution
    async def test_submit_filebased_manifest_table_and_file_mock_filename(
        self,
        helpers: Helpers,
        mocker: MockerFixture,
        synapse_store: SynapseStorage,
        schedule_for_cleanup: Callable[[CleanupItem], None],
    ) -> None:
        # GIVEN a project that exists in Synapse
        project_id = "syn23643250"

        # AND a dataset/files that exist in Synapse
        dataset_folder = await Folder(
            name=f"test_submit_filebased_manifest_table_and_file_mock_filename_{uuid.uuid4()}",
            files=[file_instance(), file_instance()],
            parent_id=project_id,
        ).store_async(synapse_client=synapse_store.syn)
        schedule_for_cleanup(CleanupItem(synapse_id=dataset_folder.id))
        # Wait for the fileview to be updated
        await asyncio.sleep(10)

        # AND a CSV file on disk
        filenames = [
            f"schematic - main/{dataset_folder.name}/{file.name}"
            for file in dataset_folder.files
        ]
        entity_ids = [file.id for file in dataset_folder.files]
        random_uuids = [str(uuid.uuid4()) for _ in range(len(filenames))]
        data = {
            "Filename": filenames,
            "Sample ID": random_uuids,
            "Id": random_uuids,
            "Component": ["MockFilename" for _ in range(len(filenames))],
            "entityId": entity_ids,
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(
            delete=True,
            suffix=".csv",
            dir=create_temp_folder(path=tempfile.gettempdir()),
        ) as tmp_file:
            df.to_csv(tmp_file.name, index=False)

            # WHEN the manifest is submitted (Assertions are handled in the helper method)
            self._submit_and_verify_manifest(
                helpers=helpers,
                mocker=mocker,
                synapse_store=synapse_store,
                manifest_path=tmp_file.name,
                dataset_id=dataset_folder.id,
                manifest_record_type="table_and_file",
                validate_component="MockFilename",
                dataset_scope=dataset_folder.id,
                # Find by name instead of ID
                expected_table_id=None,
                expected_table_name="mockfilename_synapse_storage_manifest_table",
                project_id=project_id,
                # Find by name instead of ID
                expected_manifest_id=None,
                expected_manifest_name="synapse_storage_manifest_mockfilename.csv",
            )

    def _submit_and_verify_manifest(
        self,
        helpers,
        mocker,
        synapse_store: SynapseStorage,
        manifest_path: str,
        dataset_id: str,
        manifest_record_type: str,
        project_id: Optional[str] = None,
        expected_table_id: Optional[str] = None,
        expected_table_name: Optional[str] = None,
        expected_manifest_id: Optional[str] = None,
        expected_manifest_name: Optional[str] = None,
        validate_component: Optional[str] = None,
        dataset_scope: Optional[str] = None,
        already_spied: bool = False,
    ) -> None:
        """Handles submission and verification of file-based manifests.

        Args:
            helpers: Test helper functions
            mocker: Pytest mocker fixture
            synapse_store: Synapse storage object
            manifest_path: Path to the manifest file
            dataset_id: Synapse ID of the dataset
            manifest_record_type: Type of manifest record
            project_id: Synapse ID of the project (Required if using `expected_table_name`)
            expected_table_id: Synapse ID of the expected table (Alternative to `expected_table_name`)
            expected_table_name: Name of the expected table (Alternative to `expected_table_id`)
            expected_manifest_id: Synapse ID of the expected manifest (Alternative to `expected_manifest_name`)
            expected_manifest_name: Name of the expected manifest (Alternative to `expected_manifest_id`)
            validate_component: Component to validate
            dataset_scope: Dataset scope
            already_spied: Whether the methods have already been spied
        """
        if not (expected_table_id or (expected_table_name and project_id)):
            raise ValueError(
                "expected_table_id or (expected_table_name + project_id) must be provided"
            )
        if not (expected_manifest_id or expected_manifest_name):
            raise ValueError(
                "expected_manifest_id or expected_manifest_name must be provided"
            )
        # HACK: must requery the fileview to get new files, since SynapseStorage will query the last state
        # of the fileview which may not contain any new folders in the fileview.
        # This is a workaround to fileviews not always containing the latest information
        # Since the tests don't always follow a similar process as testing resources are created and destroyed
        synapse_store.query_fileview(force_requery=True)

        # Spies
        if already_spied:
            spy_upload_file_as_csv = SynapseStorage.upload_manifest_as_csv
            spy_upload_file_as_table = SynapseStorage.upload_manifest_as_table
            spy_upload_file_combo = SynapseStorage.upload_manifest_combo
            spy_add_annotations = SynapseStorage.add_annotations_to_entities_files
        else:
            spy_upload_file_as_csv = mocker.spy(
                SynapseStorage, "upload_manifest_as_csv"
            )
            spy_upload_file_as_table = mocker.spy(
                SynapseStorage, "upload_manifest_as_table"
            )
            spy_upload_file_combo = mocker.spy(SynapseStorage, "upload_manifest_combo")
            spy_add_annotations = mocker.spy(
                SynapseStorage, "add_annotations_to_entities_files"
            )

        # GIVEN a metadata model object using class labels
        meta_data_model = metadata_model(helpers, "class_label")

        # AND a filebased test manifest
        load_args = {"dtype": "string"}
        manifest = helpers.get_data_frame(
            manifest_path, preserve_raw_input=True, **load_args
        )
        manifest_full_path = helpers.get_data_path(manifest_path)

        # WHEN the manifest is submitted and files are annotated
        # THEN submission should complete without error

        with does_not_raise():
            manifest_id = meta_data_model.submit_metadata_manifest(
                manifest_path=manifest_full_path,
                dataset_id=dataset_id,
                manifest_record_type=manifest_record_type,
                restrict_rules=False,
                file_annotations_upload=True,
                hide_blanks=False,
                validate_component=validate_component,
                dataset_scope=dataset_scope,
            )

        # AND the files should be annotated
        if already_spied:
            spy_add_annotations.call_count == 2
        else:
            spy_add_annotations.call_count == 1

        # AND the annotations on the entities should have the correct metadata
        for _, row in manifest.iterrows():
            entityId = row["entityId"]
            expected_sample_id = row["Sample ID"]
            annos = synapse_store.syn.get_annotations(entityId)
            sample_id = annos["SampleID"][0]
            assert str(sample_id) == str(expected_sample_id)

        # AND the annotations on the manifest file itself are correct
        expected_manifest_id = expected_manifest_id or synapse_store.syn.findEntityId(
            name=expected_manifest_name,
            parent=dataset_id,
        )
        manifest_file_annotations = synapse_store.syn.get_annotations(
            expected_manifest_id
        )
        self.validate_manifest_annotations(
            manifest_annotations=manifest_file_annotations,
            manifest_entity_type="file",
            expected_entity_id=expected_manifest_id,
            manifest_file_contents=manifest,
        )

        if manifest_record_type == "table_and_file":
            with tempfile.TemporaryDirectory() as download_dir:
                expected_table_id = expected_table_id or synapse_store.syn.findEntityId(
                    name=expected_table_name,
                    parent=project_id,
                )
                manifest_table = synapse_store.syn.tableQuery(
                    f"select * from {expected_table_id}", downloadLocation=download_dir
                ).asDataFrame(na_values=STR_NA_VALUES_FILTERED, keep_default_na=False)

                # AND the columns in the manifest table should reflect the ones in the file
                table_columns = manifest_table.columns
                manifest_columns = [col.replace(" ", "") for col in manifest.columns]
                assert set(table_columns) == set(manifest_columns)

                # AND the annotations on the manifest table itself are correct
                manifest_table_annotations = synapse_store.syn.get_annotations(
                    expected_table_id
                )
                self.validate_manifest_annotations(
                    manifest_annotations=manifest_table_annotations,
                    manifest_entity_type="table",
                    expected_entity_id=expected_table_id,
                )

        # AND the manifest should be submitted to the correct place
        assert manifest_id == expected_manifest_id

        # AND the correct upload methods were called for the given record type
        if manifest_record_type == "file_and_entities":
            if already_spied:
                spy_upload_file_as_csv.call_count == 2
            else:
                spy_upload_file_as_csv.call_count == 1
            spy_upload_file_as_table.assert_not_called()
            spy_upload_file_combo.assert_not_called()
        elif manifest_record_type == "table_and_file":
            if already_spied:
                spy_upload_file_as_table.call_count == 2
            else:
                spy_upload_file_as_table.call_count == 1
            spy_upload_file_as_csv.assert_not_called()
            spy_upload_file_combo.assert_not_called()

    def test_validate_model_manifest_valid_with_none_string(
        self, helpers: Helpers
    ) -> None:
        """
        Tests for validateModelManifest when the manifest is valid with 'None' values

        Args:
            helpers: Test helper functions
        """
        meta_data_model = metadata_model(helpers, "class_label")

        errors, warnings = meta_data_model.validateModelManifest(
            manifestPath="tests/data/mock_manifests/Valid_none_value_test_manifest.csv",
            rootNode="Biospecimen",
        )
        assert not warnings
        assert not errors

    def test_validate_model_manifest_invalid(self, helpers: Helpers) -> None:
        """
        Tests for validateModelManifest when the manifest requires values to be from a set of values containing 'None'

        Args:
            helpers: Test helper functions
        """
        meta_data_model = metadata_model(helpers, "class_label")

        errors, warnings = meta_data_model.validateModelManifest(
            manifestPath="tests/data/mock_manifests/Invalid_none_value_test_manifest.csv",
            rootNode="Biospecimen",
        )
        assert not warnings
        assert errors[0][0] == "6"
        assert errors[0][1] == "Tissue Status"
        assert errors[0][3] == "InvalidValue"
        # The order of the valid values in the error message are random, so the test must be
        # slightly complicated:
        # 'InvalidValue' is not one of ['Malignant', 'Healthy', 'None']
        # 'InvalidValue' is not one of ['Healthy', 'Malignant', 'None']
        error_message = errors[0][2]
        assert isinstance(error_message, str)
        assert error_message.startswith("'InvalidValue' is not one of")
