"""
This script contains a test suite for verifying the submission and annotation of file-based manifests using the `TestMetadataModel` class
to communicate with Synapse and verify the expected behavior of uploading annotation manifest CSVs using the metadata model.

It utilizes the `pytest` framework along with `pytest-mock` to mock and spy on methods of the `SynapseStorage` class,
which is responsible for handling file uploads and annotations in Synapse.
"""

import logging
import os
import pytest
import shutil

from contextlib import nullcontext as does_not_raise

import pytest
from pytest_mock import MockerFixture

from schematic.store.synapse import SynapseStorage
from tests.conftest import metadata_model

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestMetadataModel:

    # Define the test cases as a class attribute
    test_cases = [
        # Test 1: Check that a valid manifest can be submitted, and corresponding entities annotated from it
        (
            "mock_manifests/filepath_submission_test_manifest.csv",
            "syn62276880",
            None,
            "syn62280543",
            "syn53011753",
            None,
        ),
        # Test 2: Change the Sample ID annotation from the previous test to ensure the manifest file is getting updated
        (
            "mock_manifests/filepath_submission_test_manifest_sampleidx10.csv",
            "syn62276880",
            None,
            "syn62280543",
            "syn53011753",
            None,
        ),
        # Test 3: Test manifest file upload with validation based on the MockFilename component and given dataset_scope
        (
            "mock_manifests/ValidFilenameManifest.csv",
            "syn62822337",
            "MockFilename",
            "syn62822975",
            "syn63192751",
            "syn62822337",
        ),
    ]

    @pytest.mark.parametrize(
        "manifest_path, dataset_id, validate_component, expected_manifest_id, "
        "expected_table_id, dataset_scope",
        test_cases,
    )
    def test_submit_filebased_manifest_file_and_entities(
        self,
        helpers,
        manifest_path,
        dataset_id,
        validate_component,
        expected_manifest_id,
        expected_table_id,
        dataset_scope,
        mocker: MockerFixture,
        synapse_store,
    ):
        self._submit_and_verify_manifest(
            helpers=helpers,
            mocker=mocker,
            synapse_store=synapse_store,
            manifest_path=manifest_path,
            dataset_id=dataset_id,
            expected_manifest_id=expected_manifest_id,
            expected_table_id=expected_table_id,
            manifest_record_type="file_and_entities",
            validate_component=validate_component,
            dataset_scope=dataset_scope,
        )

    @pytest.mark.parametrize(
        "manifest_path, dataset_id, validate_component, expected_manifest_id, "
        "expected_table_id, dataset_scope",
        test_cases,
    )
    def test_submit_filebased_manifest_table_and_file(
        self,
        helpers,
        manifest_path,
        dataset_id,
        validate_component,
        expected_manifest_id,
        expected_table_id,
        dataset_scope,
        mocker: MockerFixture,
        synapse_store,
    ):
        self._submit_and_verify_manifest(
            helpers=helpers,
            mocker=mocker,
            synapse_store=synapse_store,
            manifest_path=manifest_path,
            dataset_id=dataset_id,
            expected_manifest_id=expected_manifest_id,
            expected_table_id=expected_table_id,
            manifest_record_type="table_and_file",
            validate_component=validate_component,
            dataset_scope=dataset_scope,
        )

    def _submit_and_verify_manifest(
        self,
        helpers,
        mocker,
        synapse_store,
        manifest_path,
        dataset_id,
        expected_manifest_id,
        expected_table_id,
        manifest_record_type,
        validate_component=None,
        dataset_scope=None,
        download_dir=os.path.join(os.getcwd(), "temp"),
    ):
        # spys
        spy_upload_file_as_csv = mocker.spy(SynapseStorage, "upload_manifest_as_csv")
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
        load_args = {
            "dtype": "string",
        }
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
        spy_add_annotations.assert_called_once()

        # AND the annotations on the entities should have the correct metadata
        for _, row in manifest.iterrows():
            entityId = row["entityId"]
            expected_sample_id = row["Sample ID"]
            annos = synapse_store.syn.get_annotations(entityId)
            sample_id = annos["SampleID"][0]
            assert str(sample_id) == str(expected_sample_id)

        # AND the annotations on the manifest file itself should have the correct metadata
        manifest_annos = synapse_store.syn.get_annotations(manifest_id)
        for annotation in manifest_annos.keys():
            if annotation == "eTag":
                continue
            if annotation == "entityId":
                assert manifest_annos[annotation][0] == expected_manifest_id
            else:
                assert manifest_annos[annotation][0] == manifest[annotation].unique()

        # AND the annotations in the manifest table should reflect the ones in the file, if applicable
        if manifest_record_type == "table_and_file":
            table_annotations = synapse_store.syn.tableQuery(
                f"select * from {expected_table_id}", downloadLocation=download_dir
            ).asDataFrame()

            table_columns = table_annotations.columns
            manifest_columns = [col.replace(" ", "") for col in manifest.columns]
            assert set(table_columns) == set(manifest_columns)

            # Cleanup
            shutil.rmtree(download_dir)

        # AND the manifest should be submitted to the correct place
        assert manifest_id == expected_manifest_id

        # AND the correct upload methods were called for the given record type
        if manifest_record_type == "file_and_entities":
            spy_upload_file_as_csv.assert_called_once()
            spy_upload_file_as_table.assert_not_called()
            spy_upload_file_combo.assert_not_called()
        elif manifest_record_type == "table_and_file":
            spy_upload_file_as_table.assert_called_once()
            spy_upload_file_as_csv.assert_not_called()
            spy_upload_file_combo.assert_not_called()
