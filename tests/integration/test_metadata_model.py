"""
This script contains a test suite for verifying the submission and annotation of file-based manifests using the `TestMetadataModel` class
to communicate with Synapse and verify the expected behavior of uploading annotation manifest CSVs using the metadata model.

It utilizes the `pytest` framework along with `pytest-mock` to mock and spy on methods of the `SynapseStorage` class,
which is responsible for handling file uploads and annotations in Synapse.
"""

import logging
import pytest

from contextlib import nullcontext as does_not_raise

import pytest
from pytest_mock import MockerFixture

from schematic.store.synapse import SynapseStorage
from tests.conftest import metadata_model

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestMetadataModel:
    @pytest.mark.parametrize(
        "manifest_path, dataset_id, validate_component, expected_manifest_id, dataset_scope",
        [
            (
                "mock_manifests/filepath_submission_test_manifest.csv",
                "syn62276880",
                None,
                "syn62280543",
                None,
            ),
            (
                "mock_manifests/filepath_submission_test_manifest_sampleidx10.csv",
                "syn62276880",
                None,
                "syn62280543",
                None,
            ),
            (
                "mock_manifests/ValidFilenameManifest.csv",
                "syn62822337",
                "MockFilename",
                "syn62822975",
                "syn62822337",
            ),
        ],
    )
    def test_submit_filebased_manifest(
        self,
        helpers,
        manifest_path,
        dataset_id,
        validate_component,
        expected_manifest_id,
        dataset_scope,
        mocker: MockerFixture,
        synapse_store,
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
                manifest_record_type="file_and_entities",
                restrict_rules=False,
                file_annotations_upload=True,
                hide_blanks=False,
                validate_component=validate_component,
                dataset_scope=dataset_scope,
            )

        # AND the files should be annotated
        spy_add_annotations.assert_called_once()

        # AND the annotations should have the correct metadata
        for index, row in manifest.iterrows():
            entityId = row["entityId"]
            expected_sample_id = row["Sample ID"]
            annos = synapse_store.syn.get_annotations(entityId)
            sample_id = annos["SampleID"][0]
            assert str(sample_id) == str(expected_sample_id)

        # AND the manifest should be submitted to the correct place
        assert manifest_id == expected_manifest_id

        # AND the manifest should be uploaded as a CSV
        spy_upload_file_as_csv.assert_called_once()

        # AND the manifest should not be uploaded as a table or combination of table, file, and entities
        spy_upload_file_as_table.assert_not_called()
        spy_upload_file_combo.assert_not_called()
