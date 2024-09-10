import logging
import pytest
from contextlib import nullcontext as does_not_raise

from pytest_mock import MockerFixture

from schematic.store.synapse import SynapseStorage
from tests.conftest import metadata_model

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestMetadataModel:
    @pytest.mark.parametrize("manifest_path", ["mock_manifests/filepath_submission_test_manifest.csv", "mock_manifests/filepath_submission_test_manifest_sampleidx10.csv"])
    def test_submit_filebased_manifest(
        self, helpers, mocker: MockerFixture, synapse_store, manifest_path
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
            manifest_path, preserve_raw_input=False, **load_args
        )
        manifest_full_path = helpers.get_data_path(manifest_path)

        # WHEN the manifest it submitted and files are annotated
        # THEN submission should complete without error
        with does_not_raise():
            manifest_id = meta_data_model.submit_metadata_manifest(
                manifest_path=manifest_full_path,
                dataset_id="syn62276880",
                manifest_record_type="file_and_entities",
                restrict_rules=False,
                file_annotations_upload=True,
                hide_blanks=False,
            )

        # AND the files should be annotated
        spy_add_annotations.assert_called_once()

        # AND the annotations should have the correct metadata
        for row in manifest.itertuples():
            entityId = row.entityId
            expected_sample_id = row._2
            annos = synapse_store.syn.get_annotations(entityId)
            sample_id = annos["SampleID"][0]
            assert sample_id == expected_sample_id

        # AND the manifest should be submitted to the correct place
        assert manifest_id == "syn62280543"

        # AND the manifest should be uploaded as a CSV
        spy_upload_file_as_csv.assert_called_once()

        # AND the manifest should not be uploaded as a table or combination of table, file, and entities
        spy_upload_file_as_table.assert_not_called()
        spy_upload_file_combo.assert_not_called()
