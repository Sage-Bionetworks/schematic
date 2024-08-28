import logging
from contextlib import nullcontext as does_not_raise
from unittest.mock import patch

from tests.conftest import metadata_model

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestMetadataModel:
    def test_submit_filebased_manifest(self, helpers):
        meta_data_model = metadata_model(helpers, "class_label")

        manifest_path = helpers.get_data_path(
            "mock_manifests/filepath_submission_test_manifest.csv"
        )

        with does_not_raise():
            manifest_id = meta_data_model.submit_metadata_manifest(
                manifest_path=manifest_path,
                dataset_id="syn62276880",
                manifest_record_type="file_only",
                restrict_rules=False,
                file_annotations_upload=True,
            )
            assert manifest_id == "syn62280543"
