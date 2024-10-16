import logging
import tempfile
import uuid
from typing import Dict

import pytest
import requests
from flask.testing import FlaskClient

from schematic.store.synapse import SynapseStorage
from tests.conftest import ConfigurationForTesting, Helpers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_MODEL_JSON_LD = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"


@pytest.fixture
def request_headers(syn_token: str) -> Dict[str, str]:
    headers = {"Authorization": "Bearer " + syn_token}
    return headers


@pytest.mark.schematic_api
class TestManifestSubmission:
    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.local_or_remote_api
    def test_submit_nested_manifest_table_and_file_replace(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        helpers: Helpers,
        synapse_store: SynapseStorage,
        testing_config: ConfigurationForTesting,
    ) -> None:
        # GIVEN the parameters to submit a manifest
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_type": "BulkRNA-seqAssay",
            "restrict_rules": False,
            "manifest_record_type": "table_and_file",
            "asset_view": "syn63646213",
            "dataset_id": "syn63646197",
            "table_manipulation": "replace",
            "data_model_labels": "class_label",
            "table_column_names": "display_name",
        }

        # AND a test manifest with a nested file entity
        nested_manifest_replace_csv = helpers.get_data_path(
            "mock_manifests/TestManifestOperation_test_submit_nested_manifest_table_and_file_replace.csv"
        )

        # AND a randomized annotation we can verify was added
        df = helpers.get_data_frame(path=nested_manifest_replace_csv)
        randomized_annotation_content = str(uuid.uuid4())
        df["RandomizedAnnotation"] = randomized_annotation_content

        with tempfile.NamedTemporaryFile(delete=True, suffix=".csv") as tmp_file:
            # Write the DF to a temporary file
            df.to_csv(tmp_file.name, index=False)

            # WHEN I submit that manifest
            url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
            response_csv = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={"file_name": open(tmp_file.name, "rb")},
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    headers=request_headers,
                    query_string=params,
                    data={"file_name": open(tmp_file.name, "rb")},
                )
            )

        # THEN the submission should be successful
        assert response_csv.status_code == 200

        # AND the file should be uploaded to Synapse with the new annotation
        modified_file = synapse_store.syn.get(df["entityId"][0], downloadFile=False)
        assert modified_file is not None
        assert modified_file["RandomizedAnnotation"][0] == randomized_annotation_content

        # AND the manifest should exist in the dataset folder
        manifest_synapse_id = synapse_store.syn.findEntityId(
            name="synapse_storage_manifest_bulkrna-seqassay.csv", parent="syn63646197"
        )
        assert manifest_synapse_id is not None
        synapse_manifest_entity = synapse_store.syn.get(
            entity=manifest_synapse_id, downloadFile=False
        )
        assert synapse_manifest_entity is not None
        assert (
            synapse_manifest_entity["_file_handle"]["fileName"]
            == "synapse_storage_manifest_bulkrna-seqassay.csv"
        )

        # AND the manifest table is created
        expected_table_name = "bulkrna-seqassay_synapse_storage_manifest_table"
        synapse_id = synapse_store.syn.findEntityId(
            parent="syn23643250", name=expected_table_name
        )
        assert synapse_id is not None
