import io
import logging
import uuid
from typing import Dict, Generator

import flask
import pytest
from flask.testing import FlaskClient

from schematic.store.synapse import SynapseStorage
from schematic_api.api import create_app
from tests.conftest import Helpers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_MODEL_JSON_LD = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"


@pytest.fixture(scope="class")
def app() -> flask.Flask:
    app = create_app()
    return app


@pytest.fixture(scope="class")
def client(app: flask.Flask) -> Generator[FlaskClient, None, None]:
    app.config["SCHEMATIC_CONFIG"] = None

    with app.test_client() as client:
        yield client


@pytest.fixture
def request_headers(syn_token: str) -> Dict[str, str]:
    headers = {"Authorization": "Bearer " + syn_token}
    return headers


@pytest.mark.schematic_api
class TestManifestSubmission:
    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    def test_submit_nested_manifest_table_and_file_replace(
        self,
        client: FlaskClient,
        request_headers: Dict[str, str],
        helpers: Helpers,
        synapse_store: SynapseStorage,
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
        csv_file = io.BytesIO()
        df.to_csv(csv_file, index=False)
        csv_file.seek(0)  # Rewind the buffer to the beginning

        # WHEN I submit that manifest
        response_csv = client.post(
            "http://localhost:3001/v1/model/submit",
            query_string=params,
            data={"file_name": (csv_file, "test.csv")},
            headers=request_headers,
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
