import logging
import os
from typing import Any, Callable

import pandas as pd
import requests
from flask.testing import FlaskClient
from synapseclient.client import Synapse

from tests.conftest import ConfigurationForTesting, Helpers
from tests.utils import CleanupItem

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestManifestSubmission:
    def validate_submitted_manifest_file(
        self,
        response: Any,
        syn: Synapse,
        download_location: str,
        data_type: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
        testing_config: ConfigurationForTesting,
    ) -> None:
        """
        Validates the manifest by downloading it, checking its properties, and ensuring the correct columns.

        Args:
            response (Any): The response containing the manifest ID.
            syn (Synapse): An instance of the Synapse client.
            data_type (str): The data type used in manifest.
            download_location (str): path to download location
            schedule_for_cleanup (Callable[[CleanupItem], None]): Returns a closure that takes an item that should be scheduled for cleanup.
            testing_config (ConfigurationForTesting): Confiugration for testing
        """
        # Get the manifest ID from the response
        # manifest_id = (
        #     response.json()
        #     if testing_config.use_deployed_schematic_api_server
        #     else response.json
        # )
        try:
            manifest_id = response.json()
        except (ValueError, TypeError):
            manifest_id = response.json

        # clean up
        schedule_for_cleanup(CleanupItem(manifest_id))

        # Load then manifest from synapse
        manifest_data = syn.get(
            manifest_id,
            downloadLocation=download_location,
            ifcollision="overwrite.local",
        )
        # make sure that the file name of manifest is correct
        assert (
            manifest_data["properties"]["name"]
            == (f"synapse_storage_manifest_{data_type}.csv").lower()
        )

        # make sure that entity id and id columns were added
        manifest_file_path = os.path.join(
            download_location, manifest_data["properties"]["name"]
        )
        manifest_submitted_df = pd.read_csv(manifest_file_path)
        assert "entityId" in manifest_submitted_df.columns
        assert "Id" in manifest_submitted_df.columns

        # make sure that Id column is not empty
        assert manifest_submitted_df["Id"].notnull().all()

    def validate_submitted_manifest_table(
        self,
        syn: Synapse,
        project_id: str,
        data_type: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
    ) -> None:
        """
        Validates the manifest table by checking if it was created in the parent project.

        Args:
            syn (Synapse): An instance of the Synapse client.
            project_id (str): The project ID where the table should be created.
            data_type (str): The data type used in manifest.
            schedule_for_cleanup (Callable[[CleanupItem], None]): Returns a closure that takes an item that should be scheduled for cleanup.
        """
        expected_table_name = f"{data_type}_synapse_storage_manifest_table".lower()
        synapse_id = syn.findEntityId(parent=project_id, name=expected_table_name)
        assert synapse_id is not None
        schedule_for_cleanup(CleanupItem(synapse_id))

    def test_submit_record_based_test_manifest_file_only(
        self,
        helpers: Helpers,
        download_location: str,
        syn: Synapse,
        syn_token: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
        testing_config: ConfigurationForTesting,
        flask_client: FlaskClient,
    ) -> None:
        """Test that a record-based manifest can be submitted with the file_only and replace option

        Args:
            helpers (Helpers): a pytest fixture
            syn_token (str): synapse access token
            syn (Synapse): synapse client
            download_location (str): path to download location
            schedule_for_cleanup (Callable[[CleanupItem], None]): Returns a closure that takes an item that should be scheduled for cleanup.
            testing_config (ConfigurationForTesting): Confiugration for testing
            flask_client (FlaskClient): Local flask client to use instead of API server.

        We are validating the following:
        - The submitted manifest has correct file name: synapse_storage_manifest_<data_type>.csv
        - The submitted manifest has column entityId and Id
        - The submitted manifest has Id column that is not empty
        """

        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        data_type = "Biospecimen"
        params = {
            "schema_url": "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld",
            "data_model_labels": "class_label",
            "data_type": data_type,
            "dataset_id": "syn63561474",
            "manifest_record_type": "file_only",
            "restrict_rules": "false",
            "hide_blanks": "false",
            "asset_view": "syn63561606",
            "table_manipulation": "replace",
            "table_column_names": "class_label",
            "annotation_keys": "class_label",
            "file_annotations_upload": "false",
        }

        headers = {"Authorization": f"Bearer {syn_token}"}
        test_manifest_path = helpers.get_data_path(
            "mock_manifests/mock_example_biospecimen_manifest.csv"
        )

        # THEN we expect a successful response
        response = (
            requests.post(
                url,
                headers=headers,
                params=params,
                files={"file_name": open(test_manifest_path, "rb")},
            )
            if testing_config.use_deployed_schematic_api_server
            else flask_client.post(
                url,
                headers=headers,
                query_string=params,
                data={"file_name": open(test_manifest_path, "rb")},
            )
        )

        assert response.status_code == 200
        self.validate_submitted_manifest_file(
            response=response,
            syn=syn,
            testing_config=testing_config,
            data_type=data_type,
            download_location=download_location,
            schedule_for_cleanup=schedule_for_cleanup,
        )

    def test_submit_record_based_test_manifest_table_and_file(
        self,
        helpers: Helpers,
        syn_token: str,
        syn: Synapse,
        download_location: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
        testing_config: ConfigurationForTesting,
        flask_client: FlaskClient,
    ) -> None:
        """Test that a record-based manifest can be submitted with the table and file and replace option

        Args:
            helpers (Helpers): a pytest fixture
            syn (Synapse): synapse client
            syn_token (str): synapse access token
            download_location (str): path to download location
            schedule_for_cleanup (Callable[[CleanupItem], None]): Returns a closure that takes an item that should be scheduled for cleanup.
            testing_config (ConfigurationForTesting): Confiugration for testing
            flask_client (FlaskClient): Local flask client to use instead of API server.

        We are validating the following:
        - The submitted manifest has correct file name: synapse_storage_manifest_<data_type>.csv
        - The submitted manifest has column entityId and Id
        - The submitted manifest has Id column that is not empty
        - The table gets created in the parent synapse project
        """
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        data_type = "Biospecimen"
        project_id = "syn63561415"
        dataset_id = "syn63561474"
        asset_view = "syn63561606"

        params = {
            "schema_url": "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld",
            "data_model_labels": "class_label",
            "data_type": data_type,
            "dataset_id": dataset_id,
            "manifest_record_type": "table_and_file",
            "restrict_rules": "false",
            "hide_blanks": "false",
            "asset_view": asset_view,
            "table_column_names": "class_label",
            "annotation_keys": "class_label",
            "file_annotations_upload": "false",
        }

        headers = {"Authorization": f"Bearer {syn_token}"}
        test_manifest_path = helpers.get_data_path(
            "mock_manifests/mock_example_biospecimen_manifest.csv"
        )

        # THEN we expect a successful response
        response = (
            requests.post(
                url,
                headers=headers,
                params=params,
                files={"file_name": open(test_manifest_path, "rb")},
            )
            if testing_config.use_deployed_schematic_api_server
            else flask_client.post(
                url,
                headers=headers,
                query_string=params,
                data={"file_name": open(test_manifest_path, "rb")},
            )
        )

        assert response.status_code == 200
        self.validate_submitted_manifest_file(
            response=response,
            syn=syn,
            testing_config=testing_config,
            data_type=data_type,
            download_location=download_location,
            schedule_for_cleanup=schedule_for_cleanup,
        )
        self.validate_submitted_manifest_table(
            syn=syn,
            project_id=project_id,
            data_type=data_type,
            schedule_for_cleanup=schedule_for_cleanup,
        )

    def test_submit_file_based_test_manifest_file_only(
        self,
        helpers: Helpers,
        syn_token: str,
        download_location: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
        testing_config: ConfigurationForTesting,
        syn: Synapse,
    ) -> None:
        """Test that a file-based manifest can be submitted with the file_only and replace option

        Args:
            helpers (Helpers): a pytest fixture
            syn_token (str): synapse access token
            syn (Synapse): synapse client
            download_location (str): path to download location
            schedule_for_cleanup (Callable[[CleanupItem], None]): Returns a closure that takes an item that should be scheduled for cleanup.
            testing_config (ConfigurationForTesting): Confiugration for testing

        We are validating the following:
        - The submitted manifest has correct file name: synapse_storage_manifest_<data_type>.csv
        - The submitted manifest has column entityId and Id
        - The submitted manifest has Id column that is not empty
        """
        url = "http://localhost:3001/v1/model/submit"
        data_type = "BulkRNA-seqAssay"
        params = {
            "schema_url": "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld",
            "data_model_labels": "class_label",
            "data_type": data_type,
            "dataset_id": "syn63561911",
            "manifest_record_type": "file_only",
            "restrict_rules": "false",
            "hide_blanks": "false",
            "asset_view": "syn63561920",
            "table_manipulation": "replace",
            "table_column_names": "class_label",
            "annotation_keys": "class_label",
            "file_annotations_upload": "false",
        }

        headers = {"Authorization": f"Bearer {syn_token}"}
        test_manifest_path = helpers.get_data_path(
            "mock_manifests/mock_example_bulkrnaseq_manifest.csv"
        )

        # THEN we expect a successful response
        response = requests.post(
            url,
            headers=headers,
            params=params,
            files={"file_name": open(test_manifest_path, "rb")},
        )
        assert response.status_code == 200
        self.validate_submitted_manifest_file(
            response=response,
            syn=syn,
            testing_config=testing_config,
            data_type=data_type,
            download_location=download_location,
            schedule_for_cleanup=schedule_for_cleanup,
        )
