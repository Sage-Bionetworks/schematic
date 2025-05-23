import logging
import os
import tempfile
import uuid
from typing import Any, Callable, Dict

import pytest
import requests
from flask.testing import FlaskClient
from synapseclient.client import Synapse

from schematic.configuration.configuration import CONFIG
from schematic.store.synapse import SynapseStorage
from schematic.utils.df_utils import read_csv
from tests.conftest import ConfigurationForTesting, Helpers
from tests.utils import CleanupItem

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DATA_MODEL_JSON_LD = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"


@pytest.fixture
def request_headers(syn_token: str) -> Dict[str, str]:
    """Simple bearer token header for requests"""
    headers = {"Authorization": "Bearer " + syn_token}
    return headers
@pytest.mark.single_process_execution
class TestManifestSubmission:
    def validate_submitted_manifest_file(
        self,
        response: Any,
        syn: Synapse,
        download_location: str,
        data_type: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
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
        manifest_submitted_df = read_csv(manifest_file_path)
        assert "entityId" in manifest_submitted_df.columns
        assert "Id" in manifest_submitted_df.columns

        # make sure that Id column is not empty
        assert manifest_submitted_df["Id"].notnull().all()

    def validate_submitted_manifest_table(
        self,
        syn: Synapse,
        project_id: str,
        data_type: str,
    ) -> None:
        """
        Validates the manifest table by checking if it was created in the parent project.

        Args:
            syn (Synapse): An instance of the Synapse client.
            project_id (str): The project ID where the table should be created.
            data_type (str): The data type used in manifest.
        """
        expected_table_name = f"{data_type}_synapse_storage_manifest_table".lower()
        synapse_id = syn.findEntityId(parent=project_id, name=expected_table_name)
        assert synapse_id is not None

    @pytest.mark.local_or_remote_api
    def test_submit_record_based_test_manifest_file_only(
        self,
        helpers: Helpers,
        download_location: str,
        syn: Synapse,
        schedule_for_cleanup: Callable[[CleanupItem], None],
        testing_config: ConfigurationForTesting,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
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
            request_headers (Dict[str, str]): Headers to use for the request

        We are validating the following:
        - The submitted manifest has correct file name: synapse_storage_manifest_<data_type>.csv
        - The submitted manifest has column entityId and Id
        - The submitted manifest has Id column that is not empty
        """

        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        data_type = "Biospecimen"
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
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

        test_manifest_path = helpers.get_data_path(
            "mock_manifests/mock_example_biospecimen_manifest.csv"
        )

        # THEN we expect a successful response
        try:
            response = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={"file_name": open(test_manifest_path, "rb")},
                    timeout=300,
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    headers=request_headers,
                    query_string=params,
                    data={"file_name": open(test_manifest_path, "rb")},
                )
            )
        finally:
            # Resets the config to its default state
            # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
            CONFIG.load_config("config_example.yml")

        assert response.status_code == 200
        self.validate_submitted_manifest_file(
            response=response,
            syn=syn,
            data_type=data_type,
            download_location=download_location,
            schedule_for_cleanup=schedule_for_cleanup,
        )

    @pytest.mark.slow_test
    @pytest.mark.local_or_remote_api
    def test_submit_record_based_test_manifest_table_and_file(
        self,
        helpers: Helpers,
        syn: Synapse,
        download_location: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
        testing_config: ConfigurationForTesting,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
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
            request_headers (Dict[str, str]): Headers to use for the request

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
            "schema_url": DATA_MODEL_JSON_LD,
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

        test_manifest_path = helpers.get_data_path(
            "mock_manifests/mock_example_biospecimen_manifest.csv"
        )

        # THEN we expect a successful response
        try:
            response = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={"file_name": open(test_manifest_path, "rb")},
                    timeout=300,
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    headers=request_headers,
                    query_string=params,
                    data={"file_name": open(test_manifest_path, "rb")},
                )
            )
        finally:
            # Resets the config to its default state
            # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
            CONFIG.load_config("config_example.yml")

        assert response.status_code == 200
        self.validate_submitted_manifest_file(
            response=response,
            syn=syn,
            data_type=data_type,
            download_location=download_location,
            schedule_for_cleanup=schedule_for_cleanup,
        )
        self.validate_submitted_manifest_table(
            syn=syn,
            project_id=project_id,
            data_type=data_type,
        )

    def test_submit_file_based_test_manifest_file_only(
        self,
        helpers: Helpers,
        download_location: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
        testing_config: ConfigurationForTesting,
        flask_client: FlaskClient,
        syn: Synapse,
        request_headers: Dict[str, str],
    ) -> None:
        """Test that a file-based manifest can be submitted with the file_only and replace option

        Args:
            helpers (Helpers): Utilities for testing
            download_location (str): path to download location
            schedule_for_cleanup (Callable[[CleanupItem], None]): Returns a closure that takes an item that should be scheduled for cleanup.
            testing_config (ConfigurationForTesting): Confiugration for testing
            flask_client (FlaskClient): Local flask client to use instead of API server.
            syn (Synapse): synapse client
            request_headers (Dict[str, str]): Headers to use for the request

        We are validating the following:
        - The submitted manifest has correct file name: synapse_storage_manifest_<data_type>.csv
        - The submitted manifest has column entityId and Id
        - The submitted manifest has Id column that is not empty
        """
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        data_type = "BulkRNA-seqAssay"
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
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

        test_manifest_path = helpers.get_data_path(
            "mock_manifests/mock_example_bulkrnaseq_manifest.csv"
        )

        # THEN we expect a successful response
        response = (
            requests.post(
                url,
                headers=request_headers,
                params=params,
                files={"file_name": open(test_manifest_path, "rb")},
                timeout=300,
            )
            if testing_config.use_deployed_schematic_api_server
            else flask_client.post(
                url,
                headers=request_headers,
                query_string=params,
                data={"file_name": open(test_manifest_path, "rb")},
            )
        )

        assert response.status_code == 200
        self.validate_submitted_manifest_file(
            response=response,
            syn=syn,
            data_type=data_type,
            download_location=download_location,
            schedule_for_cleanup=schedule_for_cleanup,
        )

    @pytest.mark.local_or_remote_api
    def test_submit_file_based_test_manifest_table_and_file(
        self,
        helpers: Helpers,
        syn: Synapse,
        download_location: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
        testing_config: ConfigurationForTesting,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
    ) -> None:
        """Test that a file-based manifest can be submitted with the table and file and replace option

        Args:
            helpers (Helpers): a pytest fixture
            syn (Synapse): synapse client
            syn_token (str): synapse access token
            download_location (str): path to download location
            schedule_for_cleanup (Callable[[CleanupItem], None]): Returns a closure that takes an item that should be scheduled for cleanup.
            testing_config (ConfigurationForTesting): Confiugration for testing
            flask_client (FlaskClient): Local flask client to use instead of API server.
            request_headers (Dict[str, str]): Headers to use for the request

        We are validating the following:
        - The submitted manifest has correct file name: synapse_storage_manifest_<data_type>.csv
        - The submitted manifest has column entityId and Id
        - The submitted manifest has Id column that is not empty
        - The table gets created in the parent synapse project
        """
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        data_type = "BulkRNA-seqAssay"
        project_id = "syn63561904"
        dataset_id = "syn63561911"
        asset_view = "syn63561920"

        params = {
            "schema_url": DATA_MODEL_JSON_LD,
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

        test_manifest_path = helpers.get_data_path(
            "mock_manifests/mock_example_bulkrnaseq_manifest.csv"
        )

        # THEN we expect a successful response
        try:
            response = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={"file_name": open(test_manifest_path, "rb")},
                    timeout=300,
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    headers=request_headers,
                    query_string=params,
                    data={"file_name": open(test_manifest_path, "rb")},
                )
            )
        finally:
            # Resets the config to its default state
            # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
            CONFIG.load_config("config_example.yml")

        assert response.status_code == 200
        self.validate_submitted_manifest_file(
            response=response,
            syn=syn,
            data_type=data_type,
            download_location=download_location,
            schedule_for_cleanup=schedule_for_cleanup,
        )
        self.validate_submitted_manifest_table(
            syn=syn,
            project_id=project_id,
            data_type=data_type,
        )

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
        """
        Testing submit manifest in a csv format as a table and a file.

        We are validating the following:
        - The submission should be successful
        - The file should be uploaded to Synapse with the new annotation
        - The manifest should exist in the dataset folder
        - The manifest table is created
        - Submission works for a nested manifest
        """
        # GIVEN the parameters to submit a manifest
        data_type = "BulkRNA-seqAssay"
        project_id = "syn23643250"
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_type": data_type,
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
            try:
                response_csv = (
                    requests.post(
                        url,
                        headers=request_headers,
                        params=params,
                        files={"file_name": open(tmp_file.name, "rb")},
                        timeout=300,
                    )
                    if testing_config.use_deployed_schematic_api_server
                    else flask_client.post(
                        url,
                        headers=request_headers,
                        query_string=params,
                        data={"file_name": open(tmp_file.name, "rb")},
                    )
                )
            finally:
                # Resets the config to its default state
                # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
                CONFIG.load_config("config_example.yml")

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
        self.validate_submitted_manifest_table(
            syn=synapse_store.syn,
            project_id=project_id,
            data_type=data_type,
        )

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.local_or_remote_api
    def test_submit_manifest_table_and_file_replace(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        helpers: Helpers,
        syn: Synapse,
        testing_config: ConfigurationForTesting,
    ) -> None:
        """Testing submit manifest in a csv format as a table and a file. Only replace
        the table.

        We are validating the following:
        - The submission should be successful
        - The manifest table is created
        """
        # GIVEN the parameters to submit a manifest
        data_type = "Biospecimen"
        project_id = "syn23643250"
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_type": data_type,
            "restrict_rules": False,
            "hide_blanks": False,
            "manifest_record_type": "table_and_file",
            "asset_view": "syn51514344",
            "dataset_id": "syn51514345",
            "table_manipulation": "replace",
            "data_model_labels": "class_label",
            "table_column_names": "class_label",
        }

        # AND a test manifest
        test_manifest_submit = helpers.get_data_path(
            "mock_manifests/example_biospecimen_test.csv"
        )

        # WHEN I submit that manifest
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        try:
            response_csv = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={"file_name": open(test_manifest_submit, "rb")},
                    timeout=300,
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    query_string=params,
                    data={"file_name": (open(test_manifest_submit, "rb"), "test.csv")},
                    headers=request_headers,
                )
            )
        finally:
            # Resets the config to its default state
            # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
            CONFIG.load_config("config_example.yml")

        # THEN the submission should be successful
        assert response_csv.status_code == 200
        self.validate_submitted_manifest_table(
            syn=syn,
            project_id=project_id,
            data_type=data_type,
        )

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.local_or_remote_api
    @pytest.mark.parametrize(
        "data_type",
        [
            ("Biospecimen"),
            ("MockComponent"),
        ],
    )
    def test_submit_manifest_file_only_replace(
        self,
        helpers: Helpers,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        data_type: str,
        syn: Synapse,
        testing_config: ConfigurationForTesting,
    ) -> None:
        """Testing submit manifest in a csv format as a file.

        We are validating the following:
        - The submission should be successful
        - The manifest table is created
        """
        # GIVEN a test manifest
        if data_type == "Biospecimen":
            manifest_path = helpers.get_data_path(
                "mock_manifests/example_biospecimen_test.csv"
            )
        elif data_type == "MockComponent":
            manifest_path = helpers.get_data_path(
                "mock_manifests/Valid_Test_Manifest.csv"
            )

        # AND the parameters to submit a manifest
        project_id = "syn23643250"
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_type": data_type,
            "restrict_rules": False,
            "manifest_record_type": "file_only",
            "table_manipulation": "replace",
            "data_model_labels": "class_label",
            "table_column_names": "class_label",
        }

        if data_type == "Biospecimen":
            specific_params = {
                "asset_view": "syn51514344",
                "dataset_id": "syn51514345",
            }

        elif data_type == "MockComponent":
            python_version = helpers.get_python_version()

            if python_version == "3.10":
                dataset_id = "syn52656106"
            elif python_version == "3.9":
                dataset_id = "syn52656104"

            specific_params = {
                "asset_view": "syn23643253",
                "dataset_id": dataset_id,
                "project_scope": ["syn54126707"],
            }

        params.update(specific_params)

        # WHEN I submit that manifest
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        response_csv = (
            requests.post(
                url,
                headers=request_headers,
                params=params,
                files={"file_name": open(manifest_path, "rb")},
                timeout=300,
            )
            if testing_config.use_deployed_schematic_api_server
            else flask_client.post(
                url,
                query_string=params,
                data={"file_name": (open(manifest_path, "rb"), "test.csv")},
                headers=request_headers,
            )
        )

        # THEN the submission should be successful
        assert response_csv.status_code == 200
        self.validate_submitted_manifest_table(
            syn=syn,
            project_id=project_id,
            data_type=data_type,
        )

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.local_or_remote_api
    def test_submit_manifest_json_str_replace(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        syn: Synapse,
        testing_config: ConfigurationForTesting,
    ) -> None:
        """Submit json str as a file.


        We are validating the following:
        - The submission should be successful
        - The manifest table is created
        """
        # GIVEN a test json str
        json_str = '[{"Sample ID": 123, "Patient ID": 1,"Tissue Status": "Healthy","Component": "Biospecimen"}]'

        # AND the parameters to submit a manifest
        project_id = "syn23643250"
        data_type = "Biospecimen"
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_type": data_type,
            "json_str": json_str,
            "restrict_rules": False,
            "manifest_record_type": "file_only",
            "asset_view": "syn51514344",
            "dataset_id": "syn51514345",
            "table_manipulation": "replace",
            "data_model_labels": "class_label",
            "table_column_names": "class_label",
        }
        params["json_str"] = json_str

        # WHEN I submit that manifest
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        try:
            response = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={"file_name": ""},
                    timeout=300,
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    query_string=params,
                    data={"file_name": ""},
                    headers=request_headers,
                )
            )
        finally:
            # Resets the config to its default state
            # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
            CONFIG.load_config("config_example.yml")

        # THEN the submission should be successful
        assert response.status_code == 200
        self.validate_submitted_manifest_table(
            syn=syn,
            project_id=project_id,
            data_type=data_type,
        )

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.local_or_remote_api
    def test_submit_manifest_w_file_and_entities(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        helpers: Helpers,
        syn: Synapse,
        testing_config: ConfigurationForTesting,
    ) -> None:
        """Testing submit manifest in a csv format as a file and entities.


        We are validating the following:
        - The submission should be successful
        - The manifest table is created
        """
        # GIVEN the parameters to submit a manifest
        project_id = "syn23643250"
        data_type = "Biospecimen"
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_type": data_type,
            "restrict_rules": False,
            "manifest_record_type": "file_and_entities",
            "asset_view": "syn51514501",
            "dataset_id": "syn51514523",
            "table_manipulation": "replace",
            "data_model_labels": "class_label",
            "table_column_names": "class_label",
            "annotation_keys": "class_label",
        }
        test_manifest_submit = helpers.get_data_path(
            "mock_manifests/example_biospecimen_test.csv"
        )

        # WHEN I submit that manifest
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        try:
            response_csv = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={"file_name": open(test_manifest_submit, "rb")},
                    timeout=300,
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    query_string=params,
                    data={"file_name": (open(test_manifest_submit, "rb"), "test.csv")},
                    headers=request_headers,
                )
            )
        finally:
            # Resets the config to its default state
            # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
            CONFIG.load_config("config_example.yml")

        # THEN the submission should be successful
        assert response_csv.status_code == 200
        self.validate_submitted_manifest_table(
            syn=syn,
            project_id=project_id,
            data_type=data_type,
        )

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.local_or_remote_api
    def test_submit_manifest_table_and_file_upsert(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        helpers: Helpers,
        syn: Synapse,
        testing_config: ConfigurationForTesting,
    ) -> None:
        """Testing submit manifest in a csv format as a table and a file. Upsert
        the table.


        We are validating the following:
        - The submission should be successful
        - The manifest table is created
        """
        # GIVEN the parameters to submit a manifest
        project_id = "syn23643250"
        data_type = "MockRDB"
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_type": data_type,
            "restrict_rules": False,
            "manifest_record_type": "table_and_file",
            "asset_view": "syn51514557",
            "dataset_id": "syn51514551",
            "table_manipulation": "upsert",
            "data_model_labels": "class_label",
            # have to set table_column_names to display_name to ensure upsert feature works
            "table_column_names": "display_name",
        }

        # AND a test manifest
        test_upsert_manifest_csv = helpers.get_data_path(
            "mock_manifests/rdb_table_manifest.csv"
        )

        # WHEN I submit that manifest
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        try:
            response_csv = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={"file_name": open(test_upsert_manifest_csv, "rb")},
                    timeout=300,
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    query_string=params,
                    data={
                        "file_name": (open(test_upsert_manifest_csv, "rb"), "test.csv")
                    },
                    headers=request_headers,
                )
            )
        finally:
            # Resets the config to its default state
            # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
            CONFIG.load_config("config_example.yml")

        # THEN the submission should be successful
        assert response_csv.status_code == 200
        self.validate_submitted_manifest_table(
            syn=syn,
            project_id=project_id,
            data_type=data_type,
        )

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.local_or_remote_api
    def test_submit_and_validate_filebased_manifest(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        helpers: Helpers,
        syn: Synapse,
        testing_config: ConfigurationForTesting,
    ) -> None:
        """Testing submit manifest in a csv format as a file.


        We are validating the following:
        - The submission should be successful
        - The manifest table is created
        """
        # GIVEN the parameters to submit a manifest
        project_id = "syn23643250"
        data_type = "MockFilename"
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_type": data_type,
            "restrict_rules": False,
            "manifest_record_type": "file_and_entities",
            "asset_view": "syn23643253",
            "dataset_id": "syn62822337",
            "project_scope": "syn23643250",
            "dataset_scope": "syn62822337",
            "data_model_labels": "class_label",
            "table_column_names": "class_label",
        }

        valid_filename_manifest_csv = helpers.get_data_path(
            "mock_manifests/ValidFilenameManifest.csv"
        )

        # WHEN a filebased manifest is validated with the filenameExists rule and uploaded
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        try:
            response_csv = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={"file_name": open(valid_filename_manifest_csv, "rb")},
                    timeout=300,
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    query_string=params,
                    data={
                        "file_name": (
                            open(valid_filename_manifest_csv, "rb"),
                            "test.csv",
                        )
                    },
                    headers=request_headers,
                )
            )
        finally:
            # Resets the config to its default state
            # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
            CONFIG.load_config("config_example.yml")

        # THEN the validation and submission should be successful
        assert response_csv.status_code == 200
        self.validate_submitted_manifest_table(
            syn=syn,
            project_id=project_id,
            data_type=data_type,
        )

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.local_or_remote_api
    def test_submit_manifest_with_hide_blanks(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        helpers: Helpers,
        syn: Synapse,
        testing_config: ConfigurationForTesting,
    ) -> None:
        """Testing submit manifest in a csv format as a table and a file. Hide blanks.


        We are validating the following:
        - The submission should be successful
        - A randomized annotation should be added to the file
        - The blank annotations are not present
        """
        # GIVEN the parameters to submit a manifest
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_model_labels": "class_label",
            "dataset_id": "syn63606804",
            "manifest_record_type": "table_and_file",
            "restrict_rules": "false",
            "hide_blanks": "true",
            "asset_view": "syn63561920",
            "table_column_names": "class_label",
            "annotation_keys": "class_label",
            "file_annotations_upload": "true",
        }

        # AND a test manifest
        test_submit_manifest_with_hide_blanks_manifest = helpers.get_data_path(
            "mock_manifests/TestManifestSubmission_test_submit_manifest_with_hide_blanks.csv"
        )

        # AND a randomized annotation we can verify was added
        df = helpers.get_data_frame(path=test_submit_manifest_with_hide_blanks_manifest)
        randomized_annotation_content = str(uuid.uuid4())
        df["RandomizedAnnotation"] = randomized_annotation_content

        # AND a "None" string remains in the manifest
        df["NoneString"] = "None"
        df["NoneString1"] = "none"
        df["NoneString2"] = "NoNe"

        with tempfile.NamedTemporaryFile(delete=True, suffix=".csv") as tmp_file:
            # Write the DF to a temporary file
            df.to_csv(tmp_file.name, index=False)

            # WHEN the manifest is submitted
            url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
            try:
                response_csv = (
                    requests.post(
                        url,
                        headers=request_headers,
                        params=params,
                        files={"file_name": open(tmp_file.name, "rb")},
                        timeout=300,
                    )
                    if testing_config.use_deployed_schematic_api_server
                    else flask_client.post(
                        url,
                        query_string=params,
                        data={"file_name": (open(tmp_file.name, "rb"), "test.csv")},
                        headers=request_headers,
                    )
                )
            finally:
                # Resets the config to its default state
                # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
                CONFIG.load_config("config_example.yml")

        # THEN the validation and submission should be successful
        assert response_csv.status_code == 200

        # AND the randomized annotation should be added to the file
        modified_file = syn.get(df["entityId"][0], downloadFile=False)
        assert modified_file is not None
        assert modified_file["RandomizedAnnotation"][0] == randomized_annotation_content
        assert modified_file["NoneString"][0] == "None"
        assert modified_file["NoneString1"][0] == "none"
        assert modified_file["NoneString2"][0] == "NoNe"

        # AND the blank annotations are not present
        assert "Genome Build" not in modified_file
        assert "Genome FASTA" not in modified_file

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.local_or_remote_api
    def test_submit_manifest_with_blacklisted_characters(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        helpers: Helpers,
        syn: Synapse,
        testing_config: ConfigurationForTesting,
    ) -> None:
        """Testing submit manifest in a csv format as a table and a file.
        Blacklisted characters.


        We are validating the following:
        - The submission should be successful
        - Annotation with blacklisted characters should not be present
        - Annotation with the stripped blacklisted characters should be present
        """
        # GIVEN the parameters to submit a manifest
        params = {
            "schema_url": DATA_MODEL_JSON_LD,
            "data_model_labels": "class_label",
            "dataset_id": "syn63607040",
            "manifest_record_type": "table_and_file",
            "restrict_rules": "false",
            "hide_blanks": "true",
            "asset_view": "syn63561920",
            "table_column_names": "display_label",
            "annotation_keys": "display_label",
            "file_annotations_upload": "true",
        }

        # AND a test manifest
        test_submit_manifest_with_blacklisted_characters = helpers.get_data_path(
            "mock_manifests/TestManifestSubmission_test_submit_manifest_with_blacklisted_characters.csv"
        )
        df = helpers.get_data_frame(
            path=test_submit_manifest_with_blacklisted_characters
        )

        # WHEN the manifest is submitted
        url = f"{testing_config.schematic_api_server_url}/v1/model/submit"
        try:
            response_csv = (
                requests.post(
                    url,
                    headers=request_headers,
                    params=params,
                    files={
                        "file_name": open(
                            test_submit_manifest_with_blacklisted_characters, "rb"
                        )
                    },
                    timeout=300,
                )
                if testing_config.use_deployed_schematic_api_server
                else flask_client.post(
                    url,
                    query_string=params,
                    data={
                        "file_name": (
                            open(
                                test_submit_manifest_with_blacklisted_characters, "rb"
                            ),
                            "test.csv",
                        )
                    },
                    headers=request_headers,
                )
            )
        finally:
            # Resets the config to its default state
            # TODO: remove with https://sagebionetworks.jira.com/browse/SCHEMATIC-202
            CONFIG.load_config("config_example.yml")

        # THEN the validation and submission should be successful
        assert response_csv.status_code == 200

        # AND the randomized annotation should be added to the file
        modified_file = syn.get(df["entityId"][0], downloadFile=False)
        assert modified_file is not None

        # AND the blacklisted characters are not present
        assert "File-Format" not in modified_file

        # AND the stripped non-blacklisted characters are present
        assert "FileFormat" in modified_file
