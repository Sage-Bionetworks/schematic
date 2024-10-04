"""
This module is responsible for running through the "Manifest Validation" portion of
the schematic API test plan found here: <https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/3055779846/Schematic+API+test+plan>.
"""

import pytest
import requests
from tests.conftest import Helpers


class TestManifestValidation:
    @pytest.fixture(scope="module")
    def setup_api(self, request):
        local = getattr(request, "param", False)
        if local:
            api_server = "http://localhost:3001"
            url = api_server
        else:
            api_server = "https://schematic-dev.api.sagebionetworks.org/v1"
            url = f"{api_server}/ui"

        # Make a request to the API and make sure it is responsive
        response = requests.get(url)
        assert response.status_code == 200, f"Failed to connect to API: {response.text}"
        return api_server

    @pytest.mark.parametrize(
        ("input_data_type", "input_file_name"),
        [
            ("Biospecimen", "mock_manifests/example_biospecimen_test.csv"),
            ("Patient", "mock_manifests/example.patient_component_rule.manifest.csv"),
        ],
    )
    def test_manifest_validation_basic_valid(
        self, input_data_type: str, input_file_name: str, setup_api, helpers: Helpers
    ) -> None:
        """
        Test that the manifest validation API returns no errors when a valid manifest is provided.
        """
        # GIVEN the manifest validation endpoint and parameters
        url = f"{setup_api}/model/validate"
        params = {
            "schema_url": "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld",
            "data_type": input_data_type,
            "data_model_labels": "class_label",
            "restrict_rules": False,
        }
        # AND a valid file
        file = helpers.get_data_path(input_file_name)
        with open(file, "rb") as file_obj:
            files = {"file_name": (file, file_obj, "text/csv")}

            # WHEN we make a POST request to validate the file
            response = requests.post(url, params=params, files=files)
            response_json = response.json()

            # THEN we expect a successful response
            assert (
                response.status_code == 200
            ), f"Got status code: {response.status_code}. Expected '200'."

            # AND with expected keys in the json
            assert "warnings" in response_json.keys()
            assert "errors" in response_json.keys()

            # AND with no expected errors
            assert len(response_json.get("errors")) == 0

    @pytest.mark.parametrize(
        ("input_data_type", "input_file_name"),
        [
            (
                "Biospecimen",
                "mock_manifests/Invalid_Biospecimen_Missing_Column_Manifest.csv",
            ),
        ],
    )
    def test_manifest_validation_basic_invalid(
        self, input_data_type: str, input_file_name: str, setup_api, helpers: Helpers
    ) -> None:
        """
        Test that the manifest validation API returns errors when an invalid manifest is provided.
        """
        # GIVEN the manifest validation endpoint and parameters
        url = f"{setup_api}/model/validate"
        params = {
            "schema_url": "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld",
            "data_type": input_data_type,
            "data_model_labels": "class_label",
            "restrict_rules": False,
        }
        # AND an invalid file
        file = helpers.get_data_path(input_file_name)
        with open(file, "rb") as file_obj:
            files = {"file_name": (file, file_obj, "text/csv")}

            # WHEN we make a POST request to validate the file
            response = requests.post(url, params=params, files=files)
            response_json = response.json()

            # THEN we expect a successful response
            assert (
                response.status_code == 200
            ), f"Got status code: {response.status_code}. Expected '200'."

            # AND with expected keys in the json
            assert (
                "warnings" in response_json.keys()
            ), f"Expected 'warnings' in response json. Got {response_json.keys()}"
            assert (
                "errors" in response_json.keys()
            ), f"Expected 'errors' in response json. Got {response_json.keys()}"

            # AND with the expected error
            assert (
                len(response_json.get("errors")) > 0
            ), "Expected at least one error. Got none."

            # AND with the expected error message
            assert any(
                "Wrong schema" in error for error in response_json.get("errors")
            ), f"Expected 'Wrong schema' error. Got {response_json.get('errors')}"
