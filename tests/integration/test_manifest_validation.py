"""
This module is responsible for running through the "Manifest Validation" portion of
the schematic API test plan found here: <https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/3055779846/Schematic+API+test+plan>.
"""

import json
import pytest
import requests
from tests.conftest import Helpers

EXAMPLE_SCHEMA_URL = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"


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
        url = f"{setup_api()}/model/validate"
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
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
        url = f"{setup_api()}/model/validate"
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
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
            ), f"Should be 200 status code. Got {response.status_code}"

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

    # Validate a manifest that triggers simple cross manifest validation rules
    def test_cross_manifest_validation(self, setup_api, helpers: Helpers) -> None:
        # GIVEN the manifest validation endpoint and parameters
        url = f"{setup_api()}/model/validate"
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": "MockComponent",
            "data_model_labels": "class_label",
            "restrict_rules": False,
            "asset_view": "syn63596704",
        }

        # AND a manifest that triggers cross-manifest validation rules
        files = {
            "file_name": (
                "MockComponent-cross-manifest-1.csv",
                open("MockComponent-cross-manifest-1.csv", "rb"),
                "text/csv",
            )
        }

        # AND a list of expected warnings from the POST request
        expected_warnings = [
            [
                None,
                "Check Recommended",
                "Column Check Recommended is recommended but empty.",
                None,
            ],
            [
                None,
                "Check Match at Least",
                "Cross Manifest Validation Warning: There are no target columns to validate this manifest against for attribute: Check Match at Least, and validation rule: matchAtLeastOne Patient.PatientID set. It is assumed this is the first manifest in a series to be submitted, so validation will pass, for now, and will run again when there are manifests uploaded to validate against.",
                None,
            ],
            [
                None,
                "Check Match at Least values",
                "Cross Manifest Validation Warning: There are no target columns to validate this manifest against for attribute: Check Match at Least values, and validation rule: matchAtLeastOne MockComponent.checkMatchatLeastvalues value. It is assumed this is the first manifest in a series to be submitted, so validation will pass, for now, and will run again when there are manifests uploaded to validate against.",
                None,
            ],
            [
                None,
                "Check Match Exactly",
                "Cross Manifest Validation Warning: There are no target columns to validate this manifest against for attribute: Check Match Exactly, and validation rule: matchExactlyOne MockComponent.checkMatchExactly set. It is assumed this is the first manifest in a series to be submitted, so validation will pass, for now, and will run again when there are manifests uploaded to validate against.",
                None,
            ],
            [
                None,
                "Check Match Exactly values",
                "Cross Manifest Validation Warning: There are no target columns to validate this manifest against for attribute: Check Match Exactly values, and validation rule: matchExactlyOne MockComponent.checkMatchExactlyvalues value. It is assumed this is the first manifest in a series to be submitted, so validation will pass, for now, and will run again when there are manifests uploaded to validate against.",
                None,
            ],
            [
                None,
                "Check Match None",
                "Cross Manifest Validation Warning: There are no target columns to validate this manifest against for attribute: Check Match None, and validation rule: matchNone MockComponent.checkMatchNone set error. It is assumed this is the first manifest in a series to be submitted, so validation will pass, for now, and will run again when there are manifests uploaded to validate against.",
                None,
            ],
            [
                None,
                "Check Match None values",
                "Cross Manifest Validation Warning: There are no target columns to validate this manifest against for attribute: Check Match None values, and validation rule: matchNone MockComponent.checkMatchNonevalues value error. It is assumed this is the first manifest in a series to be submitted, so validation will pass, for now, and will run again when there are manifests uploaded to validate against.",
                None,
            ],
        ]

        # TODO: Turn this into a class attribute in case we need the Synapse instance
        # in other places.
        import synapseclient

        syn = synapseclient.login()

        # WHEN any previous files are deleted
        # files_to_delete = syn.getChildren("syn63582792")
        # for file_to_delete in files_to_delete:
        #     syn.delete(file_to_delete['id'])

        # AND we make a POST request to validate the file
        response = requests.post(url, params=params, files=files)

        # THEN we expect a successful response
        assert (
            response.status_code == 200
        ), f"Should be 200 status code. Got {response.status_code}"

        # AND the response should contain the expected warnings
        content = response.content.decode("utf-8")
        data = json.loads(content)
        warnings = data.get("warnings", [])

        for idx, expected_idx in zip(warnings, expected_warnings):
            assert idx == expected_idx

    # Validate a manifest that triggers simple rule combination validation rules
    def test_rule_combination_validation(self, setup_api, helpers: Helpers) -> None:
        # WHEN a manifest file has been uploaded to the Synapse project
        submit_url = f"{setup_api()}/model/submit"
        submit_params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": "MockComponent",
            "dataset_id": "syn63582792",
            "data_model_labels": "class_label",
            "restrict_rules": False,
            "asset_view": "syn63596704",
        }
        submit_files = {
            "file_name": (
                "MockComponent-cross-manifest-1.csv",
                open("MockComponent-cross-manifest-1.csv", "rb"),
                "text/csv",
            )
        }
        submit_response = requests.post(
            submit_url, headers=HEADERS, params=submit_params, files=submit_files
        )
        assert (
            submit_response.status_code == 200
        ), f"File submission was unsuccessful. Got {submit_response.status_code}"

        # AND the manifest validation endpoint and parameters are given
        url = f"{setup_api()}/model/validate"
        params = {
            "schema_url": "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld",
            "data_type": "MockComponent",
            "data_model_labels": "class_label",
            "restrict_rules": False,
            "asset_view": "syn63596704",
            "project_scope": "syn63582791",
        }
        # AND a list of expected warnings is given
        expected_warnings = [
            [
                None,
                "Check Recommended",
                "Column Check Recommended is recommended but empty.",
                None,
            ],
            [
                None,
                "Check Match at Least",
                "Cross Manifest Validation Warning: There are no target columns to validate this manifest against for attribute: Check Match at Least, and validation rule: matchAtLeastOne Patient.PatientID set. It is assumed this is the first manifest in a series to be submitted, so validation will pass, for now, and will run again when there are manifests uploaded to validate against.",
                None,
            ],
        ]

        # AND a file to be uploaded for validation is defined
        files = {
            "file_name": (
                "MockComponent-cross-manifest-2.csv",
                open("MockComponent-cross-manifest-2.csv", "rb"),
                "text/csv",
            )
        }

        # Make the POST request with parameters, headers, and file
        response = requests.post(url, headers=HEADERS, params=params, files=files)

        # THEN we expect a successful response
        assert (
            response.status_code == 200
        ), f"Should be 200 status code. Got {response.status_code}"

        # AND the response should contain the expected warnings
        content = response.content.decode("utf-8")
        data = json.loads(content)
        warnings = data.get("warnings", [])

        for idx, expected_idx in zip(warnings, expected_warnings):
            assert idx == expected_idx

    # Validate a manifest that triggers filename validation rules (should be sufficiently covered by integration test)
    # def test_filename_validation(self, setup_api, helpers: Helpers) -> None:
