"""
This module is responsible for running through the "Manifest Validation" portion of
the schematic API test plan found here: <https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/3055779846/Schematic+API+test+plan>.
"""

import json
from typing import Dict

import pytest
import requests
from flask.testing import FlaskClient
from synapseclient.client import Synapse

from tests.conftest import ConfigurationForTesting, Helpers

EXAMPLE_SCHEMA_URL = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"


@pytest.fixture
def request_headers(syn_token: str) -> Dict[str, str]:
    """Simple bearer token header for requests"""
    headers = {"Authorization": "Bearer " + syn_token}
    return headers


class TestManifestValidation:
    @pytest.mark.local_or_remote_api
    @pytest.mark.parametrize(
        ("input_data_type", "input_file_name"),
        [
            ("Biospecimen", "mock_manifests/example_biospecimen_test.csv"),
            ("Patient", "mock_manifests/example.patient_component_rule.manifest.csv"),
        ],
    )
    def test_manifest_validation_basic_valid(
        self,
        input_data_type: str,
        input_file_name: str,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        testing_config: ConfigurationForTesting,
        helpers: Helpers,
    ) -> None:
        """
        Test that the manifest validation API returns no errors when a valid manifest is provided.
        """
        # GIVEN the manifest validation endpoint and parameters
        url = f"{testing_config.schematic_api_server_url}/v1/model/validate"
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": input_data_type,
            "data_model_labels": "class_label",
            "restrict_rules": False,
        }

        # AND a valid file
        file_path = helpers.get_data_path(input_file_name)

        # WHEN we make a POST request to validate the file
        response = (
            requests.post(
                url,
                params=params,
                files={"file_name": open(file_path, "rb")},
                headers=request_headers,
            )
            if testing_config.use_deployed_schematic_api_server
            else flask_client.post(
                url,
                headers=request_headers,
                query_string=params,
                data={"file_name": open(file_path, "rb")},
            )
        )

        # THEN we expect a successful response
        assert (
            response.status_code == 200
        ), f"Got status code: {response.status_code}. Expected '200'."

        # AND with expected keys in the json
        response_json = (
            response.json()
            if testing_config.use_deployed_schematic_api_server
            else response.json
        )
        assert "warnings" in response_json.keys()
        assert "errors" in response_json.keys()

        # AND with no expected errors
        assert len(response_json.get("errors")) == 0

    @pytest.mark.local_or_remote_api
    @pytest.mark.parametrize(
        ("input_data_type", "input_file_name"),
        [
            (
                "Patient",
                "mock_manifests/TestManifestValidation_test_patient_manifest_invalid.csv",
            ),
        ],
    )
    def test_manifest_validation_basic_invalid(
        self,
        input_data_type: str,
        input_file_name: str,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        testing_config: ConfigurationForTesting,
        helpers: Helpers,
    ) -> None:
        """
        Test that the manifest validation API returns errors when an invalid manifest is provided.
        """
        # GIVEN the manifest validation endpoint and parameters
        url = f"{testing_config.schematic_api_server_url}/v1/model/validate"
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": input_data_type,
            "data_model_labels": "class_label",
            "restrict_rules": False,
        }

        # AND an invalid file
        file = helpers.get_data_path(input_file_name)

        # WHEN we make a POST request to validate the file
        response = (
            requests.post(
                url,
                params=params,
                files={"file_name": open(file, "rb")},
                headers=request_headers,
            )
            if testing_config.use_deployed_schematic_api_server
            else flask_client.post(
                url,
                headers=request_headers,
                query_string=params,
                data={"file_name": open(file, "rb")},
            )
        )
        response_json = (
            response.json()
            if testing_config.use_deployed_schematic_api_server
            else response.json
        )

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
        expected_errors = [
            [
                "2",
                "Family History",
                "For attribute Family History in row 2 it does not appear as if you provided a comma delimited string. Please check your entry ('Random'') and try again.",
                "Random",
            ],
            [
                "2",
                "Family History",
                # Truncating the rest of the message because order of the list is not guaranteed
                "'Random' is not one of [",
                "Random",
            ],
            [
                "2",
                "Cancer Type",
                # Truncating the rest of the message because order of the list is not guaranteed
                "'Random' is not one of [",
                "Random",
            ],
        ]

        response_errors = response_json.get("errors")

        for response_error in response_errors:
            assert any(
                response_error[0] == expected_error[0]
                and response_error[1] == expected_error[1]
                and response_error[2].startswith(expected_error[2])
                and response_error[3] == expected_error[3]
                for expected_error in expected_errors
            )
            if response_error[2].startswith("'Random' is not one of"):
                assert "Lung" in response_error[2]
                assert "Breast" in response_error[2]
                assert "Prostate" in response_error[2]
                assert "Colorectal" in response_error[2]
                assert "Skin" in response_error[2]

    @pytest.mark.local_or_remote_api
    def test_cross_manifest_validation_with_no_target(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        testing_config: ConfigurationForTesting,
        helpers: Helpers,
    ) -> None:
        """
        Test that the manifest validation API returns warnings when cross validation is triggered
        with no target provided.
        """
        # GIVEN the manifest validation endpoint and parameters
        url = f"{testing_config.schematic_api_server_url}/v1/model/validate"
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": "MockComponent",
            "data_model_labels": "class_label",
            "restrict_rules": False,
            "asset_view": "syn63825013",
        }

        # AND a manifest that triggers cross-manifest validation rules
        input_file_name = "mock_manifests/MockComponent-cross-manifest-1.csv"
        file_path = helpers.get_data_path(input_file_name)

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

        # AND we make a POST request to validate the file
        response = (
            requests.post(
                url,
                headers=request_headers,
                params=params,
                files={"file_name": open(file_path, "rb")},
                timeout=300,
            )
            if testing_config.use_deployed_schematic_api_server
            else flask_client.post(
                url,
                headers=request_headers,
                query_string=params,
                data={"file_name": open(file_path, "rb")},
            )
        )

        # THEN we expect a successful response
        assert (
            response.status_code == 200
        ), f"Should be 200 status code. Got {response.status_code}"

        # AND the response should contain the expected warnings
        content = (
            response.content
            if testing_config.use_deployed_schematic_api_server
            else response.data
        ).decode("utf-8")
        data = json.loads(content)
        warnings = data.get("warnings", [])

        for idx, expected_idx in zip(warnings, expected_warnings):
            assert idx == expected_idx

    @pytest.mark.local_or_remote_api
    def test_cross_manifest_validation_with_target(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        testing_config: ConfigurationForTesting,
        helpers: Helpers,
    ) -> None:
        """
        Test that the manifest validation API returns warnings when a manifest target is provided.
        """
        # WHEN a manifest file has been uploaded to the Synapse project
        # the manifest validation endpoint and parameters are given
        url = f"{testing_config.schematic_api_server_url}/v1/model/validate"
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
        input_file = "mock_manifests/MockComponent-cross-manifest-2.csv"
        input_file_path = helpers.get_data_path(input_file)

        # AND we make a POST request to validate the file
        response = (
            requests.post(
                url,
                headers=request_headers,
                params=params,
                files={"file_name": open(input_file_path, "rb")},
                timeout=300,
            )
            if testing_config.use_deployed_schematic_api_server
            else flask_client.post(
                url,
                headers=request_headers,
                query_string=params,
                data={"file_name": open(input_file_path, "rb")},
            )
        )

        # THEN we expect a successful response
        assert (
            response.status_code == 200
        ), f"Should be 200 status code. Got {response.status_code}"

        # AND the response should contain the expected warnings
        content = (
            response.content
            if testing_config.use_deployed_schematic_api_server
            else response.data
        ).decode("utf-8")
        data = json.loads(content)
        warnings = data.get("warnings", [])

        for idx, expected_idx in zip(warnings, expected_warnings):
            assert idx == expected_idx

    @pytest.mark.local_or_remote_api
    def test_manifest_validation_with_rule_combination(
        self,
        flask_client: FlaskClient,
        request_headers: Dict[str, str],
        testing_config: ConfigurationForTesting,
        helpers: Helpers,
    ) -> None:
        """
        Test that the manifest validation API returns the expected warnings and errors when
        simple rule combination validation rules are triggered.
        """
        # GIVEN the manifest validation endpoint and parameters
        url = f"{testing_config.schematic_api_server_url}/v1/model/validate"
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": "MockComponent",
            "data_model_labels": "class_label",
            "restrict_rules": False,
            "asset_view": "syn63622565",
        }

        # AND a file to be uploaded for validation is defined
        input_file = "mock_manifests/Mock_Component_rule_combination.csv"
        input_file_path = helpers.get_data_path(input_file)

        # AND we make a POST request to validate the file
        response = (
            requests.post(
                url,
                headers=request_headers,
                params=params,
                files={"file_name": open(input_file_path, "rb")},
                timeout=300,
            )
            if testing_config.use_deployed_schematic_api_server
            else flask_client.post(
                url,
                headers=request_headers,
                query_string=params,
                data={"file_name": open(input_file_path, "rb")},
            )
        )

        # AND the expected response contents is given
        expected_contents = {
            "errors": [
                [
                    "2",
                    "Check Regex List",
                    'For the attribute Check Regex List, on row 2, the string is not properly formatted. It should follow the following re.match pattern "[a-f]".',
                    ["a", "b", "c", "d", "e", "f", "g", "h"],
                ],
                [
                    "2",
                    "Check Regex List",
                    'For the attribute Check Regex List, on row 2, the string is not properly formatted. It should follow the following re.match pattern "[a-f]".',
                    ["a", "b", "c", "d", "e", "f", "g", "h"],
                ],
                [
                    "4",
                    "Check Regex List Like",
                    'For the attribute Check Regex List Like, on row 4, the string is not properly formatted. It should follow the following re.match pattern "[a-f]".',
                    ["a", "c", "h"],
                ],
                [
                    "2",
                    "Check Regex List Strict",
                    "For attribute Check Regex List Strict in row 2 it does not appear as if you provided a comma delimited string. Please check your entry ('a'') and try again.",
                    "a",
                ],
                [
                    "4",
                    "Check Regex List Strict",
                    'For the attribute Check Regex List Strict, on row 4, the string is not properly formatted. It should follow the following re.match pattern "[a-f]".',
                    ["a", "b", "h"],
                ],
                ["2", "Check NA", "'' should be non-empty", ""],
            ],
            "warnings": [
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
            ],
        }

        # THEN we expect a successful response
        assert (
            response.status_code == 200
        ), f"Should be 200 status code. Got {response.status_code}"

        # AND the response should match the expected response
        content = (
            response.content
            if testing_config.use_deployed_schematic_api_server
            else response.data
        ).decode("utf-8")
        content_dict = json.loads(content)
        assert content_dict == expected_contents
