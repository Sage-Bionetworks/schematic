"""
This module is responsible for running through the "Manifest Validation" portion of
the schematic API test plan found here: <https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/3055779846/Schematic+API+test+plan>.
"""

import os
import json
import pytest
import requests
from tests.conftest import Helpers, testing_config, ConfigurationForTesting


EXAMPLE_SCHEMA_URL = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
HEADERS = {"Authorization": f"Bearer {syn_token}"}


class TestManifestValidation:
    @pytest.fixture(scope="module")
    def setup_api(testing_config: ConfigurationForTesting) -> str:
        url = testing_config.schematic_api_server_url

        # Make a request to the API and make sure it is responsive
        response = requests.get(url)
        assert response.status_code == 200, f"Failed to connect to API: {response.text}"
        return url

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

        Arguments:
            input_data_type: The data type of the manifest
            input_file_name: The name of the manifest file
            setup_api: A pytest fixture returning the API endpoint to use
            helpers: The Helpers object

        Returns:
            None
        """
        # GIVEN the manifest validation endpoint and parameters
        url = os.path.join(setup_api, "model/validate")
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": input_data_type,
            "data_model_labels": "class_label",
            "restrict_rules": False,
        }

        # AND a valid file
        file_path = helpers.get_data_path(input_file_name)
        with open(file_path, "rb") as file_obj:
            files = {"file_name": (os.path.basename(file_path), file_obj, "text/csv")}

            # WHEN we make a POST request to validate the file
            response = requests.post(url, params=params, files=files)

            # THEN we expect a successful response
            assert (
                response.status_code == 200
            ), f"Got status code: {response.status_code}. Expected '200'."

            # AND with expected keys in the json
            response_json = response.json()
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

        Arguments:
            input_data_type: The data type of the manifest
            input_file_name: The name of the manifest file
            setup_api: A pytest fixture returning the API endpoint to use
            helpers: The Helpers object

        Returns:
            None
        """
        # GIVEN the manifest validation endpoint and parameters
        url = os.path.join(setup_api, "model/validate")
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": input_data_type,
            "data_model_labels": "class_label",
            "restrict_rules": False,
        }

        # AND an invalid file
        file = helpers.get_data_path(input_file_name)
        with open(file, "rb") as file_obj:
            files = {"file_name": (os.path.basename(file), file_obj, "text/csv")}

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

    def test_cross_manifest_validation_with_no_target(
        self, setup_api, helpers: Helpers
    ) -> None:
        """
        Test that the manifest validation API returns warnings when cross validation is triggered
        with no target provided.

        Arguments:
            setup_api: A pytest fixture returning the API endpoint to use
            helpers: The Helpers object

        Returns:
            None

        """
        # GIVEN the manifest validation endpoint and parameters
        url = os.path.join(setup_api, "model/validate")
        params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": "MockComponent",
            "data_model_labels": "class_label",
            "restrict_rules": False,
            "asset_view": "syn63596704",
        }

        # AND a manifest that triggers cross-manifest validation rules
        input_file_name = "mock_manifests/MockComponent-cross-manifest-1.csv"
        file_path = helpers.get_data_path(input_file_name)
        with open(file_path, "rb") as file_obj:
            files = {
                "file_name": (
                    os.path.basename(file_path),
                    file_obj,
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

            # AND we make a POST request to validate the file
            response = requests.post(url, headers=HEADERS, params=params, files=files)
            print(response.content)

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

    def test_cross_manifest_validation_with_target(
        self, setup_api, helpers: Helpers
    ) -> None:
        """
        Test that the manifest validation API returns warnings when a manifest target is provided.

        Arguments:
            setup_api: A pytest fixture returning the API endpoint to use
            helpers: The Helpers object

        Returns:
            None

        """
        # WHEN a manifest file has been uploaded to the Synapse project
        submit_url = os.path.join(setup_api, "model/submit")
        submit_params = {
            "schema_url": EXAMPLE_SCHEMA_URL,
            "data_type": "MockComponent",
            "dataset_id": "syn63582792",
            "data_model_labels": "class_label",
            "restrict_rules": False,
            "asset_view": "syn63596704",
        }
        submit_file = "mock_manifests/MockComponent-cross-manifest-1.csv"
        submit_file_path = helpers.get_data_path(submit_file)
        with open(submit_file_path, "rb") as file_obj:
            submit_files = {
                "file_name": (
                    os.path.basename(submit_file_path),
                    file_obj,
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
        url = os.path.join(setup_api, "model/validate")
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
        with open(input_file_path, "rb") as file_obj:
            files = {
                "file_name": (
                    os.path.basename(input_file),
                    file_obj,
                    "text/csv",
                )
            }

            # AND we make a POST request to validate the file
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

        # TODO: Turn this into a class attribute in case we need the Synapse instance
        # in other places.
        import synapseclient

        syn = synapseclient.login()

        # WHEN any previous files are deleted
        files_to_delete = syn.getChildren("syn63582792")
        for file_to_delete in files_to_delete:
            syn.delete(file_to_delete["id"])

    def test_manifest_validation_with_rule_combination(
        self, setup_api, helpers: Helpers
    ) -> None:
        """
        Test that the manifest validation API returns the expected warnings and errors when
        simple rule combination validation rules are triggered.

        Arguments:
            setup_api: A pytest fixture returning the API endpoint to use
            helpers: The Helpers object

        Returns:
            None

        """
        # GIVEN the manifest validation endpoint and parameters
        url = os.path.join(setup_api, "model/validate")
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
        with open(input_file_path, "rb") as file_obj:
            files = {
                "file_name": (
                    os.path.basename(input_file_path),
                    file_obj,
                    "text/csv",
                )
            }

            # AND we make a POST request to validate the file
            response = requests.post(url, headers=HEADERS, params=params, files=files)

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
            content = response.content.decode("utf-8")
            content_dict = json.loads(content)
            assert content_dict == expected_contents
