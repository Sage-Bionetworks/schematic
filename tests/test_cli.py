import os

import pytest

from click.testing import CliRunner

# from schematic import init
from schematic.schemas.commands import schema
from schematic.utils.google_api_utils import download_creds_file
from schematic.manifest.commands import manifest

@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""

    return CliRunner()


@pytest.fixture
def data_model_jsonld(helpers):
    data_model_jsonld =helpers.get_data_path("example.model.jsonld")
    yield data_model_jsonld


class TestSchemaCli:
    def test_schema_convert_cli(self, runner, helpers, data_model_jsonld):

        data_model_csv_path = helpers.get_data_path("example.model.csv")

        result = runner.invoke(
            schema, ["convert", data_model_csv_path, "--output_jsonld", data_model_jsonld]
        )

        assert result.exit_code == 0

        expected_substr = (
            "The Data Model was created and saved to " f"'{data_model_jsonld}' location."
        )

        assert expected_substr in result.output

    # get manifest by default
    @pytest.mark.google_credentials_needed
    def test_get_manifest_cli(self, runner, helpers, config, data_model_jsonld):
        output_path = helpers.get_data_path("example.Patient.manifest.csv")

        result = runner.invoke(
            manifest, ["--config", config.CONFIG_PATH, "get",  "--data_type", "Patient", "--jsonld", data_model_jsonld]
        )


        assert result.exit_code == 0

        expected_substr_one = "Find the manifest template using this CSV file path:" 

        assert expected_substr_one in result.output
        assert output_path in result.output

        # clean up
        helpers.clean_up_file(helpers, file_name=output_path)
    
    # get manifest as a csv
    # use google drive to export
    @pytest.mark.google_credentials_needed
    def test_get_manifest_csv(self, runner, helpers, config, data_model_jsonld):
        output_path = helpers.get_data_path("test.csv")

        result = runner.invoke(
            manifest, ["--config", config.CONFIG_PATH, "get",  "--data_type", "Patient", "--jsonld", data_model_jsonld, "--output_csv", output_path]
        )
        assert result.exit_code == 0
        assert output_path in result.output

        # clean up
        helpers.clean_up_file(helpers, file_name=output_path)

    # get manifest as an excel spreadsheet
    @pytest.mark.google_credentials_needed
    def test_get_manifest_excel(self, runner, helpers, config, data_model_jsonld):
        output_path = helpers.get_data_path("test.xlsx")

        result = runner.invoke(
            manifest, ["--config", config.CONFIG_PATH,  "get",  "--data_type", "Patient", "--jsonld", data_model_jsonld, "--output_xlsx", output_path]
        )

        assert result.exit_code == 0
        assert output_path in result.output

        # clean up
        helpers.clean_up_file(helpers, file_name=output_path)