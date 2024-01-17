import os

import pytest

from click.testing import CliRunner

# from schematic import init
from schematic.schemas.commands import schema
from schematic.manifest.commands import manifest
from schematic.configuration.configuration import Configuration

@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""

    return CliRunner()

@pytest.fixture
def data_model_jsonld(helpers):
    data_model_jsonld =helpers.get_data_path("example.model.jsonld")
    yield data_model_jsonld


class TestSchemaCli:
    def assert_expected_file(self, result, output_path):
        extension=os.path.splitext(output_path)[-1].lower()

        if extension==".xlsx":
            expected_substr = f"Find the manifest template using this Excel file path: {output_path}" 
        else: 
            expected_substr = f"Find the manifest template using this CSV file path: {output_path}" 

        assert expected_substr in result.output
        
        # clean up
        try: 
            os.remove(output_path)
        except: 
            pass

    def test_schema_convert_cli(self, runner, helpers):

        data_model_csv_path = helpers.get_data_path("example.model.csv")

        output_path = helpers.get_data_path("example.model.jsonld")

        result = runner.invoke(
            schema, ["convert", data_model_csv_path, "--output_jsonld", output_path]
        )

        assert result.exit_code == 0

        expected_substr = (
            "The Data Model was created and saved to " f"'{output_path}' location."
        )

        assert expected_substr in result.output

    # get manifest by default
    # by default this should download the manifest as a CSV file
    @pytest.mark.google_credentials_needed
    def test_get_example_manifest_default(self, runner, helpers, config: Configuration, data_model_jsonld):
        output_path = helpers.get_data_path("example.Patient.manifest.csv")
        config.load_config("config_example.yml")

        result = runner.invoke(
            manifest, ["--config", config.config_path, "get",  "--data_type", "Patient", "--jsonld", data_model_jsonld]
        )


        assert result.exit_code == 0
        self.assert_expected_file(result, output_path)

    # get manifest as a csv
    # use google drive to export
    @pytest.mark.google_credentials_needed
    def test_get_example_manifest_csv(self, runner, helpers, config: Configuration, data_model_jsonld):
        output_path = helpers.get_data_path("test.csv")
        config.load_config("config_example.yml")

        result = runner.invoke(
            manifest, ["--config", config.config_path, "get",  "--data_type", "Patient", "--jsonld", data_model_jsonld, "--output_csv", output_path]
        )
        assert result.exit_code == 0
        self.assert_expected_file(result, output_path)

    # get manifest as an excel spreadsheet
    @pytest.mark.google_credentials_needed
    def test_get_example_manifest_excel(self, runner, helpers, config: Configuration, data_model_jsonld):
        output_path = helpers.get_data_path("test.xlsx")
        config.load_config("config_example.yml")

        result = runner.invoke(
            manifest, ["--config", config.config_path,  "get",  "--data_type", "Patient", "--jsonld", data_model_jsonld, "--output_xlsx", output_path]
        )

        assert result.exit_code == 0
        self.assert_expected_file(result, output_path)