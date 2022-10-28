import os

import pytest

from click.testing import CliRunner

# from schematic import init
from schematic.schemas.commands import schema
from schematic.utils.google_api_utils import download_creds_file


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""

    return CliRunner()


class TestSchemaCli:
    def test_schema_convert_cli(self, runner, config_path, helpers):

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
