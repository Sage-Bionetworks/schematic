import os

import pytest

from click.testing import CliRunner

from schematic.schemas.commands import schema


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""

    return CliRunner()


class SchemaCli:

    def test_schema_convert_cli(self, runner, config_path, helpers):

        rfc_csv_path = helpers.get_data_path("simple.model.csv")

        result = runner.invoke(schema, 
                               ["--config", config_path, 
                               "convert", rfc_csv_path])

        assert result.exit_code == 0

        output_path = helpers.get_data_path("simple.model.jsonld")

        expected_substr = (
                          "The Data Model was created and saved to "
                          f"'{output_path}' location."
                        )

        assert expected_substr in result.output
        