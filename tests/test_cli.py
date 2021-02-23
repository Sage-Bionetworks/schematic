import os

import pytest

from click.testing import CliRunner

from scripts.rfc_to_data_model import rfc_to_data_model


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""

    return CliRunner()


class TestRfcToDataModelCli:

    def test_rfc_to_data_model_cli(self, runner, config_path, helpers):
        
        rfc_csv_path = helpers.get_data_path("simple.model.csv")
        
        result = runner.invoke(rfc_to_data_model, 
                               [rfc_csv_path, 
                                "--config", config_path])

        assert result.exit_code == 0

        output_path = helpers.get_data_path("simple.model.jsonld")

        expected_substr = (
                          "The Data Model was created and saved to "
                          f"'{output_path}' location."
                        )

        assert expected_substr in result.output
