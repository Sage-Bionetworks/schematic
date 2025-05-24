import os
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from schematic.configuration.configuration import Configuration
from schematic.schemas.commands import schema
from tests.conftest import Helpers


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""

    return CliRunner()


@pytest.fixture
def data_model_jsonld(helpers):
    data_model_jsonld = helpers.get_data_path("example.model.jsonld")
    yield data_model_jsonld


@pytest.mark.single_process_execution
class TestSchemaCli:
    # TODO: change url target to develop branch
    @pytest.mark.parametrize(
        "data_model_location",
        [
            "example.model.jsonld",
            "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/f49215d5a7968ceffde855907fa0128a309768fb/tests/data/example.model.jsonld",
        ],
        ids=["local", "remote"],
    )
    @pytest.mark.parametrize(
        "data_type",
        ["MockComponent", None],
        ids=["component specified", "all components"],
    )
    @pytest.mark.parametrize(
        "output_directory",
        ["test/jsonschema_output/", None],
        ids=["output directory specified", "no output directory specified"],
    )
    def test_json_schema_generation(
        self,
        runner: CliRunner,
        helpers: Helpers,
        config: Configuration,
        data_model_location: str,
        output_directory: str,
        data_type: str,
    ):
        if data_model_location.startswith("example"):
            data_model_location = helpers.get_data_path(data_model_location)

        config.load_config("config_example.yml")

        result = runner.invoke(
            schema,
            [
                "generate-jsonschema",
                "--data_model_location",
                data_model_location,
                "--output_directory",
                output_directory,
                "--data_type",
                data_type,
            ],
        )
        assert result.exit_code == 0
