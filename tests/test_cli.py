import json
import os
import pickle
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from schematic.configuration.configuration import Configuration
from schematic.manifest.commands import manifest
from schematic.models.commands import model
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


class TestSchemaCli:
    def assert_expected_file(self, result, output_path):
        extension = os.path.splitext(output_path)[-1].lower()

        if extension == ".xlsx":
            expected_substr = (
                f"Find the manifest template using this Excel file path: {output_path}"
            )
        else:
            expected_substr = (
                f"Find the manifest template using this CSV file path: {output_path}"
            )

        assert expected_substr in result.output

        # clean up
        try:
            os.remove(output_path)
        except:
            pass

    @pytest.mark.parametrize(
        "output_path",
        [
            # Test case 1: pickle file passed to output_path
            "tests/data/example.model.pickle",
            # Test case 2: jsonld file passed to output_path
            "tests/data/example.model.jsonld",
        ],
        ids=["output_path_pickle", "output_path_jsonld"],
    )
    @pytest.mark.parametrize(
        "output_type",
        [
            # Test case 1: jsonld passed to output_type
            "jsonld",
            # Test case 2: graph passed to output_type
            "graph",
            # Test case 3: both jsonld and graph are created
            "all",
        ],
        ids=["output_type_jsonld", "output_type_graph", "output_type_all"],
    )
    def test_schema_convert_cli(self, runner, output_path, output_type):
        model = "tests/data/example.model.csv"
        label_type = "class_label"
        expected = 0

        result_one = runner.invoke(schema, ["convert", model])

        assert result_one.exit_code == expected
        # check output_path file is created then remove it
        assert os.path.exists(output_path)

        result_two = runner.invoke(
            schema, ["convert", model, "--output_path", output_path]
        )

        assert result_two.exit_code == expected
        # check output_path file is created then remove it
        assert os.path.exists(output_path)

        result_three = runner.invoke(
            schema, ["convert", model, "--output_type", output_type]
        )

        assert result_three.exit_code == expected
        # check output_path file is created then remove it
        assert os.path.exists(output_path)

        result_four = runner.invoke(
            schema,
            [
                "convert",
                model,
                "--output_type",
                output_type,
                "--output_jsonld",
                output_path,
            ],
        )

        assert result_four.exit_code == expected
        # check output_path file is created then remove it
        assert os.path.exists(output_path)

        result = runner.invoke(
            schema,
            [
                "convert",
                model,
                "--output_jsonld",
                output_path,
                "--data_model_labels",
                label_type,
            ],
        )

        assert result.exit_code == expected
        # check output_path file is created then remove it
        assert os.path.exists(output_path)

        result_five = runner.invoke(
            schema,
            [
                "convert",
                model,
                "--output_jsonld",
                "tests/data/example.model.pickle",
                "--output_path",
                "tests/data/example.model.pickle",
            ],
        )

        assert result_five.exit_code == expected
        # check output_path file is created then remove it
        assert os.path.exists(output_path)

        result_six = runner.invoke(
            schema, ["convert", model, "--output_jsonld", "", "--output_path", ""]
        )

        assert result_six.exit_code == expected
        # check output_path file is created then remove it
        assert os.path.exists(output_path)

    # get manifest by default
    # by default this should download the manifest as a CSV file
    @pytest.mark.google_credentials_needed
    def test_get_example_manifest_default(
        self, runner, helpers, config: Configuration, data_model_jsonld
    ):
        output_path = helpers.get_data_path("example.Patient.manifest.csv")
        config.load_config("config_example.yml")

        result = runner.invoke(
            manifest,
            [
                "--config",
                config.config_path,
                "get",
                "--data_type",
                "Patient",
                "--path_to_data_model",
                data_model_jsonld,
            ],
        )

        assert result.exit_code == 0
        self.assert_expected_file(result, output_path)

    # get manifest as a csv
    # use google drive to export
    @pytest.mark.google_credentials_needed
    def test_get_example_manifest_csv(
        self, runner, helpers, config: Configuration, data_model_jsonld
    ):
        output_path = helpers.get_data_path("test.csv")
        config.load_config("config_example.yml")

        result = runner.invoke(
            manifest,
            [
                "--config",
                config.config_path,
                "get",
                "--data_type",
                "Patient",
                "--path_to_data_model",
                data_model_jsonld,
                "--output_csv",
                output_path,
            ],
        )
        assert result.exit_code == 0
        self.assert_expected_file(result, output_path)

    # get manifest as an excel spreadsheet
    @pytest.mark.google_credentials_needed
    def test_get_example_manifest_excel(
        self, runner, helpers, config: Configuration, data_model_jsonld
    ):
        output_path = helpers.get_data_path("test.xlsx")
        config.load_config("config_example.yml")

        result = runner.invoke(
            manifest,
            [
                "--config",
                config.config_path,
                "get",
                "--data_type",
                "Patient",
                "--path_to_data_model",
                data_model_jsonld,
                "--output_xlsx",
                output_path,
            ],
        )

        assert result.exit_code == 0
        self.assert_expected_file(result, output_path)

    @pytest.mark.parametrize("with_annotations", [True, False])
    def test_submit_file_based_manifest(
        self,
        runner: CliRunner,
        helpers: Helpers,
        with_annotations: bool,
        config: Configuration,
    ) -> None:
        manifest_path = helpers.get_data_path("mock_manifests/test_BulkRNAseq.csv")
        config.load_config("config_example.yml")
        config.synapse_master_fileview_id = "syn1234"

        if with_annotations:
            annotation_opt = "-fa"
        else:
            annotation_opt = "-no-fa"

        with patch("schematic.models.metadata.MetadataModel.submit_metadata_manifest"):
            result = runner.invoke(
                model,
                [
                    "-c",
                    config.config_path,
                    "submit",
                    "-mrt",
                    "file_only",
                    "-d",
                    "syn12345",
                    "-vc",
                    "BulkRNA-seqAssay",
                    "-mp",
                    manifest_path,
                    annotation_opt,
                ],
            )

            assert result.exit_code == 0
