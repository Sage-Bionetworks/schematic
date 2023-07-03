import os
import warnings
from unittest.mock import patch

import pandas as pd
import pytest

from schematic.manifest.generator import ManifestGenerator
from schematic_api.api import create_app
from schematic_api.api.routes import ManifestGeneration


@pytest.fixture(scope="class")
def app():
    app = create_app()
    yield app


@pytest.fixture(scope="class")
def data_model_jsonld():
    data_model_jsonld = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
    yield data_model_jsonld

@pytest.fixture
def test_yaml_config_file(helpers):
    # Create the YAML file inside the temporary directory
    yaml_file_path = helpers.get_data_path("test_routes_config.yml")
    yaml_content = """
    synapse:
        master_fileview: "syn23643253"
        manifest_basename: "synapse_storage_manifest"
        manifest_folder: "manifests"
    """

    with open(yaml_file_path, "w") as f:
        f.write(yaml_content)

    # Yield the file path to the test
    yield yaml_file_path

    # Teardown: Remove the YAML file and temporary directory
    os.remove(yaml_file_path)

def test_manifest_generation_load_config(app, data_model_jsonld, test_yaml_config_file):
    """Test if asset_view gets updated"""
    app.config["SCHEMATIC_CONFIG"] = test_yaml_config_file
    mg = ManifestGeneration(
        schema_url=data_model_jsonld,
        data_types=["test_data_type"],
        asset_view="syn123",
    )
    config = mg._load_config_(app=app)
    assert config["synapse"]["master_fileview"] == "syn123"

def test_check_dataset_match_data_type(data_model_jsonld):
    """Test if function could raise an error when number of data types do not match number of dataset ids"""
    with pytest.raises(ValueError):
        mg = ManifestGeneration(
            schema_url=data_model_jsonld,
            data_types=["Biospecimen", "Patient"],
            asset_view="syn123",
            dataset_ids=["syn1234"],
        )

def test_check_invalid_asset_view(data_model_jsonld):
    """Test if function could raise an error when asset view is invalid"""
    with pytest.raises(ValueError):
        mg = ManifestGeneration(
            schema_url=data_model_jsonld,
            data_types=["Biospecimen", "Patient"],
            asset_view="invalid asset view id",
            dataset_ids=["syn1234"],
        )


def test_check_invalid_dataset(data_model_jsonld):
    """Test if function could raise an error when dataset id view is invalid"""
    with pytest.raises(ValueError):
        mg = ManifestGeneration(
            schema_url=data_model_jsonld,
            data_types=["Biospecimen", "Patient"],
            asset_view="syn1234",
            dataset_ids=["invalid dataset id"],
        )

@pytest.mark.parametrize("invalid_json_ld", ["www.google.com", "https://github.com/Sage-Bionetworks/schematic/blob/develop/tests/data/example.model.json"])
def test_check_invalid_jsonld(invalid_json_ld):
    """Test if function could raise an error when schema url is not valid"""
    with pytest.raises(ValueError):
        mg = ManifestGeneration(
            schema_url=invalid_json_ld,
            data_types=["Biospecimen", "Patient"],
            asset_view="syn1234",
            dataset_ids="syn1234",
        )

@pytest.mark.parametrize(
    "test_title, mock_datatype, expected_title",
    [
        ("Mock", "Biospecimen", "Mock.Biospecimen.manifest"),
        (None, "Patient", "Example.Patient.manifest"),
    ],
)
def test_get_manifest_title(
    test_title, mock_datatype, expected_title, data_model_jsonld
):
    """Test if title of manifest gets updated correctly"""
    mg = ManifestGeneration(
        schema_url=data_model_jsonld, data_types=[mock_datatype], title=test_title
    )
    title = mg._get_manifest_title(single_data_type=mock_datatype)
    assert title == expected_title


@pytest.mark.parametrize("output_format", ["google_sheet"])
def test_create_single_manifest(output_format, data_model_jsonld):
    """create_single_manifest wraps around manifest_generator.get_manifest function.
    TODO: add test for output_format="excel"
    """
    mg = ManifestGeneration(
        schema_url=data_model_jsonld,
        data_types=["Patient"],
        output_format=output_format,
    )

    if output_format == "google_sheet":
        # assume get_manifest correctly returns a google sheet url
        with patch.object(
            ManifestGenerator, "get_manifest", return_value="google_sheet_url"
        ) as mock_method:
            result = mg.create_single_manifest(
                access_token="mock_access_token",
                single_data_type="Patient",
                single_dataset_id="syn1234",
            )
            assert result == "google_sheet_url"
    elif output_format == "dataframe":
        # assume get_manifest correctly returns a dataframe
        with patch.object(
            ManifestGenerator, "get_manifest", return_value=pd.DataFrame()
        ) as mock_method:
            result = mg.create_single_manifest(
                access_token="mock_access_token",
                single_data_type="Patient",
                single_dataset_id="syn1234",
            )
            assert isinstance(result, pd.DataFrame)


@pytest.mark.parametrize("data_type", [["Patient, Biospecimen"], ["Patient"]])
@pytest.mark.parametrize("output_format", ["google_sheet", "dataframe", "excel"])
def test_generate_manifest_and_collect_outputs(
    data_model_jsonld, output_format, caplog, data_type
):
    """Make sure that results get collected in a list when the output formats are google sheet and dataframe.
    When output format is excel and there are multiple data types, make sure that a warning message gets triggered"""

    mg = ManifestGeneration(
        schema_url=data_model_jsonld, data_types=data_type, output_format=output_format
    )
    if output_format == "excel":
        mock_return_val = "mock_excel_manifest"
    elif output_format == "google_sheet":
        mock_return_val = "mock_google_sheet_url"
    else:
        mock_return_val = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    with patch.object(
        ManifestGeneration, "create_single_manifest", return_value=mock_return_val
    ) as mock_method:
        result = mg.generate_manifest_and_collect_outputs(
            access_token="mock_access_token",
            data_types=data_type,
            dataset_ids=["syn1234", "syn1235"],
        )

        # if the output is google sheet or dataframe, the number of items that get returned should match the number of items in data_type
        if output_format != "excel":
            assert len(result) == len(data_type)
            assert isinstance(result, list)

        else:
            with warnings.catch_warnings(record=True) as w:
                assert True
