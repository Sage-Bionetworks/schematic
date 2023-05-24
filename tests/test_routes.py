import pytest
import pandas as pd
import logging

from schematic_api.api import create_app
from schematic_api.api.routes import ManifestGeneration
from schematic.manifest.generator import ManifestGenerator

from unittest.mock import patch


@pytest.fixture(scope="class")
def app():
    app = create_app()
    yield app


@pytest.fixture(scope="class")
def data_model_jsonld():
    data_model_jsonld = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
    yield data_model_jsonld


def test_manifest_generation_load_config(app, config_path, data_model_jsonld):
    """Test if asset_view gets updated"""
    app.config["SCHEMATIC_CONFIG"] = config_path
    mg = ManifestGeneration(
        schema_url=data_model_jsonld,
        data_type=["test_data_type"],
        asset_view="syn123",
    )
    config = mg._load_config_(app=app)

    assert config["synapse"]["master_fileview"] == "syn123"


def test_check_dataset_match_data_type(data_model_jsonld):
    """Test if function could raise an error when number of data types do not match number of dataset ids"""
    with pytest.raises(ValueError):
        mg = ManifestGeneration(
            schema_url=data_model_jsonld,
            data_type=["Biospecimen", "Patient"],
            asset_view="syn123",
            dataset_id=["syn1234"],
        )


def test_check_if_asset_view_valid(data_model_jsonld):
    with pytest.raises(ValueError):
        mg = ManifestGeneration(
            schema_url=data_model_jsonld,
            data_type=["Biospecimen", "Patient"],
            asset_view="invalid asset view id",
            dataset_id=["syn1234"],
        )


def test_check_if_dataset_id_valid(data_model_jsonld):
    with pytest.raises(ValueError):
        mg = ManifestGeneration(
            schema_url=data_model_jsonld,
            data_type=["Biospecimen", "Patient"],
            asset_view="syn1234",
            dataset_id=["invalid dataset id"],
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
        schema_url=data_model_jsonld, data_type=[mock_datatype], title=test_title
    )
    title = mg._get_manifest_title(single_data_type=mock_datatype)
    assert title == expected_title


@pytest.mark.parametrize("output_format", ["google_sheet", "dataframe"])
def test_create_single_manifest(output_format, data_model_jsonld):
    """create_single_manifest wraps around manifest_generator.get_manifest function.
    TODO: add test for output_format="excel"
    """
    mg = ManifestGeneration(
        schema_url=data_model_jsonld, data_type=["Patient"], output_format=output_format
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
        schema_url=data_model_jsonld, data_type=data_type, output_format=output_format
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
            data_type_lst=data_type,
            dataset_id=["syn1234", "syn1235"],
        )

        # if the output is google sheet or dataframe, the number of items that get returned should match the number of items in data_type
        if output_format != "excel":
            assert len(result) == len(data_type)
            assert isinstance(result, list)

        else:
            with caplog.at_level(logging.WARNING):
                # return warning messages when there are multiple data types and the output format is excel
                if len(data_type) > 1:
                    assert (
                        f"Currently we do not support returning multiple files as Excel format at once. Only manifest generated by using dataset id {data_type[0]} would get returned with title Example.{data_type[0]}.manifest"
                        in caplog.text
                    )
