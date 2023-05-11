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

def test_manifest_generation_load_config(app, config_path):
    """Test if asset_view gets updated
    """
    app.config['SCHEMATIC_CONFIG'] = config_path
    mg = ManifestGeneration(schema_url="example schema url", data_type=["test_data_type"], asset_view="test_asset_view")
    config = mg._load_config_(app=app)
    
    assert config["synapse"]["master_fileview"] == "test_asset_view"

@pytest.mark.parametrize("data_type_lst", [["Biospecimen", "Patient"], ["Biospecimen"]])
def test_check_dataset_match_data_type(data_type_lst):
    """Test if function could raise an error when number of data types do not match number of dataset ids
    """
    test_dataset_id=["test_dataset_id1", "test_dataset_id2"]
    mg = ManifestGeneration(schema_url="example schema url", data_type=data_type_lst, asset_view="test_asset_view", dataset_id=test_dataset_id)
    if len(data_type_lst) !=len(test_dataset_id):
        with pytest.raises(ValueError):
            mg._check_dataset_match_datatype_()
    else:
        pass

@pytest.mark.parametrize("test_title, mock_datatype, expected_title", [("Mock", "Biospecimen",  "Mock.Biospecimen.manifest"), (None, "Patient", "Example.Patient.manifest")])
def test_get_manifest_title(test_title, mock_datatype, expected_title):
    """Test if title of manifest gets updated correctly
    """  
    mg = ManifestGeneration(schema_url="example schema url",data_type=[mock_datatype],title=test_title)
    title = mg._get_manifest_title(single_data_type=mock_datatype)
    assert title==expected_title

@pytest.mark.parametrize("output_format", ["google_sheet", "dataframe"])
def test_create_single_manifest(output_format):
    """create_single_manifest wraps around manifest_generator.get_manifest function.
    TODO: add test for output_format="excel"
    """
    mg = ManifestGeneration(schema_url="test_schema_url",data_type=["Patient"], output_format=output_format)

    if output_format == "google_sheet":
        # assume get_manifest correctly returns a google sheet url
        with patch.object(ManifestGenerator, 'get_manifest', return_value="google_sheet_url") as mock_method:
            result=mg.create_single_manifest(single_data_type="Patient", single_dataset_id="mock dataset id")
            assert result == "google_sheet_url"
    elif output_format == "dataframe":
        # assume get_manifest correctly returns a dataframe
        with patch.object(ManifestGenerator, 'get_manifest', return_value=pd.DataFrame()) as mock_method:
            result=mg.create_single_manifest(single_data_type="Patient", single_dataset_id="mock dataset id")
            assert isinstance(result, pd.DataFrame)

# @pytest.mark.parametrize("output_format", ["excel", "google_sheet", "dataframe"])
# def test_generate_manifest_and_collect_outputs(output_format, caplog):
#     if output_format == "excel":
#         """Test if warning messages get triggered when providing multiple data types"""
#         mg = ManifestGeneration(schema_url="test_schema_url",data_type=["Patient", "Biospecimen"], output_format="excel")
#         with patch.object(ManifestGeneration, 'create_single_manifest', return_value="mock_excel_manifest_path") as mock_method:
#             result=mg.generate_manifest_and_collect_outputs(data_type_lst=["test_data_type_one", "test_data_type_two"], dataset_id=["test_dataset_id"])
#             # assert warning message
#             with caplog.at_level(logging.WARNING):
#                 result=mg.generate_manifest_and_collect_outputs(data_type_lst=["test_data_type_one", "test_data_type_two"], dataset_id=["test_dataset_id"])
#                 assert 'Currently we do not support returning multiple files as Excel format at once' in caplog.text

#     else:
#         """Test if outputs get collected in a list"""
#         mg = ManifestGeneration(schema_url="test_schema_url",data_type=["Patient", "Biospecimen"], output_format=output_format)
#         with patch.object(ManifestGeneration, 'create_single_manifest', return_value="test_google_sheet") as mock_method:
#             result=mg.generate_manifest_and_collect_outputs(data_type_lst=["test_data_type_one", "test_data_type_two"], dataset_id=["test_dataset_id"])

            



