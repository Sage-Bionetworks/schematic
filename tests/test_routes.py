import pytest

from schematic_api.api import create_app
from schematic_api.api.routes import ManifestGeneration


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




    
    
    
