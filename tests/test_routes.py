import pytest
from schematic_api.api.routes import ManifestGeneration
from schematic_api.api import create_app

@pytest.fixture(scope="class")
def app():
    app = create_app()
    yield app

def test_manifest_generation_load_config(app, config_path):
    app.config['SCHEMATIC_CONFIG'] = config_path
    mg = ManifestGeneration(schema_url="example schema url", data_type=["test_data_type"], asset_view="test_asset_view")
    config = mg._load_config_(app=app)
    
    assert config["synapse"]["master_fileview"] == "test_asset_view"




    
    
    
