
import os
import pytest
from api import create_app
from pathlib import Path
import configparser

@pytest.fixture
def app():
    app = create_app()
    return app

@pytest.fixture
def client(app, config_path):
    app.config['SCHEMATIC_CONFIG'] = config_path

    with app.test_client() as client:
        yield client

@pytest.fixture
def data_model_jsonld(helpers):
    data_model_jsonld =helpers.get_data_path("example.model.jsonld")
    yield data_model_jsonld


@pytest.fixture
def syn_token(config):
    synapse_config_path = config.SYNAPSE_CONFIG_PATH
    config_parser = configparser.ConfigParser()
    config_parser.read(synapse_config_path)
    token = config_parser["authentication"]["authtoken"]
    yield token

@pytest.mark.schematic_api
def test_get_storage_assets_tables(client, syn_token):
    params = {
        "input_token": syn_token,
        "asset_view": "syn23643253",
        "return_type": "json"
    }

    response = client.get('http://localhost:3001/v1/storage/assets/tables', query_string=params)

    assert response.status_code == 200

