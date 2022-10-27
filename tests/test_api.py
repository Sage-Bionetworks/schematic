
import os
import pytest
from run_api import create_app
from pathlib import Path

@pytest.fixture
def client(config):
    app = create_app()
    app.config['SCHEMATIC_CONFIG'] = config
    with app.app_context():
        with app.test_client() as client:
            yield client

@pytest.fixture
def data_model_jsonld(helpers):
    data_model_jsonld =helpers.get_data_path("example.model.jsonld")
    yield data_model_jsonld


@pytest.fixture
def get_token(config):
    synapse_config_path = config["synapse_config"]
    with open(synapse_config_path) as f:
        text = f.read()
        print(text)



# def test_populate_manifest_route(client, data_model_json_ld):
#     client.get('/manifest/populate')

# @pytest.fixture
# def config_env(config):
#     os.environ['SCHEMATIC_CONFIG'] = config
#     return os.environ.get('SCHEMATIC_CONFIG')

# @pytest.fixture
# def mock_manifest

# class TestJsonConverter:
#     def test_read_json():

