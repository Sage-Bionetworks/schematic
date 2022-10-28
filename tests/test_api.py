
import pytest
from api import create_app
import configparser
import json

'''
To run the tests, you have to keep API running locally first by doing `python3 run_api.py`
'''

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
@pytest.mark.parametrize("return_type", ["json", "csv"])
def test_get_storage_assets_tables(client, syn_token, return_type):
    params = {
        "input_token": syn_token,
        "asset_view": "syn23643253",
        "return_type": return_type
    }

    response = client.get('http://localhost:3001/v1/storage/assets/tables', query_string=params)

    assert response.status_code == 200

    response_dt = json.loads(response.data)

    if return_type == "json":
        assert isinstance(response_dt, str)
    else:
        assert response_dt.endswith("file_view_table.csv")
