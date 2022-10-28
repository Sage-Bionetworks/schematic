
import pytest
from api import create_app
import configparser
import json
from flask import jsonify

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
def test_manifest(helpers):
    test_manifest_path = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
    yield test_manifest_path
    

@pytest.fixture
def data_model_jsonld():
    data_model_jsonld ="https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
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


@pytest.mark.schematic_api
class TestManifestOperation:

    @pytest.mark.parametrize("data_type", ["Patient", "all manifests", ["Biospecimen", "Patient"]])
    def test_generate_manifest(self, client, data_model_jsonld, data_type):
        # set dataset
        if data_type == "Patient":
            dataset_id = "syn42171373" #Mock Patient Manifest folder on synapse
        elif data_type == "Biospecimen":
            dataset_id = "syn42171508" #Mock biospecimen manifest folder on synapse
        else: 
            dataset_id = None

        params = {
            "schema_url": data_model_jsonld,
            "asset_view": "syn23643253",
            "title": "Example",
            "data_type": data_type,
            "use_annotations": False,
            "dataset_id": dataset_id
        }

        response = client.get('http://localhost:3001/v1/manifest/generate', query_string=params)
        assert response.status_code == 200
        response_dt = json.loads(response.data)
        assert isinstance(response_dt, list)

        # return three google sheet link
        if data_type == "all manifests":
            assert len(response_dt) == 3
        # only return two links
        elif isinstance(data_type, list):
            assert len(response_dt) == 2
        # return one link
        else: 
            assert len(response_dt) == 1

    def test_populate_manifest(self, client, data_model_jsonld, test_manifest):
        # test manifest
        test_manifest_data = open(test_manifest, "rb")
        
        params = {
            "data_type": "MockComponent",
            "schema_url": data_model_jsonld,
            "title": "Example",
            "csv_file": test_manifest_data
        }

        response = client.get('http://localhost:3001/v1/manifest/generate', query_string=params)

        assert response.status_code == 200
    
    @pytest.mark.parametrize("json_str", [None, '[{"Patient ID": 123, "Sex": "Female", "Year of Birth": "", "Diagnosis": "Healthy", "Component": "Patient", "Cancer Type": "Breast", "Family History": "Breast, Lung"}]'])
    def test_validate_manifest(self, data_model_jsonld, client, json_str, test_manifest):

        params = {
            "schema_url": data_model_jsonld,
        }

        if json_str:
            data_type = "Patient"
            params["json_str"] = json_str
            params["data_type"] = data_type
            response = client.post('http://localhost:3001/v1/model/validate', query_string=params)
            assert response.status_code == 200

        #test_manifest_data = open(test_manifest, "rb")

        #print('test manifest data', test_manifest_data)
        # else: 
        #     data_type = "MockComponent"
        #     params["file_name"] = test_manifest_data
        #     response = client.post('http://localhost:3001/v1/model/validate', query_string=params)
        #     assert response.status_code == 200


        print('params', params)


        #assert response.status_code == 200










