
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
def test_manifest_csv(helpers):
    test_manifest_path = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
    yield test_manifest_path
    
@pytest.fixture
def test_manifest_json(helpers):
    test_manifest_path = helpers.get_data_path("mock_manifests/Example.Patient.manifest.json")
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

    def test_populate_manifest(self, client, data_model_jsonld, test_manifest_csv):
        # test manifest
        test_manifest_data = open(test_manifest_csv, "rb")
        
        params = {
            "data_type": "MockComponent",
            "schema_url": data_model_jsonld,
            "title": "Example",
            "csv_file": test_manifest_data
        }

        response = client.get('http://localhost:3001/v1/manifest/generate', query_string=params)

        assert response.status_code == 200
        response_dt = json.loads(response.data)
    
        # should return a list with one google sheet link 
        assert isinstance(response_dt[0], str)
        assert response_dt[0].startswith("https://docs.google.com/")
    
    @pytest.mark.parametrize("json_str", [None, '[{"Patient ID": 123, "Sex": "Female", "Year of Birth": "", "Diagnosis": "Healthy", "Component": "Patient", "Cancer Type": "Breast", "Family History": "Breast, Lung"}]'])
    def test_validate_manifest(self, data_model_jsonld, client, json_str, test_manifest_csv, test_manifest_json):

        params = {
            "schema_url": data_model_jsonld,
        }

        if json_str:
            params["json_str"] = json_str
            params["data_type"] = "Patient"
            response = client.post('http://localhost:3001/v1/model/validate', query_string=params)
            response_dt = json.loads(response.data)
            assert response.status_code == 200

        else: 
            params["data_type"] = "MockComponent"

            headers = {
            'Content-Type': "multipart/form-data",
            'Accept': "application/json"
            }

            # test uploading a csv file
            response_csv = client.post('http://localhost:3001/v1/model/validate', query_string=params, data={"file_name": (open(test_manifest_csv, 'rb'), "test.csv")}, headers=headers)
            response_dt = json.loads(response_csv.data)
            assert response_csv.status_code == 200
            

            # test uploading a json file
            # change data type to patient since the testing json manifest is using Patient component
            params["data_type"] = "Patient"
            response_json =  client.post('http://localhost:3001/v1/model/validate', query_string=params, data={"file_name": (open(test_manifest_json, 'rb'), "test.json")}, headers=headers)
            response_dt = json.loads(response_json.data)
            assert response_json.status_code == 200

        assert "errors" in response_dt.keys()
        assert "warnings" in response_dt.keys()










