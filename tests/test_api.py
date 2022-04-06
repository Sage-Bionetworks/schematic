from pathlib import Path
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__),'api'))
from api import create_app
import pytest
import json 
from urllib.parse import urlencode

@pytest.fixture
def app():
    app = create_app()
    yield app

@pytest.fixture
def client(app):
    return app.test_client()

@pytest.fixture
def construct_basic_params():
    url_params={
        'schema_url': 'https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld',
        'title': 'Example',
        'data_type': 'Biospecimen',
    }

    return url_params

def generate_manifest_url_response(params):
    url = "http://localhost:3001/v1/manifest/generate?%s" % (urlencode(params))

    return url

def populate_manifest_url_response(params):
    url = "http://localhost:3001/v1/manifest/populate?%s" % (urlencode(params))

    return url

def get_request_url(client, url):
    response = client.get(url)
    json_obj = json.loads(response.data)
    return json_obj

def post_request_url(client, url, files):
    response = client.post(url, data=files, headers={"Content-Type": "multipart/form-data"})
    json_obj = json.loads(response.data)
    return json_obj

def upload_biospecimen_file():
    files = {'upload_file': open('tests/data/mock_manifests/biospecimen_manifest.csv','rb')}
    return files

# def test_generate_manifest(construct_basic_params, client):
#     # call response
#     construct_basic_params['oauth'] = True
#     requested_url=generate_manifest_url_response(construct_basic_params)

#     # get json object of response
#     json_obj = get_request_url(client, requested_url)

#     # count the number of url in the response
#     assert len(json_obj)==1

# def test_generate_manifest_with_dataset_assetview(client, construct_basic_params):
#     # add new params
#     construct_basic_params["dataset_id"] = "syn28268700"
#     construct_basic_params["asset_view"] = "syn28559058"
#     construct_basic_params['oauth'] = True
    
#     # call response
#     requested_url=generate_manifest_url_response(construct_basic_params)

#     # get json object of response
#     json_obj = get_request_url(client, requested_url)

#     # count the number of url in the response
#     assert len(json_obj)==1

def test_populate_manifest(client, construct_basic_params):
    # call response
    files = upload_biospecimen_file()
    requested_url=populate_manifest_url_response(construct_basic_params)

    # get json object of response
    json_obj = post_request_url(client, requested_url, files)

    #print('post result', json_obj)

    assert len(json_obj)==1