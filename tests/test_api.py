from pathlib import Path
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__),'api'))
from api import create_app
import pytest
import json 
from urllib.parse import urlencode
from werkzeug.datastructures import FileStorage
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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

def get_response(client, url):
    response = client.get(url)
    status_code = response.status_code
    result = json.loads(response.data)
    return status_code, result

def post_request_url(client, url, content):
    response = client.post(url, data={"csv_file":content}, content_type="multipart/form-data")
    status_code = response.status_code
    result = json.loads(response.data)
    return status_code, result

def upload_biospecimen_file():
    biospecimen_manifest = FileStorage(
        stream=open('tests/data/mock_manifests/biospecimen_manifest.csv', 'rb'),
        filename="biospecimen_manifest.csv",
        content_type="text/csv",
    )
    return biospecimen_manifest

def test_generate_manifest(construct_basic_params, client):
    # construct url 
    construct_basic_params['oauth'] = True

    # make api request
    requested_url=generate_manifest_url_response(construct_basic_params)

    # get json object of response
    status_code, json_obj = get_response(client, requested_url)

    # count the number of url in the response and see status code
    assert status_code == 200
    assert len(json_obj)==1

def test_generate_manifest_with_dataset_assetview(client, construct_basic_params):
    # add new params 
    construct_basic_params["dataset_id"] = "syn28268700"
    construct_basic_params["asset_view"] = "syn28559058"
    construct_basic_params['oauth'] = True
    
    # make api request 
    requested_url=generate_manifest_url_response(construct_basic_params)

    # get json object of response
    status_code, json_obj = get_response(client, requested_url)

    # count the number of url in the response
    assert status_code == 200
    assert len(json_obj)==1


def test_populate_manifest(client, construct_basic_params):
    # convert csv file to byte
    files = upload_biospecimen_file()

    # make api request
    requested_url=populate_manifest_url_response(construct_basic_params)

    # make post request    
    status_code, google_sheet_url = post_request_url(client, requested_url, files)

    assert status_code == 200
    assert isinstance(google_sheet_url, str)
    assert google_sheet_url != ''