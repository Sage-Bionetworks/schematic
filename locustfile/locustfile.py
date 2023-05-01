from locust import HttpUser, task
import urllib.parse
import os

class HelloWorldUser(HttpUser):
    @task
    def swagger_ui(self):
        self.client.get("/v1/ui")
        with self.client.get('/v1/ui', catch_response=True) as response:
            if response.text != 'Success':
                response.failure('The server is down.')

    @task
    def manifest_generate_dynamic(self):
        if "SYNAPSE_AUTH_TOKEN" in os.environ:
            token = os.environ["SYNAPSE_AUTH_TOKEN"]
        else:
            token = os.environ["TOKEN"]

        base_url = '/v1/manifest/generate?'
        params = {'schema_url': 'https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld', 'input_token': token, 'data_type': 'Patient', 'output_format':'google_sheet'}
        url = base_url + urllib.parse.urlencode(params)
        self.client.get(url)

    
    

    