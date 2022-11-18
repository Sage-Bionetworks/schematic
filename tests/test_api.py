
import pytest
from api import create_app
import configparser
import json
import os
import pandas as pd
import re
from tenacity import retry, TryAgain, RetryError, stop_after_attempt, wait_random_exponential

@pytest.fixture(scope="class")
def app():
    app = create_app()
    yield app

@pytest.fixture(scope="class")
def client(app, config_path):
    app.config['SCHEMATIC_CONFIG'] = config_path

    with app.test_client() as client:
        yield client

@pytest.fixture(scope="class")
def test_manifest_csv(helpers):
    test_manifest_path = helpers.get_data_path("mock_manifests/Example_Test_BulkRNAseq.csv")
    yield test_manifest_path
    
@pytest.fixture(scope="class")
def test_manifest_json(helpers):
    test_manifest_path = helpers.get_data_path("mock_manifests/Example.Patient.manifest.json")
    yield test_manifest_path

@pytest.fixture(scope="class")
def data_model_jsonld():
    data_model_jsonld ="https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
    yield data_model_jsonld

@pytest.fixture(scope="class")
def syn_token(config):
    synapse_config_path = config.SYNAPSE_CONFIG_PATH
    config_parser = configparser.ConfigParser()
    config_parser.read(synapse_config_path)
    # try using synapse access token
    if "SYNAPSE_ACCESS_TOKEN" in os.environ:
        token=os.getenv("SYNAPSE_ACCESS_TOKEN")
    else:
        token = config_parser["authentication"]["authtoken"]
    yield token

@pytest.mark.schematic_api
class TestSynapseStorage:
    @pytest.mark.parametrize("return_type", ["json", "csv"])
    def test_get_storage_assets_tables(self, client, syn_token, return_type):
        params = {
            "input_token": syn_token,
            "asset_view": "syn23643253",
            "return_type": return_type
        }

        response = client.get('http://localhost:3001/v1/storage/assets/tables', query_string=params)

        assert response.status_code == 200

        response_dt = json.loads(response.data)

        # if return type == json, returning json str
        if return_type == "json":
            assert isinstance(response_dt, str)
        # if return type == csv, returning a csv file
        else:
            assert response_dt.endswith("file_view_table.csv")
        # clean up 
        if os.path.exists(response_dt):
            os.remove(response_dt)
        else: 
            pass
    @pytest.mark.parametrize("full_path", [True, False])
    @pytest.mark.parametrize("file_names", [None, "Sample_A.txt"])
    def test_get_dataset_files(self,full_path, file_names, syn_token, client):
        params = {
            "input_token": syn_token,
            "asset_view": "syn23643253",
            "dataset_id": "syn23643250",
            "full_path": full_path,
        }

        if file_names:
            params["file_names"] = file_names
        
        response = client.get('http://localhost:3001/v1/storage/dataset/files', query_string=params)

        assert response.status_code == 200
        response_dt = json.loads(response.data)

        # would show full file path .txt in result
        if full_path:
            if file_names: 
                assert ["syn23643255","schematic - main/DataTypeX/Sample_A.txt"] and ["syn24226530","schematic - main/TestDatasets/TestDataset-Annotations/Sample_A.txt"] and ["syn25057024","schematic - main/TestDatasets/TestDataset-Annotations-v2/Sample_A.txt"] in response_dt
            else: 
                assert   ["syn23643255","schematic - main/DataTypeX/Sample_A.txt"] in response_dt
        else: 
            if file_names: 
                assert ["syn23643255","Sample_A.txt"] and ["syn24226530","Sample_A.txt"] and ["syn25057024","Sample_A.txt"] in response_dt
            else: 
                assert ["syn25705259","Boolean Test"] and ["syn23667202","DataTypeX_table"] in response_dt
        
    def test_get_storage_project_dataset(self, syn_token, client):
        params = {
        "input_token": syn_token,
        "asset_view": "syn23643253",
        "project_id": "syn26251192"
        }

        response = client.get("http://localhost:3001/v1/storage/project/datasets", query_string = params)
        assert response.status_code == 200
        response_dt = json.loads(response.data)
        assert ["syn26251193","Issue522"] in response_dt

    def test_get_storage_project_manifests(self, syn_token, client):

        params = {
        "input_token": syn_token,
        "asset_view": "syn23643253",
        "project_id": "syn30988314"
        }

        response = client.get("http://localhost:3001/v1/storage/project/manifests", query_string=params)

        assert response.status_code == 200

    def test_get_storage_projects(self, syn_token, client):

        params = {
        "input_token": syn_token,
        "asset_view": "syn23643253"
        }

        response = client.get("http://localhost:3001/v1/storage/projects", query_string = params)

        assert response.status_code == 200
    

        


@pytest.mark.schematic_api
class TestMetadataModelOperation:
    @pytest.mark.parametrize("as_graph", [True, False]) 
    def test_component_requirement(self, client, data_model_jsonld, as_graph):
        params = {
            "schema_url": data_model_jsonld,
            "source_component": "BulkRNA-seqAssay", 
            "as_graph": as_graph
        }

        response = client.get("http://localhost:3001/v1/model/component-requirements", query_string = params)

        assert response.status_code == 200

        response_dt = json.loads(response.data)

        if as_graph:
            assert response_dt == [['Biospecimen','Patient'],['BulkRNA-seqAssay','Biospecimen']]
        else: 
            assert response_dt == ['Patient','Biospecimen','BulkRNA-seqAssay']


@pytest.mark.schematic_api
class TestSchemaExplorerOperation:
    @pytest.mark.parametrize("strict_camel_case", [True, False]) 
    def test_get_property_label_from_display_name(self, client, data_model_jsonld, strict_camel_case):
        params = {
            "schema_url": data_model_jsonld,
            "display_name": "mocular entity",
            "strict_camel_case": strict_camel_case
        }

        response = client.get("http://localhost:3001/v1/explorer/get_property_label_from_display_name", query_string = params)
        assert response.status_code == 200

        response_dt = json.loads(response.data)

        if strict_camel_case:
            assert response_dt == "mocularEntity"
        else:
            assert response_dt == "mocularentity"

    def test_get_schema(self, client, data_model_jsonld):
        params = {
            "schema_url": data_model_jsonld
        }
        response = client.get("http://localhost:3001/v1/schemas/get/schema", query_string = params)

        response_dt = response.data
        assert response.status_code == 200
        assert os.path.exists(response_dt)

        # if path exists, remove the file
        if os.path.exists(response_dt):
            os.remove(response_dt)



@pytest.mark.schematic_api
class TestSchemaGeneratorOperation:
    @pytest.mark.parametrize("relationship", ["parentOf", "requiresDependency", "rangeValue", "domainValue"])
    def test_get_subgraph_by_edge(self, client, data_model_jsonld, relationship):
        params = {
            "schema_url": data_model_jsonld,
            "relationship": relationship
        }

        response = client.get("http://localhost:3001/v1/schemas/get/graph_by_edge_type", query_string=params)
        assert response.status_code == 200


    @pytest.mark.parametrize("return_display_names", [True, False])
    @pytest.mark.parametrize("node_label", ["FamilyHistory", "TissueStatus"])
    def test_get_node_range(self, client, data_model_jsonld, return_display_names, node_label):
        params = {
            "schema_url": data_model_jsonld,
            "return_display_names": return_display_names,
            "node_label": node_label
        }

        response = client.get('http://localhost:3001/v1/explorer/get_node_range', query_string=params)
        response_dt = json.loads(response.data)
        assert response.status_code == 200

        if "node_label" == "FamilyHistory": 
            assert "Breast" in response_dt
            assert "Lung" in response_dt

        elif "node_label" == "TissueStatus":
            assert "Healthy" in response_dt
            assert "Malignant" in response_dt

    @pytest.mark.parametrize("return_display_names", [None, True, False])
    @pytest.mark.parametrize("return_schema_ordered", [None, True, False])
    @pytest.mark.parametrize("source_node", ["Patient", "Biospecimen"])
    def test_node_dependencies(self, client, data_model_jsonld, source_node, return_display_names, return_schema_ordered):

        return_display_names = True
        return_schema_ordered = False

        params = {
            "schema_url": data_model_jsonld,
            "source_node": source_node,
            "return_display_names": return_display_names,
            "return_schema_ordered": return_schema_ordered
        }

        response = client.get('http://localhost:3001/v1/explorer/get_node_dependencies', query_string=params)
        response_dt = json.loads(response.data)
        assert response.status_code == 200

        if source_node == "Patient":
            # if doesn't get set, return_display_names == True
            if return_display_names == True or return_display_names == None:
                assert "Sex" and "Year of Birth" in response_dt

                # by default, return_schema_ordered is set to True
                if return_schema_ordered == True or return_schema_ordered == None:
                    assert response_dt == ["Patient ID","Sex","Year of Birth","Diagnosis","Component"]
                else: 
                    assert "Year of Birth" in response_dt
                    assert "Diagnosis" in response_dt
                    assert "Patient ID" in response_dt            
            else:
                assert "YearofBirth" in response_dt

        elif source_node == "Biospecimen":
            if return_display_names == True or return_display_names == None:
                assert "Tissue Status" in response_dt
            else: 
                assert "TissueStatus" in response_dt

@pytest.mark.schematic_api
class TestManifestOperation:

    def ifExcelExists(self, response, file_name):
        # return one excel file
        d = response.headers['content-disposition']
        fname = re.findall("filename=(.+)", d)[0]
        assert fname == file_name
    
    def ifGoogleSheetExists(self, response_dt):
        for i in response_dt: 
            assert i.startswith("https://docs.google.com/")
    def ifPandasDataframe(self, response_dt):
        for i in response_dt:
            df = pd.read_json(i)
            assert isinstance(df, pd.DataFrame)


    @pytest.mark.parametrize("output_format", [None, "excel", "google_sheet", "dataframe (only if getting existing manifests)"])
    @pytest.mark.parametrize("data_type", ["Biospecimen", "Patient", "all manifests", ["Biospecimen", "Patient"]])
    def test_generate_existing_manifest(self, client, data_model_jsonld, data_type, output_format, caplog):
        # set dataset
        if data_type == "Patient":
            dataset_id = ["syn42171373"] #Mock Patient Manifest folder on synapse
        elif data_type == "Biospecimen":
            dataset_id = ["syn42171508"] #Mock biospecimen manifest folder
        elif data_type == ["Biospecimen", "Patient"]:
            dataset_id = ["syn42171508", "syn42171373"]
        else: 
            dataset_id = None #if "all manifests", dataset id is None

        params = {
            "schema_url": data_model_jsonld,
            "asset_view": "syn23643253",
            "title": "Example",
            "data_type": data_type,
            "use_annotations": False, 
            }
        if dataset_id: 
            params['dataset_id'] = dataset_id
        
        if output_format: 
            params['output_format'] = output_format

        response = client.get('http://localhost:3001/v1/manifest/generate', query_string=params)

        assert response.status_code == 200

        if dataset_id and output_format:
            if output_format == "excel":
                # for multiple data_types
                if isinstance(data_type, list) and len(data_type) > 1:
                    # return warning message
                    for record in caplog.records:
                        assert record.levelname == "WARNING"
                    assert "Currently we do not support returning multiple files as Excel format at once." in caplog.text
                    self.ifExcelExists(response, "Example.Biospecimen.manifest.xlsx")
                # for single data type
                else: 
                    self.ifExcelExists(response, "Example.xlsx")
            else:
                response_dt = json.loads(response.data)
                if "dataframe" in output_format:
                    self.ifPandasDataframe(response_dt)
                    assert len(response_dt) == len(dataset_id)
                else: 
                    self.ifGoogleSheetExists(response_dt)
        else:
            response_dt = json.loads(response.data)
            self.ifGoogleSheetExists(response_dt)


    @pytest.mark.parametrize("output_format", ["excel", "google_sheet", "dataframe (only if getting existing manifests)", None])
    @pytest.mark.parametrize("data_type", ["all manifests", ["Biospecimen", "Patient"], "Patient"])
    def test_generate_new_manifest(self, caplog, client, data_model_jsonld, data_type, output_format):
        params = {
            "schema_url": data_model_jsonld,
            "asset_view": "syn23643253",
            "title": "Example",
            "data_type": data_type,
            "use_annotations": False,
            "dataset_id": None,
        }

        if output_format: 
            params["output_format"] = output_format
    

        response = client.get('http://localhost:3001/v1/manifest/generate', query_string=params)
        assert response.status_code == 200


        if output_format and output_format == "excel":
            if data_type == "all manifests":
                # return error message
                for record in caplog.records:
                    assert record.levelname == "ERROR"
                assert "Currently we do not support returning multiple files as Excel format at once. Please choose a different output format." in caplog.text
            elif isinstance(data_type, list) and len(data_type) > 1:
                # return warning message
                for record in caplog.records:
                    assert record.levelname == "WARNING"
                assert "Currently we do not support returning multiple files as Excel format at once." in caplog.text
                self.ifExcelExists(response, "Example.Biospecimen.manifest.xlsx")
            else:
                self.ifExcelExists(response, "Example.xlsx")
        
        # return one or multiple google sheet links in all other cases
        # note: output_format == dataframe only matters when dataset_id is not None
        else:
            response_dt = json.loads(response.data)
            self.ifGoogleSheetExists(response_dt)

            if data_type == "all manifests":
                assert len(response_dt) == 3
            elif isinstance(data_type, list) and len(data_type) >1:
                assert len(response_dt) == 2
            else: 
                assert len(response_dt) == 1

    def test_populate_manifest(self, client, data_model_jsonld, test_manifest_csv):
        # test manifest
        test_manifest_data = open(test_manifest_csv, "rb")
        
        params = {
            "data_type": "BulkRNA-seqAssay",
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
            params["data_type"] = "BulkRNA-seqAssay"

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

    def test_get_datatype_manifest(self, client, syn_token):
        params = {
            "input_token": syn_token,
            "asset_view": "syn23643253",
            "manifest_id": "syn27600110"
        }

        response = client.get('http://localhost:3001/v1/get/datatype/manifest', query_string=params)  

        assert response.status_code == 200
        response_dt = json.loads(response.data)
        assert response_dt =={
                "Cancer Type": "string",
                "Component": "string",
                "Diagnosis": "string",
                "Family History": "string",
                "Patient ID": "Int64",
                "Sex": "string",
                "Year of Birth": "Int64",
                "entityId": "string"}

    @pytest.mark.parametrize("as_json", [None, True, False])
    @pytest.mark.parametrize("new_manifest_name", [None, "Test"])
    def test_manifest_download(self, client, as_json, syn_token, new_manifest_name):
        params = {
            "input_token": syn_token,
            "asset_view": "syn28559058",
            "dataset_id": "syn28268700",
            "as_json": as_json,
            "new_manifest_name": new_manifest_name
        }

        response = client.get('http://localhost:3001/v1/manifest/download', query_string = params)
        assert response.status_code == 200
        response_dt = response.data

        if as_json: 
            response_json = json.loads(response_dt)
            assert response_json == [{'Component': 'BulkRNA-seqAssay', 'File Format': 'CSV/TSV', 'Filename': 'Sample_A', 'Genome Build': 'GRCm38', 'Genome FASTA': None, 'Sample ID': 2022, 'entityId': 'syn28278954'}]
        else:
            # return a file path
            response_path = response_dt.decode('utf-8')

            assert isinstance(response_path, str)
            assert response_path.endswith(".csv")
    
    @retry(wait=wait_random_exponential(multiplier=1,min=10,max=120), stop=stop_after_attempt(10))
    def submit_manifest_json(self, client, params):
        response = client.post('http://localhost:3001/v1/model/submit', query_string = params, data={"file_name":''})
        if response.status_code == 500:
            raise TryAgain
        else:
            return response.status_code

    @retry(wait=wait_random_exponential(multiplier=1,min=10,max=120), stop=stop_after_attempt(10))
    def submit_manifest_csv(self, client, params, headers, test_manifest_csv):
        response = client.post('http://localhost:3001/v1/model/submit', query_string=params, data={"file_name": (open(test_manifest_csv, 'rb'), "test.csv")}, headers=headers)
        if response.status_code == 500:
            raise TryAgain
        else:
            return response.status_code


    @pytest.mark.parametrize("json_str", [None, '[{ "Patient ID": 123, "Sex": "Female", "Year of Birth": "", "Diagnosis": "Healthy", "Component": "Patient", "Cancer Type": "Breast", "Family History": "Breast, Lung", }]'])
    def test_submit_manifest(self, client, syn_token, data_model_jsonld, json_str, test_manifest_csv):
        params = {
            "input_token": syn_token,
            "schema_url": data_model_jsonld,
            "data_type": "Patient",
            "restrict_rules": False, 
            "manifest_record_type": "table",
            "asset_view": "syn44259375",
            "dataset_id": "syn44259313",
        }

        if json_str:
            params["json_str"] = json_str
            try:
                status_code = self.submit_manifest_json(params=params,client=client)
            except (RetryError) as e:
                pass
            assert status_code == 200
        else: 
            headers = {
            'Content-Type': "multipart/form-data",
            'Accept': "application/json"
            }
            params["data_type"] = "BulkRNA-seqAssay"

            try:
                status_code=self.submit_manifest_csv(params=params,client=client,headers=headers,test_manifest_csv=test_manifest_csv)
            except (RetryError) as e:
                pass
            assert status_code == 200     


@pytest.mark.schematic_api
class TestSchemaVisualization:
    def test_visualize_attributes(self, client, data_model_jsonld):
        params = {
            "schema_url": data_model_jsonld
        }

        response = client.get("http://localhost:3001/v1/visualize/attributes", query_string = params)

        assert response.status_code == 200

    @pytest.mark.parametrize("figure_type", ["component", "dependency"])
    def test_visualize_tangled_tree_layers(self, client, figure_type, data_model_jsonld):
        params = {
            "schema_url": data_model_jsonld,
            "figure_type": figure_type
        }

        response = client.get("http://localhost:3001/v1/visualize/tangled_tree/layers", query_string = params)

        assert response.status_code == 200








