
import pytest
from api import create_app
import configparser
import json
import os
import re
from math import ceil
import logging
from time import perf_counter
import pandas as pd # third party library import
from schematic.schemas.generator import SchemaGenerator #Local application/library specific imports.

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

'''
To run the tests, you have to keep API running locally first by doing `python3 run_api.py`
'''

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
    test_manifest_path = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
    yield test_manifest_path

@pytest.fixture(scope="class")
def test_invalid_manifest(helpers):
    test_invalid_manifest = helpers.get_data_frame("mock_manifests/Invalid_Test_Manifest.csv", preserve_raw_input=False)
    yield test_invalid_manifest

@pytest.fixture(scope="class")
def test_upsert_manifest_csv(helpers):
    test_upsert_manifest_path = helpers.get_data_path("mock_manifests/rdb_table_manifest.csv")
    yield test_upsert_manifest_path

@pytest.fixture(scope="class")
def test_manifest_json(helpers):
    test_manifest_path = helpers.get_data_path("mock_manifests/Example.Patient.manifest.json")
    yield test_manifest_path

@pytest.fixture(scope="class")
def data_model_jsonld():
    data_model_jsonld ="https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld"
    yield data_model_jsonld

@pytest.fixture(scope="class")
def benchmark_data_model_jsonld():
    benchmark_data_model_jsonld = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.single_rule.model.jsonld"
    yield benchmark_data_model_jsonld

def get_MockComponent_attribute():
    """
    Yield all of the mock conponent attributes one at a time
    TODO: pull in jsonld from fixture
    """
    sg = SchemaGenerator("https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.single_rule.model.jsonld")
    attributes=sg.get_node_dependencies('MockComponent')
    attributes.remove('Component')

    for MockComponent_attribute in attributes:
        yield MockComponent_attribute   

@pytest.fixture(scope="class")
def syn_token(config):
    synapse_config_path = config.SYNAPSE_CONFIG_PATH
    config_parser = configparser.ConfigParser()
    config_parser.read(synapse_config_path)
    # try using synapse access token
    if "SYNAPSE_ACCESS_TOKEN" in os.environ:
        token=os.environ["SYNAPSE_ACCESS_TOKEN"]
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

    @pytest.mark.parametrize("entity_id", ["syn34640850", "syn23643253", "syn24992754"])
    def test_get_entity_type(self, syn_token, client, entity_id):
        params = {
            "input_token": syn_token,
            "asset_view": "syn23643253",
            "entity_id": entity_id
        }
        response = client.get("http://localhost:3001/v1/storage/entity/type", query_string = params)

        assert response.status_code == 200
        response_dt = json.loads(response.data)
        if entity_id == "syn23643253":
            assert response_dt == "asset view"
        elif entity_id == "syn34640850":
            assert response_dt == "folder"
        elif entity_id == "syn24992754":
            assert response_dt == "project"

    @pytest.mark.parametrize("entity_id", ["syn30988314", "syn27221721"])
    def test_if_in_assetview(self, syn_token, client, entity_id):
        params = {
            "input_token": syn_token,
            "asset_view": "syn23643253",
            "entity_id": entity_id
        }
        response = client.get("http://localhost:3001/v1/storage/if_in_asset_view", query_string = params)        
        assert response.status_code == 200
        response_dt = json.loads(response.data)

        if entity_id == "syn30988314":
            assert response_dt == True
        elif entity_id == "syn27221721":
            assert response_dt == False

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

    def test_if_node_required(test, client, data_model_jsonld):
        params = {
            "schema_url": data_model_jsonld,
            "node_display_name": "FamilyHistory"
        }

        response = client.get("http://localhost:3001/v1/schemas/is_node_required", query_string = params)
        response_dta = json.loads(response.data)
        assert response.status_code == 200
        assert response_dta == True
    def test_get_node_validation_rules(test, client, data_model_jsonld):
        params = {
            "schema_url": data_model_jsonld,
            "node_display_name": "CheckRegexList"
        }
        response = client.get("http://localhost:3001/v1/schemas/get_node_validation_rules", query_string = params)
        response_dta = json.loads(response.data)
        assert response.status_code == 200
        assert "list strict" in response_dta
        assert "regex match [a-f]" in response_dta        

    def test_get_nodes_display_names(test, client, data_model_jsonld):
        params = {
            "schema_url": data_model_jsonld,
            "node_list": ["FamilyHistory", "Biospecimen"]
        }
        response = client.get("http://localhost:3001/v1/schemas/get_nodes_display_names", query_string = params)
        response_dta = json.loads(response.data)
        assert response.status_code == 200
        assert "Family History" and "Biospecimen" in response_dta


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


    #@pytest.mark.parametrize("output_format", [None, "excel", "google_sheet", "dataframe (only if getting existing manifests)"])
    @pytest.mark.parametrize("output_format", ["excel"])
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
            "input_token": None
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
                        if record.message == "Currently we do not support returning multiple files as Excel format at once.":
                            assert record.levelname == "WARNING"
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
            "input_token": None
        }

        if output_format: 
            params["output_format"] = output_format
    

        response = client.get('http://localhost:3001/v1/manifest/generate', query_string=params)
        assert response.status_code == 200


        if output_format and output_format == "excel":
            if data_type == "all manifests":
                # return error message
                for record in caplog.records:
                    if record.message == "Currently we do not support returning multiple files as Excel format at once.":
                        assert record.levelname == "WARNING"
            elif isinstance(data_type, list) and len(data_type) > 1:
                # return warning message
                for record in caplog.records:
                    if record.message == "Currently we do not support returning multiple files as Excel format at once.":
                        assert record.levelname == "WARNING"
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

    @pytest.mark.parametrize("restrict_rules", [False, True, None])
    @pytest.mark.parametrize("json_str", [None, '[{"Patient ID": 123, "Sex": "Female", "Year of Birth": "", "Diagnosis": "Healthy", "Component": "Patient", "Cancer Type": "Breast", "Family History": "Breast, Lung"}]'])
    def test_validate_manifest(self, data_model_jsonld, client, json_str, restrict_rules, test_manifest_csv):

        params = {
            "schema_url": data_model_jsonld,
            "restrict_rules": restrict_rules
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
            # WILL DEPRECATE uploading a json file for validation
            # params["data_type"] = "Patient"
            # response_json =  client.post('http://localhost:3001/v1/model/validate', query_string=params, data={"file_name": (open(test_manifest_json, 'rb'), "test.json")}, headers=headers)
            # response_dt = json.loads(response_json.data)
            # assert response_json.status_code == 200

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

    @pytest.mark.parametrize("json_str", [None, '[{ "Patient ID": 123, "Sex": "Female", "Year of Birth": "", "Diagnosis": "Healthy", "Component": "Patient", "Cancer Type": "Breast", "Family History": "Breast, Lung", }]'])
    @pytest.mark.parametrize("use_schema_label", ['true','false'])
    @pytest.mark.parametrize("manifest_record_type", ['table_and_file', 'file_only'])
    def test_submit_manifest(self, client, syn_token, data_model_jsonld, json_str, test_manifest_csv, use_schema_label, manifest_record_type):
        params = {
            "input_token": syn_token,
            "schema_url": data_model_jsonld,
            "data_type": "Patient",
            "restrict_rules": False, 
            "manifest_record_type": manifest_record_type,
            "asset_view": "syn44259375",
            "dataset_id": "syn44259313",
            "table_manipulation": 'replace',
            "use_schema_label": use_schema_label
        }

        if json_str:
            params["json_str"] = json_str
            response = client.post('http://localhost:3001/v1/model/submit', query_string = params, data={"file_name":''})
            assert response.status_code == 200
        else: 
            headers = {
            'Content-Type': "multipart/form-data",
            'Accept': "application/json"
            }
            params["data_type"] = "MockComponent"

            # test uploading a csv file
            response_csv = client.post('http://localhost:3001/v1/model/submit', query_string=params, data={"file_name": (open(test_manifest_csv, 'rb'), "test.csv")}, headers=headers)
            assert response_csv.status_code == 200

    @pytest.mark.parametrize("json_str", [None, '[{ "Patient ID": 123, "Sex": "Female", "Year of Birth": "", "Diagnosis": "Healthy", "Component": "Patient", "Cancer Type": "Breast", "Family History": "Breast, Lung", }]'])
    @pytest.mark.parametrize("manifest_record_type", ['file_and_entities', 'table_file_and_entities'])
    def test_submit_manifest_w_entities(self, client, syn_token, data_model_jsonld, json_str, test_manifest_csv, manifest_record_type):
        params = {
            "input_token": syn_token,
            "schema_url": data_model_jsonld,
            "data_type": "Patient",
            "restrict_rules": False, 
            "manifest_record_type": manifest_record_type,
            "asset_view": "syn44259375",
            "dataset_id": "syn44259313",
            "table_manipulation": 'replace',
            "use_schema_label": True
        }

        if json_str:
            params["json_str"] = json_str
            response = client.post('http://localhost:3001/v1/model/submit', query_string = params, data={"file_name":''})
            assert response.status_code == 200
        else: 
            headers = {
            'Content-Type': "multipart/form-data",
            'Accept': "application/json"
            }
            params["data_type"] = "MockComponent"

            # test uploading a csv file
            response_csv = client.post('http://localhost:3001/v1/model/submit', query_string=params, data={"file_name": (open(test_manifest_csv, 'rb'), "test.csv")}, headers=headers)
            assert response_csv.status_code == 200  

    
    @pytest.mark.parametrize("json_str", [None, '[{ "Component": "MockRDB", "MockRDB_id": 5 }]'])
    def test_submit_manifest_upsert(self, client, syn_token, data_model_jsonld, json_str, test_upsert_manifest_csv, ):
        params = {
            "input_token": syn_token,
            "schema_url": data_model_jsonld,
            "data_type": "MockRDB",
            "restrict_rules": False, 
            "manifest_record_type": "table",
            "asset_view": "syn44259375",
            "dataset_id": "syn44259313",
            "table_manipulation": 'upsert',
            "use_schema_label": False
        }

        if json_str:
            params["json_str"] = json_str
            response = client.post('http://localhost:3001/v1/model/submit', query_string = params, data={"file_name":''})
            assert response.status_code == 200
        else: 
            headers = {
            'Content-Type': "multipart/form-data",
            'Accept': "application/json"
            }
            params["data_type"] = "MockRDB"

            # test uploading a csv file
            response_csv = client.post('http://localhost:3001/v1/model/submit', query_string=params, data={"file_name": (open(test_upsert_manifest_csv, 'rb'), "test.csv")}, headers=headers)            
            assert response_csv.status_code == 200     

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

    def test_visualize_component(self, client, data_model_jsonld):
        params = {
            "schema_url": data_model_jsonld,
            "component": "Patient"
        }

        response = client.get("http://localhost:3001/v1/visualize/component", query_string = params)

        assert response.status_code == 200


@pytest.mark.schematic_api
class TestValidationBenchmark():
    @pytest.mark.parametrize('MockComponent_attribute', get_MockComponent_attribute())
    def test_validation_performance(self, helpers, benchmark_data_model_jsonld, client, test_invalid_manifest, MockComponent_attribute ):
        """
        Test to benchamrk performance of validation rules on large manifests
        Test loads the invalid_test_manifest.csv and isolates one attribute at a time
            it then enforces an error rate of 33% in the attribute (except in the case of Match Exactly Values)
            the single attribute manifest is then extended to be ~1000 rows to see performance on a large manfiest
            the manifest is passed to the validation endpoint, and the response time of the endpoint is measured
            Target response time for all rules is under 5.00 seconds with a successful api response
        """

        # Number of rows to target for large manfiest
        target_rows = 1000
        # URL of validtion endpoint
        endpoint_url = 'http://localhost:3001/v1/model/validate'

        # Set paramters for endpoint
        params = { 
            "schema_url": benchmark_data_model_jsonld,
            "data_type": "MockComponent",

        }
        headers = {
        'Content-Type': "multipart/form-data",
        'Accept': "application/json"
        }

        # Enforce error rate when possible
        if MockComponent_attribute == 'Check Ages':
            test_invalid_manifest.loc[0,MockComponent_attribute]  = '6550'
        elif MockComponent_attribute == 'Check Date':
            test_invalid_manifest.loc[0,MockComponent_attribute]   = 'October 21 2022'
            test_invalid_manifest.loc[2,MockComponent_attribute]   = 'October 21 2022'
        elif MockComponent_attribute == 'Check Unique':
            test_invalid_manifest.loc[0,MockComponent_attribute]   = 'str2'


        # Isolate single attribute of interest, keep `Component` column
        single_attribute_manfiest = test_invalid_manifest[['Component', MockComponent_attribute]]

        # Extend to ~1000 rows in size to for performance test
        multi_factor = ceil(target_rows/single_attribute_manfiest.shape[0])
        large_manfiest = pd.concat([single_attribute_manfiest]*multi_factor, ignore_index = True)

        try:
            # Convert manfiest to csv for api endpoint
            large_manifest_path = helpers.get_data_path('mock_manifests/large_manifest_test.csv')
            large_manfiest.to_csv(large_manifest_path, index=False)

            # Run and time endpoint
            t_start = perf_counter()
            response = client.post(endpoint_url, query_string=params, data={"file_name": (open(large_manifest_path, 'rb'), "large_test.csv")}, headers=headers)
            response_time = perf_counter() - t_start
        finally:
            # Remove temp manfiest
            os.remove(large_manifest_path)
        
        # Log and check time and ensure successful response
        logger.warning(f"validation endpiont response time {round(response_time,2)} seconds.")
        assert response.status_code == 200
        assert response_time < 5.00  


        
        




