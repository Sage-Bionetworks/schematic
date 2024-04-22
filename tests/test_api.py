import configparser
import json
import logging
import os
import re
import time
from math import ceil
from time import perf_counter

import numpy as np
import pandas as pd  # third party library import
import pytest

from schematic.configuration.configuration import Configuration
from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships

from schematic_api.api import create_app


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


## TO DO: Clean up url and use a global variable SERVER_URL
@pytest.fixture(scope="class")
def app():
    app = create_app()
    yield app


@pytest.fixture(scope="class")
def client(app):
    app.config["SCHEMATIC_CONFIG"] = None

    with app.test_client() as client:
        yield client


@pytest.fixture(scope="class")
def test_manifest_csv(helpers):
    test_manifest_path = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
    yield test_manifest_path


@pytest.fixture(scope="class")
def test_manifest_submit(helpers):
    test_manifest_path = helpers.get_data_path(
        "mock_manifests/example_biospecimen_test.csv"
    )
    yield test_manifest_path


@pytest.fixture(scope="class")
def test_invalid_manifest(helpers):
    test_invalid_manifest = helpers.get_data_frame(
        "mock_manifests/Invalid_Test_Manifest.csv", preserve_raw_input=False
    )
    yield test_invalid_manifest


@pytest.fixture(scope="class")
def test_upsert_manifest_csv(helpers):
    test_upsert_manifest_path = helpers.get_data_path(
        "mock_manifests/rdb_table_manifest.csv"
    )
    yield test_upsert_manifest_path


@pytest.fixture(scope="class")
def test_manifest_json(helpers):
    test_manifest_path = helpers.get_data_path(
        "mock_manifests/Example.Patient.manifest.json"
    )
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
    schema_url = "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.single_rule.model.jsonld"
    data_model_parser = DataModelParser(path_to_data_model=schema_url)
    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    dmge = DataModelGraphExplorer(graph_data_model)

    attributes = dmge.get_node_dependencies("MockComponent")
    attributes.remove("Component")

    for MockComponent_attribute in attributes:
        yield MockComponent_attribute


@pytest.fixture(scope="class")
def syn_token(config: Configuration):
    synapse_config_path = config.synapse_configuration_path
    config_parser = configparser.ConfigParser()
    config_parser.read(synapse_config_path)
    # try using synapse access token
    if "SYNAPSE_ACCESS_TOKEN" in os.environ:
        token = os.environ["SYNAPSE_ACCESS_TOKEN"]
    else:
        token = config_parser["authentication"]["authtoken"]
    yield token


@pytest.fixture
def request_headers(syn_token):
    headers = {"Authorization": "Bearer " + syn_token}
    yield headers


@pytest.mark.schematic_api
class TestSynapseStorage:
    @pytest.mark.synapse_credentials_needed
    @pytest.mark.parametrize("return_type", ["json", "csv"])
    def test_get_storage_assets_tables(self, client, return_type, request_headers):
        params = {"asset_view": "syn23643253", "return_type": return_type}

        response = client.get(
            "http://localhost:3001/v1/storage/assets/tables",
            query_string=params,
            headers=request_headers,
        )

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

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.parametrize("full_path", [True, False])
    @pytest.mark.parametrize("file_names", [None, "Sample_A.txt"])
    def test_get_dataset_files(self, full_path, file_names, request_headers, client):
        params = {
            "asset_view": "syn23643253",
            "dataset_id": "syn23643250",
            "full_path": full_path,
        }

        if file_names:
            params["file_names"] = file_names

        response = client.get(
            "http://localhost:3001/v1/storage/dataset/files",
            query_string=params,
            headers=request_headers,
        )

        assert response.status_code == 200
        response_dt = json.loads(response.data)

        # would show full file path .txt in result
        if full_path:
            if file_names:
                assert (
                    ["syn23643255", "schematic - main/DataTypeX/Sample_A.txt"]
                    and [
                        "syn24226530",
                        "schematic - main/TestDatasets/TestDataset-Annotations/Sample_A.txt",
                    ]
                    and [
                        "syn25057024",
                        "schematic - main/TestDatasets/TestDataset-Annotations-v2/Sample_A.txt",
                    ]
                    in response_dt
                )
            else:
                assert [
                    "syn23643255",
                    "schematic - main/DataTypeX/Sample_A.txt",
                ] in response_dt
        else:
            if file_names:
                assert (
                    ["syn23643255", "Sample_A.txt"]
                    and ["syn24226530", "Sample_A.txt"]
                    and ["syn25057024", "Sample_A.txt"] in response_dt
                )
                assert ["syn23643256", "Sample_C.txt"] and [
                    "syn24226531",
                    "Sample_B.txt",
                ] not in response_dt
            else:
                assert (
                    ["syn23643256", "Sample_C.txt"]
                    and ["syn24226530", "Sample_A.txt"]
                    and ["syn24226531", "Sample_B.txt"] in response_dt
                )

    @pytest.mark.synapse_credentials_needed
    def test_get_storage_project_dataset(self, request_headers, client):
        params = {"asset_view": "syn23643253", "project_id": "syn26251192"}

        response = client.get(
            "http://localhost:3001/v1/storage/project/datasets",
            query_string=params,
            headers=request_headers,
        )
        assert response.status_code == 200
        response_dt = json.loads(response.data)
        assert ["syn26251193", "Issue522"] in response_dt

    @pytest.mark.synapse_credentials_needed
    def test_get_storage_project_manifests(self, request_headers, client):
        params = {"asset_view": "syn23643253", "project_id": "syn30988314"}

        response = client.get(
            "http://localhost:3001/v1/storage/project/manifests",
            query_string=params,
            headers=request_headers,
        )

        assert response.status_code == 200

    @pytest.mark.synapse_credentials_needed
    def test_get_storage_projects(self, request_headers, client):
        params = {"asset_view": "syn23643253"}

        response = client.get(
            "http://localhost:3001/v1/storage/projects",
            query_string=params,
            headers=request_headers,
        )

        assert response.status_code == 200

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.parametrize("entity_id", ["syn34640850", "syn23643253", "syn24992754"])
    def test_get_entity_type(self, request_headers, client, entity_id):
        params = {"asset_view": "syn23643253", "entity_id": entity_id}
        response = client.get(
            "http://localhost:3001/v1/storage/entity/type",
            query_string=params,
            headers=request_headers,
        )

        assert response.status_code == 200
        response_dt = json.loads(response.data)
        if entity_id == "syn23643253":
            assert response_dt == "asset view"
        elif entity_id == "syn34640850":
            assert response_dt == "folder"
        elif entity_id == "syn24992754":
            assert response_dt == "project"

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.parametrize("entity_id", ["syn30988314", "syn27221721"])
    def test_if_in_assetview(self, request_headers, client, entity_id):
        params = {"asset_view": "syn23643253", "entity_id": entity_id}
        response = client.get(
            "http://localhost:3001/v1/storage/if_in_asset_view",
            query_string=params,
            headers=request_headers,
        )
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
            "as_graph": as_graph,
        }

        response = client.get(
            "http://localhost:3001/v1/model/component-requirements", query_string=params
        )

        assert response.status_code == 200

        response_dt = json.loads(response.data)

        if as_graph:
            assert response_dt == [
                ["Biospecimen", "Patient"],
                ["BulkRNA-seqAssay", "Biospecimen"],
            ]
        else:
            assert response_dt == ["Patient", "Biospecimen", "BulkRNA-seqAssay"]


@pytest.mark.schematic_api
class TestUtilsOperation:
    @pytest.mark.parametrize("strict_camel_case", [True, False])
    def test_get_property_label_from_display_name(self, client, strict_camel_case):
        params = {
            "display_name": "mocular entity",
            "strict_camel_case": strict_camel_case,
        }

        response = client.get(
            "http://localhost:3001/v1/utils/get_property_label_from_display_name",
            query_string=params,
        )
        assert response.status_code == 200

        response_dt = json.loads(response.data)

        if strict_camel_case:
            assert response_dt == "mocularEntity"
        else:
            assert response_dt == "mocularentity"


@pytest.mark.schematic_api
class TestDataModelGraphExplorerOperation:
    def test_get_schema(self, client, data_model_jsonld):
        params = {"schema_url": data_model_jsonld,
                  "data_model_labels": 'class_label'}
        response = client.get(
            "http://localhost:3001/v1/schemas/get/schema", query_string=params
        )

        response_dt = response.data
        assert response.status_code == 200
        assert os.path.exists(response_dt)

        # if path exists, remove the file
        if os.path.exists(response_dt):
            os.remove(response_dt)

    def test_if_node_required(test, client, data_model_jsonld):
        params = {"schema_url": data_model_jsonld, "node_display_name": "FamilyHistory", "data_model_labels": "class_label"}

        response = client.get(
            "http://localhost:3001/v1/schemas/is_node_required", query_string=params
        )
        response_dta = json.loads(response.data)
        assert response.status_code == 200
        assert response_dta == True

    def test_get_node_validation_rules(test, client, data_model_jsonld):
        params = {
            "schema_url": data_model_jsonld,
            "node_display_name": "CheckRegexList",
        }
        response = client.get(
            "http://localhost:3001/v1/schemas/get_node_validation_rules",
            query_string=params,
        )
        response_dta = json.loads(response.data)
        assert response.status_code == 200
        assert "list strict" in response_dta
        assert "regex match [a-f]" in response_dta

    def test_get_nodes_display_names(test, client, data_model_jsonld):
        params = {
            "schema_url": data_model_jsonld,
            "node_list": ["FamilyHistory", "Biospecimen"],
        }
        response = client.get(
            "http://localhost:3001/v1/schemas/get_nodes_display_names",
            query_string=params,
        )
        response_dta = json.loads(response.data)
        assert response.status_code == 200
        assert "Family History" and "Biospecimen" in response_dta

    @pytest.mark.parametrize(
        "relationship", ["parentOf", "requiresDependency", "rangeValue", "domainValue"]
    )
    def test_get_subgraph_by_edge(self, client, data_model_jsonld, relationship):
        params = {"schema_url": data_model_jsonld, "relationship": relationship}

        response = client.get(
            "http://localhost:3001/v1/schemas/get/graph_by_edge_type",
            query_string=params,
        )
        assert response.status_code == 200

    @pytest.mark.parametrize("return_display_names", [True, False])
    @pytest.mark.parametrize("node_label", ["FamilyHistory", "TissueStatus"])
    def test_get_node_range(
        self, client, data_model_jsonld, return_display_names, node_label
    ):
        params = {
            "schema_url": data_model_jsonld,
            "return_display_names": return_display_names,
            "node_label": node_label,
        }

        response = client.get(
            "http://localhost:3001/v1/schemas/get_node_range", query_string=params
        )
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
    def test_node_dependencies(
        self,
        client,
        data_model_jsonld,
        source_node,
        return_display_names,
        return_schema_ordered,
    ):
        return_display_names = True
        return_schema_ordered = False

        params = {
            "schema_url": data_model_jsonld,
            "source_node": source_node,
            "return_display_names": return_display_names,
            "return_schema_ordered": return_schema_ordered,
        }

        response = client.get(
            "http://localhost:3001/v1/schemas/get_node_dependencies",
            query_string=params,
        )
        response_dt = json.loads(response.data)
        assert response.status_code == 200

        if source_node == "Patient":
            # if doesn't get set, return_display_names == True
            if return_display_names == True or return_display_names == None:
                assert "Sex" and "Year of Birth" in response_dt

                # by default, return_schema_ordered is set to True
                if return_schema_ordered == True or return_schema_ordered == None:
                    assert response_dt == [
                        "Patient ID",
                        "Sex",
                        "Year of Birth",
                        "Diagnosis",
                        "Component",
                    ]
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
        d = response.headers["content-disposition"]
        fname = re.findall("filename=(.+)", d)[0]
        assert fname == file_name

    def ifGoogleSheetExists(self, response_dt):
        for i in response_dt:
            assert i.startswith("https://docs.google.com/")

    def ifPandasDataframe(self, response_dt):
        for i in response_dt:
            df = pd.read_json(i)
            assert isinstance(df, pd.DataFrame)

    @pytest.mark.empty_token
    # @pytest.mark.parametrize("output_format", [None, "excel", "google_sheet", "dataframe (only if getting existing manifests)"])
    @pytest.mark.parametrize("output_format", ["excel"])
    @pytest.mark.parametrize(
        "data_type",
        ["Biospecimen", "Patient", "all manifests", ["Biospecimen", "Patient"]],
    )
    def test_generate_existing_manifest(
        self,
        client,
        data_model_jsonld,
        data_type,
        output_format,
        caplog,
        request_headers,
    ):
        # set dataset
        if data_type == "Patient":
            dataset_id = ["syn51730545"]  # Mock Patient Manifest folder on synapse
        elif data_type == "Biospecimen":
            dataset_id = ["syn51730547"]  # Mock biospecimen manifest folder
        elif data_type == ["Biospecimen", "Patient"]:
            dataset_id = ["syn51730547", "syn51730545"]
        else:
            dataset_id = None  # if "all manifests", dataset id is None

        params = {
            "schema_url": data_model_jsonld,
            "asset_view": "syn23643253",
            "title": "Example",
            "data_type": data_type,
            "use_annotations": False,
            "data_model_labels": "class_label",
        }

        # Previous form of the test had `access_token` set to `None`
        request_headers["Authorization"] = None

        if dataset_id:
            params["dataset_id"] = dataset_id

        if output_format:
            params["output_format"] = output_format

        response = client.get(
            "http://localhost:3001/v1/manifest/generate",
            query_string=params,
            headers=request_headers,
        )

        assert response.status_code == 200

        if dataset_id and output_format:
            if output_format == "excel":
                # for multiple data_types
                if isinstance(data_type, list) and len(data_type) > 1:
                    # return warning message
                    for record in caplog.records:
                        if (
                            record.message
                            == "Currently we do not support returning multiple files as Excel format at once."
                        ):
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

    @pytest.mark.empty_token
    @pytest.mark.parametrize(
        "output_format",
        [
            "excel",
            "google_sheet",
            "dataframe (only if getting existing manifests)",
            None,
        ],
    )
    @pytest.mark.parametrize(
        "data_type", ["all manifests", ["Biospecimen", "Patient"], "Patient"]
    )
    def test_generate_new_manifest(
        self,
        caplog,
        client,
        data_model_jsonld,
        data_type,
        output_format,
        request_headers,
    ):
        params = {
            "schema_url": data_model_jsonld,
            "asset_view": "syn23643253",
            "title": "Example",
            "data_type": data_type,
            "use_annotations": False,
            "dataset_id": None,
        }

        # Previous form of the test had `access_token` set to `None`
        request_headers["Authorization"] = None

        if output_format:
            params["output_format"] = output_format

        response = client.get(
            "http://localhost:3001/v1/manifest/generate",
            query_string=params,
            headers=request_headers,
        )
        assert response.status_code == 200

        if output_format and output_format == "excel":
            if data_type == "all manifests":
                # return error message
                for record in caplog.records:
                    if (
                        record.message
                        == "Currently we do not support returning multiple files as Excel format at once."
                    ):
                        assert record.levelname == "WARNING"
            elif isinstance(data_type, list) and len(data_type) > 1:
                # return warning message
                for record in caplog.records:
                    if (
                        record.message
                        == "Currently we do not support returning multiple files as Excel format at once."
                    ):
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
            elif isinstance(data_type, list) and len(data_type) > 1:
                assert len(response_dt) == 2
            else:
                assert len(response_dt) == 1

    # test case: generate a manifest when use_annotations is set to True/False for a file-based component
    # based on the parameter, the columns in the manifests would be different
    # the dataset folder does not contain an existing manifest
    @pytest.mark.parametrize(
        "use_annotations,expected",
        [
            (
                True,
                [
                    "Filename",
                    "Sample ID",
                    "File Format",
                    "Component",
                    "Genome Build",
                    "Genome FASTA",
                    "impact",
                    "Year of Birth",
                    "date",
                    "confidence",
                    "IsImportantBool",
                    "IsImportantText",
                    "author",
                    "eTag",
                    "entityId",
                ],
            ),
            (
                False,
                [
                    "Filename",
                    "Sample ID",
                    "File Format",
                    "Component",
                    "Genome Build",
                    "Genome FASTA",
                    "entityId",
                ],
            ),
        ],
    )
    def test_generate_manifest_file_based_annotations(
        self, client, use_annotations, expected, data_model_jsonld
    ):
        params = {
            "schema_url": data_model_jsonld,
            "data_type": "BulkRNA-seqAssay",
            "dataset_id": "syn25614635",
            "asset_view": "syn51707141",
            "output_format": "google_sheet",
            "use_annotations": use_annotations,
        }

        response = client.get(
            "http://localhost:3001/v1/manifest/generate", query_string=params
        )
        assert response.status_code == 200

        response_google_sheet = json.loads(response.data)

        # open the google sheet
        google_sheet_df = pd.read_csv(
            response_google_sheet[0] + "/export?gid=0&format=csv"
        )

        # make sure that columns used in annotations get added
        # and also make sure that entityId column appears in the end

        assert google_sheet_df.columns.to_list()[-1] == "entityId"

        assert sorted(google_sheet_df.columns.to_list()) == sorted(expected)

        # make sure Filename, entityId, and component get filled with correct value
        assert google_sheet_df["Filename"].to_list() == [
            "TestDataset-Annotations-v3/Sample_A.txt",
            "TestDataset-Annotations-v3/Sample_B.txt",
            "TestDataset-Annotations-v3/Sample_C.txt",
        ]
        assert google_sheet_df["entityId"].to_list() == [
            "syn25614636",
            "syn25614637",
            "syn25614638",
        ]
        assert google_sheet_df["Component"].to_list() == [
            "BulkRNA-seqAssay",
            "BulkRNA-seqAssay",
            "BulkRNA-seqAssay",
        ]

    # test case: generate a manifest with annotations when use_annotations is set to True for a component that is not file-based
    # the dataset folder does not contain an existing manifest
    def test_generate_manifest_not_file_based_with_annotations(
        self, client, data_model_jsonld
    ):
        params = {
            "schema_url": data_model_jsonld,
            "data_type": "Patient",
            "dataset_id": "syn25614635",
            "asset_view": "syn51707141",
            "output_format": "google_sheet",
            "use_annotations": False,
        }
        response = client.get(
            "http://localhost:3001/v1/manifest/generate", query_string=params
        )
        assert response.status_code == 200

        response_google_sheet = json.loads(response.data)

        # open the google sheet
        google_sheet_df = pd.read_csv(
            response_google_sheet[0] + "/export?gid=0&format=csv"
        )

        # make sure that the result is basically the same as generating a new manifest
        assert sorted(google_sheet_df.columns) == sorted(
            [
                "Patient ID",
                "Sex",
                "Year of Birth",
                "Diagnosis",
                "Component",
                "Cancer Type",
                "Family History",
            ]
        )

    def test_populate_manifest(self, client, data_model_jsonld, test_manifest_csv):
        # test manifest
        test_manifest_data = open(test_manifest_csv, "rb")

        params = {
            "data_type": "MockComponent",
            "schema_url": data_model_jsonld,
            "title": "Example",
            "csv_file": test_manifest_data,
        }

        response = client.get(
            "http://localhost:3001/v1/manifest/generate", query_string=params
        )

        assert response.status_code == 200
        response_dt = json.loads(response.data)

        # should return a list with one google sheet link
        assert isinstance(response_dt[0], str)
        assert response_dt[0].startswith("https://docs.google.com/")

    @pytest.mark.parametrize("restrict_rules", [False, True, None])
    @pytest.mark.parametrize(
        "json_str",
        [
            None,
            '[{"Patient ID": 123, "Sex": "Female", "Year of Birth": "", "Diagnosis": "Healthy", "Component": "Patient", "Cancer Type": "Breast", "Family History": "Breast, Lung"}]',
        ],
    )
    def test_validate_manifest(
        self,
        data_model_jsonld,
        client,
        json_str,
        restrict_rules,
        test_manifest_csv,
        request_headers,
    ):
        params = {"schema_url": data_model_jsonld, "restrict_rules": restrict_rules}

        if json_str:
            params["json_str"] = json_str
            params["data_type"] = "Patient"
            response = client.post(
                "http://localhost:3001/v1/model/validate", query_string=params
            )
            response_dt = json.loads(response.data)
            assert response.status_code == 200

        else:
            params["data_type"] = "MockComponent"

            request_headers.update(
                {"Content-Type": "multipart/form-data", "Accept": "application/json"}
            )

            # test uploading a csv file
            response_csv = client.post(
                "http://localhost:3001/v1/model/validate",
                query_string=params,
                data={"file_name": (open(test_manifest_csv, "rb"), "test.csv")},
                headers=request_headers,
            )
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

    @pytest.mark.synapse_credentials_needed
    def test_get_datatype_manifest(self, client, request_headers):
        params = {"asset_view": "syn23643253", "manifest_id": "syn27600110"}

        response = client.get(
            "http://localhost:3001/v1/get/datatype/manifest",
            query_string=params,
            headers=request_headers,
        )

        assert response.status_code == 200
        response_dt = json.loads(response.data)
        assert response_dt == {
            "Cancer Type": "string",
            "Component": "string",
            "Diagnosis": "string",
            "Family History": "string",
            "Patient ID": "Int64",
            "Sex": "string",
            "Year of Birth": "Int64",
            "entityId": "string",
        }

    @pytest.mark.synapse_credentials_needed
    # small manifest: syn51078535; big manifest: syn51156998
    @pytest.mark.parametrize(
        "manifest_id, expected_component, expected_file_name",
        [
            ("syn51078535", "BulkRNA-seqAssay", "synapse_storage_manifest.csv"),
            ("syn51156998", "Biospecimen", "synapse_storage_manifest_biospecimen.csv"),
        ],
    )
    @pytest.mark.parametrize("new_manifest_name", [None, "Example.csv"])
    @pytest.mark.parametrize("as_json", [None, True, False])
    def test_manifest_download(
        self,
        config: Configuration,
        client,
        request_headers,
        manifest_id,
        new_manifest_name,
        as_json,
        expected_component,
        expected_file_name,
    ):
        params = {
            "manifest_id": manifest_id,
            "new_manifest_name": new_manifest_name,
            "as_json": as_json,
        }

        response = client.get(
            "http://localhost:3001/v1/manifest/download",
            query_string=params,
            headers=request_headers,
        )
        assert response.status_code == 200

        # if as_json is set to True or as_json is not defined, then a json gets returned
        if as_json or as_json is None:
            response_dta = json.loads(response.data)

            # check if the correct manifest gets downloaded
            assert response_dta[0]["Component"] == expected_component

            current_work_dir = os.getcwd()
            folder_test_manifests = config.manifest_folder
            folder_dir = os.path.join(current_work_dir, folder_test_manifests)

            # if a manfiest gets renamed, get new manifest file path
            if new_manifest_name:
                manifest_file_path = os.path.join(
                    folder_dir, new_manifest_name + "." + "csv"
                )
            # if a manifest does not get renamed, get existing manifest file path
            else:
                manifest_file_path = os.path.join(folder_dir, expected_file_name)

        else:
            # manifest file path gets returned
            manifest_file_path = response.data.decode()

            file_base_name = os.path.basename(manifest_file_path)
            file_name = os.path.splitext(file_base_name)[0]

            if new_manifest_name:
                assert file_name == new_manifest_name

        # make sure file gets correctly downloaded
        assert os.path.exists(manifest_file_path)

        # delete files
        try:
            os.remove(manifest_file_path)
        except:
            pass

    @pytest.mark.synapse_credentials_needed
    # test downloading a manifest with access restriction and see if the correct error message got raised
    def test_download_access_restricted_manifest(self, client, request_headers):
        params = {"manifest_id": "syn29862078"}

        response = client.get(
            "http://localhost:3001/v1/manifest/download",
            query_string=params,
            headers=request_headers,
        )
        assert response.status_code == 500
        with pytest.raises(TypeError) as exc_info:
            raise TypeError("the type error got raised")
        assert exc_info.value.args[0] == "the type error got raised"

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.parametrize("as_json", [None, True, False])
    @pytest.mark.parametrize("new_manifest_name", [None, "Test"])
    def test_dataset_manifest_download(
        self, client, as_json, request_headers, new_manifest_name
    ):
        params = {
            "asset_view": "syn28559058",
            "dataset_id": "syn28268700",
            "as_json": as_json,
            "new_manifest_name": new_manifest_name,
        }

        response = client.get(
            "http://localhost:3001/v1/dataset/manifest/download",
            query_string=params,
            headers=request_headers,
        )
        assert response.status_code == 200
        response_dt = response.data

        if as_json:
            response_json = json.loads(response_dt)
            assert response_json[0]["Component"] == "BulkRNA-seqAssay"
            assert response_json[0]["File Format"] == "CSV/TSV"
            assert response_json[0]["Sample ID"] == 2022
            assert response_json[0]["entityId"] == "syn28278954"
        else:
            # return a file path
            response_path = response_dt.decode("utf-8")

            assert isinstance(response_path, str)
            assert response_path.endswith(".csv")

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    def test_submit_manifest_table_and_file_replace(
        self, client, request_headers, data_model_jsonld, test_manifest_submit
    ):
        """Testing submit manifest in a csv format as a table and a file. Only replace the table"""
        params = {
            "schema_url": data_model_jsonld,
            "data_type": "Biospecimen",
            "restrict_rules": False,
            "hide_blanks": False,
            "manifest_record_type": "table_and_file",
            "asset_view": "syn51514344",
            "dataset_id": "syn51514345",
            "table_manipulation": "replace",
            "data_model_labels": "class_label",
            "table_column_names": "class_label",
        }

        response_csv = client.post(
            "http://localhost:3001/v1/model/submit",
            query_string=params,
            data={"file_name": (open(test_manifest_submit, "rb"), "test.csv")},
            headers=request_headers,
        )
        assert response_csv.status_code == 200

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    @pytest.mark.parametrize(
        "data_type, manifest_path_fixture",
        [
            ("Biospecimen", "test_manifest_submit"),
            ("MockComponent", "test_manifest_csv"),
        ],
    )
    def test_submit_manifest_file_only_replace(
        self,
        helpers,
        client,
        request_headers,
        data_model_jsonld,
        data_type,
        manifest_path_fixture,
        request,
    ):
        """Testing submit manifest in a csv format as a file"""
        params = {
            "schema_url": data_model_jsonld,
            "data_type": data_type,
            "restrict_rules": False,
            "manifest_record_type": "file_only",
            "table_manipulation": "replace",
            "data_model_labels": "class_label",
            "table_column_names": "class_label",
        }

        if data_type == "Biospecimen":
            specific_params = {
                "asset_view": "syn51514344",
                "dataset_id": "syn51514345",
            }

        elif data_type == "MockComponent":
            python_version = helpers.get_python_version()

            if python_version == "3.10":
                dataset_id = "syn52656106"
            elif python_version == "3.9":
                dataset_id = "syn52656104"

            specific_params = {"asset_view": "syn23643253", "dataset_id": dataset_id, "project_scope":["syn54126707"]}

        params.update(specific_params)

        manifest_path = request.getfixturevalue(manifest_path_fixture)
        response_csv = client.post(
            "http://localhost:3001/v1/model/submit",
            query_string=params,
            data={"file_name": (open(manifest_path, "rb"), "test.csv")},
            headers=request_headers,
        )
        assert response_csv.status_code == 200

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    def test_submit_manifest_json_str_replace(
        self, client, request_headers, data_model_jsonld
    ):
        """Submit json str as a file"""
        json_str = '[{"Sample ID": 123, "Patient ID": 1,"Tissue Status": "Healthy","Component": "Biospecimen"}]'
        params = {
            "schema_url": data_model_jsonld,
            "data_type": "Biospecimen",
            "json_str": json_str,
            "restrict_rules": False,
            "manifest_record_type": "file_only",
            "asset_view": "syn51514344",
            "dataset_id": "syn51514345",
            "table_manipulation": "replace",
            "data_model_labels": "class_label",
            "table_column_names": "class_label",
        }
        params["json_str"] = json_str
        response = client.post(
            "http://localhost:3001/v1/model/submit",
            query_string=params,
            data={"file_name": ""},
            headers=request_headers,
        )
        assert response.status_code == 200

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    def test_submit_manifest_w_file_and_entities(
        self, client, request_headers, data_model_jsonld, test_manifest_submit
    ):
        params = {
            "schema_url": data_model_jsonld,
            "data_type": "Biospecimen",
            "restrict_rules": False,
            "manifest_record_type": "file_and_entities",
            "asset_view": "syn51514501",
            "dataset_id": "syn51514523",
            "table_manipulation": "replace",
            "data_model_labels": "class_label",
            "table_column_names": "class_label",
            "annotation_keys": "class_label",
        }

        # test uploading a csv file
        response_csv = client.post(
            "http://localhost:3001/v1/model/submit",
            query_string=params,
            data={"file_name": (open(test_manifest_submit, "rb"), "test.csv")},
            headers=request_headers,
        )
        assert response_csv.status_code == 200

    @pytest.mark.synapse_credentials_needed
    @pytest.mark.submission
    def test_submit_manifest_table_and_file_upsert(
        self,
        client,
        request_headers,
        data_model_jsonld,
        test_upsert_manifest_csv,
    ):
        params = {
            "schema_url": data_model_jsonld,
            "data_type": "MockRDB",
            "restrict_rules": False,
            "manifest_record_type": "table_and_file",
            "asset_view": "syn51514557",
            "dataset_id": "syn51514551",
            "table_manipulation": "upsert",
            "data_model_labels": "class_label",
            "table_column_names": "display_name",  # have to set table_column_names to display_name to ensure upsert feature works
        }

        # test uploading a csv file
        response_csv = client.post(
            "http://localhost:3001/v1/model/submit",
            query_string=params,
            data={"file_name": (open(test_upsert_manifest_csv, "rb"), "test.csv")},
            headers=request_headers,
        )
        assert response_csv.status_code == 200


@pytest.mark.schematic_api
class TestSchemaVisualization:
    def test_visualize_attributes(self, client, data_model_jsonld):
        params = {"schema_url": data_model_jsonld}

        response = client.get(
            "http://localhost:3001/v1/visualize/attributes", query_string=params
        )

        assert response.status_code == 200

    @pytest.mark.parametrize("figure_type", ["component", "dependency"])
    def test_visualize_tangled_tree_layers(
        self, client, figure_type, data_model_jsonld
    ):
        # TODO: Determine a 2nd data model to use for this test, test both models sequentially, add checks for content of response
        params = {"schema_url": data_model_jsonld, "figure_type": figure_type}

        response = client.get(
            "http://localhost:3001/v1/visualize/tangled_tree/layers",
            query_string=params,
        )

        assert response.status_code == 200

    @pytest.mark.parametrize(
        "component, response_text",
        [
            ("Patient", "Component,Component,TBD,True,,,,Patient"),
            ("BulkRNA-seqAssay", "Component,Component,TBD,True,,,,BulkRNA-seqAssay"),
        ],
    )
    def test_visualize_component(
        self, client, data_model_jsonld, component, response_text
    ):
        params = {
            "schema_url": data_model_jsonld,
            "component": component,
            "include_index": False,
            "data_model_labels": "class_label",
        }

        response = client.get(
            "http://localhost:3001/v1/visualize/component", query_string=params
        )

        assert response.status_code == 200
        assert (
            "Attribute,Label,Description,Required,Cond_Req,Valid Values,Conditional Requirements,Component"
            in response.text
        )
        assert response_text in response.text


@pytest.mark.schematic_api
@pytest.mark.rule_benchmark
class TestValidationBenchmark:
    @pytest.mark.parametrize("MockComponent_attribute", get_MockComponent_attribute())
    def test_validation_performance(
        self,
        helpers,
        benchmark_data_model_jsonld,
        client,
        test_invalid_manifest,
        MockComponent_attribute,
    ):
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
        endpoint_url = "http://localhost:3001/v1/model/validate"

        # Set paramters for endpoint
        params = {
            "schema_url": benchmark_data_model_jsonld,
            "data_type": "MockComponent",
        }
        headers = {"Content-Type": "multipart/form-data", "Accept": "application/json"}

        # Enforce error rate when possible
        if MockComponent_attribute == "Check Ages":
            test_invalid_manifest.loc[0, MockComponent_attribute] = "6550"
        elif MockComponent_attribute == "Check Date":
            test_invalid_manifest.loc[0, MockComponent_attribute] = "October 21 2022"
            test_invalid_manifest.loc[2, MockComponent_attribute] = "October 21 2022"
        elif MockComponent_attribute == "Check Unique":
            test_invalid_manifest.loc[0, MockComponent_attribute] = "str2"

        # Isolate single attribute of interest, keep `Component` column
        single_attribute_manfiest = test_invalid_manifest[
            ["Component", MockComponent_attribute]
        ]

        # Extend to ~1000 rows in size to for performance test
        multi_factor = ceil(target_rows / single_attribute_manfiest.shape[0])
        large_manfiest = pd.concat(
            [single_attribute_manfiest] * multi_factor, ignore_index=True
        )

        try:
            # Convert manfiest to csv for api endpoint
            large_manifest_path = helpers.get_data_path(
                "mock_manifests/large_manifest_test.csv"
            )
            large_manfiest.to_csv(large_manifest_path, index=False)

            # Run and time endpoint
            t_start = perf_counter()
            response = client.post(
                endpoint_url,
                query_string=params,
                data={"file_name": (open(large_manifest_path, "rb"), "large_test.csv")},
                headers=headers,
            )
            response_time = perf_counter() - t_start
        finally:
            # Remove temp manfiest
            os.remove(large_manifest_path)

        # Log and check time and ensure successful response
        logger.warning(
            f"validation endpiont response time {round(response_time,2)} seconds."
        )
        assert response.status_code == 200
        assert response_time < 5.00
