import json
import logging
import os
import shutil
import tempfile
import time
from datetime import datetime
from unittest import mock

import numpy as np
import pandas as pd
import pytest
import synapseclient
import synapseclient.core.cache as cache
from pandas.testing import assert_frame_equal
from synapseclient.core.exceptions import SynapseHTTPError

from schematic import LOADER
from schematic.exceptions import (MissingConfigAndArgumentValueError,
                                  MissingConfigValueError)
from schematic.schemas import df_parser
from schematic.schemas.explorer import SchemaExplorer
from schematic.store.synapse import SynapseStorage
from schematic.utils import (cli_utils, df_utils, general, io_utils,
                             validate_utils)
from schematic.utils.general import (calculate_datetime,
                                     check_synapse_cache_size,
                                     clear_synapse_cache, entity_type_mapping)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS")

@pytest.fixture
def synapse_store():
    access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")
    if access_token:
        synapse_store = SynapseStorage(access_token=access_token)
    else:
        synapse_store = SynapseStorage()
    yield synapse_store

class TestGeneral:
    def test_clear_synapse_cache(self, tmp_path):
        # define location of mock synapse cache
        mock_synapse_cache_dir = tmp_path / ".synapseCache/"
        mock_synapse_cache_dir.mkdir()
        mock_sub_folder = mock_synapse_cache_dir / "123"
        mock_sub_folder.mkdir()
        mock_table_query_folder = mock_sub_folder/ "456"
        mock_table_query_folder.mkdir()

        # create mock table query csv and a mock cache map
        mock_synapse_table_query_csv = mock_table_query_folder/ "mock_synapse_table_query.csv"
        mock_synapse_table_query_csv.write_text("mock table query content")
        mock_cache_map = mock_table_query_folder/ ".cacheMap"
        mock_cache_map.write_text(f"{mock_synapse_table_query_csv}: '2022-06-13T19:24:27.000Z'")

        assert os.path.exists(mock_synapse_table_query_csv)

        # since synapse python client would compare last modified date and before date
        # we have to create a little time gap here
        time.sleep(1)

        # clear cache
        my_cache = cache.Cache(cache_root_dir=mock_synapse_cache_dir)
        clear_synapse_cache(my_cache, minutes=0.0001)
        # make sure that cache files are now gone
        assert os.path.exists(mock_synapse_table_query_csv) == False
        assert os.path.exists(mock_cache_map) == False

    def test_calculate_datetime(self):
        input_date = datetime.strptime("07/20/23 17:36:34", '%m/%d/%y %H:%M:%S')
        minutes_before = calculate_datetime(input_date=input_date, minutes=10, before_or_after="before")
        expected_result_date_before = datetime.strptime("07/20/23 17:26:34", '%m/%d/%y %H:%M:%S')

        minutes_after = calculate_datetime(input_date=input_date, minutes=10, before_or_after="after")
        expected_result_date_after = datetime.strptime("07/20/23 17:46:34", '%m/%d/%y %H:%M:%S')
        assert minutes_before == expected_result_date_before
        assert minutes_after == expected_result_date_after
        
    def test_check_synapse_cache_size(self,tmp_path):
        mock_synapse_cache_dir = tmp_path / ".synapseCache"
        mock_synapse_cache_dir.mkdir()

        mock_synapse_table_query_csv = mock_synapse_cache_dir/ "mock_synapse_table_query.csv"
        mock_synapse_table_query_csv.write_text("example file for calculating cache")

        file_size = check_synapse_cache_size(mock_synapse_cache_dir)

        # For some reasons, when running in github action, the size of file changes.
        if IN_GITHUB_ACTIONS:
            assert file_size == 8000
        else:
            assert file_size == 4000

    def test_find_duplicates(self):

        mock_list = ["foo", "bar", "foo"]
        mock_dups = {"foo"}

        test_dups = general.find_duplicates(mock_list)
        assert test_dups == mock_dups

    def test_dict2list_with_dict(self):

        mock_dict = {"foo": "bar"}
        mock_list = [{"foo": "bar"}]

        test_list = general.dict2list(mock_dict)
        assert test_list == mock_list

    def test_dict2list_with_list(self):

        # mock_dict = {'foo': 'bar'}
        mock_list = [{"foo": "bar"}]

        test_list = general.dict2list(mock_list)
        assert test_list == mock_list

    @pytest.mark.parametrize("entity_id,expected_type", [("syn27600053","folder"), ("syn29862078", "file"), ("syn23643253", "asset view"), ("syn30988314", "folder"), ("syn51182432", "org.sagebionetworks.repo.model.table.TableEntity")])
    def test_entity_type_mapping(self, synapse_store, entity_id, expected_type):
        syn = synapse_store.syn

        entity_type = entity_type_mapping(syn, entity_id)
        assert entity_type == expected_type

    def test_entity_type_mapping_invalid_entity_id(self, synapse_store):
        syn = synapse_store.syn

        # test with an invalid entity id
        with pytest.raises(SynapseHTTPError) as exception_info:
            entity_type_mapping(syn, "syn123456")

    def test_download_manifest_to_temp_folder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path_dir = general.create_temp_folder(tmpdir)
            assert os.path.exists(path_dir)


class TestCliUtils:
    def test_query_dict(self):

        mock_dict = {"k1": {"k2": {"k3": "foobar"}}}
        mock_keys_valid = ["k1", "k2", "k3"]
        mock_keys_invalid = ["k1", "k2", "k4"]

        test_result_valid = cli_utils.query_dict(mock_dict, mock_keys_valid)
        test_result_invalid = cli_utils.query_dict(mock_dict, mock_keys_invalid)

        assert test_result_valid == "foobar"
        assert test_result_invalid is None


class FakeResponse:
    status: int
    data: bytes

    def __init__(self, *, data: bytes):
        self.data = data

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def read(self):
        self.status = 200 if self.data is not None else 404
        return self.data

    def close(self):
        pass


class TestIOUtils:
    def test_json_load(self, tmpdir):

        json_file = tmpdir.join("example.json")
        json_file.write_text(json.dumps([{"k1": "v1"}, {"k2": "v2"}]), encoding="utf-8")

        with open(json_file, encoding="utf-8") as f:
            expected = json.load(f)

        local_result = io_utils.load_json(str(json_file))

        assert local_result == expected

    def test_json_load_online(self, mocker):

        mock_urlopen = mocker.patch(
            "urllib.request.urlopen",
            return_value=FakeResponse(
                data=json.dumps([{"k1": "v1"}, {"k2": "v2"}]).encode("utf-8")
            ),
        )

        url_result = io_utils.load_json("http://example.com")
        assert url_result == [{"k1": "v1"}, {"k2": "v2"}]

        assert mock_urlopen.call_count == 1

    def test_export_json(self, tmpdir):

        json_str = json.dumps([{"k1": "v1"}, {"k2": "v2"}])

        export_file = tmpdir.join("export_json_expected.json")
        io_utils.export_json(json_str, export_file)

        with open(export_file, encoding="utf-8") as f:
            expected = json.load(f)

        assert expected == json_str

    def test_load_default(self):

        biothings_schema = io_utils.load_default()

        expected_ctx_keys = ["bts", "rdf", "rdfs", "schema", "xsd"]
        actual_ctx_keys = list(biothings_schema["@context"].keys())
        assert expected_ctx_keys == actual_ctx_keys

        expected_no_of_keys = 146
        actual_no_of_keys = len(biothings_schema["@graph"])
        assert expected_no_of_keys == actual_no_of_keys

    def test_load_schema_org(self):

        schema_org_schema = io_utils.load_schemaorg()

        expected_ctx_keys = ['brick', 'csvw', 'dc', 'dcam', 'dcat', 'dcmitype', 'dcterms', 'doap', 'foaf', 'odrl', 'org', 'owl', 'prof', 'prov', 'qb', 'rdf', 'rdfs', 'schema', 'sh', 'skos', 'sosa', 'ssn', 'time', 'vann', 'void', 'xsd']
        actual_ctx_keys = list(schema_org_schema["@context"].keys())
        assert expected_ctx_keys == actual_ctx_keys

        expected_graph_keys = 2801
        actual_graph_keys = len(schema_org_schema["@graph"])
        assert expected_graph_keys == actual_graph_keys


class TestDfUtils:
    def test_update_df_col_present(self, helpers):

        synapse_manifest = helpers.get_data_frame(
            "mock_manifests", "synapse_manifest.csv"
        )

        local_manifest = helpers.get_data_frame("mock_manifests", "local_manifest.csv")

        col_pres_res = df_utils.update_df(local_manifest, synapse_manifest, "entityId")

        assert_frame_equal(col_pres_res, synapse_manifest)

    def test_update_df_col_absent(self, helpers):

        synapse_manifest = helpers.get_data_frame(
            "mock_manifests", "synapse_manifest.csv"
        )

        local_manifest = helpers.get_data_frame("mock_manifests", "local_manifest.csv")

        with pytest.raises(AssertionError):
            df_utils.update_df(local_manifest, synapse_manifest, "Col_Not_In_Dfs")

    def test_trim_commas_df(self, helpers):

        local_manifest = helpers.get_data_frame("mock_manifests", "local_manifest.csv")

        nan_row = pd.DataFrame(
            [[np.nan] * len(local_manifest.columns)], columns=local_manifest.columns
        )

        df_with_nans = local_manifest.append(nan_row, ignore_index=True)

        df_with_nans["Unnamed: 1"] = np.nan
        trimmed_df = df_utils.trim_commas_df(df_with_nans)

        assert_frame_equal(trimmed_df, local_manifest)

    def test_update_dataframe(self):
        input_df = pd.DataFrame(
            {
                "numCol": [1, 2],
                "entityId": ["syn01", "syn02"],
                "strCol": ["foo", "bar"],
            },
            columns=["numCol", "entityId", "strCol"],
        )
        updates_df = pd.DataFrame(
            {
                "strCol": ["___", np.nan],
                "numCol": [np.nan, 4],
                "entityId": ["syn01", "syn02"],
            },
            columns=["strCol", "numCol", "entityId"],
        )
        expected_df = pd.DataFrame(
            {
                "numCol": [int(1), int(4)],
                "entityId": ["syn01", "syn02"],
                "strCol": ["___", "bar"],
            },
            columns=["numCol", "entityId", "strCol"],
        )

        actual_df = df_utils.update_df(input_df, updates_df, "entityId")
        pd.testing.assert_frame_equal(expected_df, actual_df)

    def test_populate_column(self):
        input_df = pd.DataFrame(
            {
                "column1": ["col1Val","col1Val"],
                "column2": [None, None]
            }
        )

        output_df = df_utils.populate_df_col_with_another_col(input_df,'column1','column2')
        assert (output_df["column2"].values == ["col1Val","col1Val"]).all()


class TestValidateUtils:
    def test_validate_schema(self, helpers):

        se_obj = helpers.get_schema_explorer("example.model.jsonld")

        actual = validate_utils.validate_schema(se_obj.schema)

        assert actual is None

    def test_validate_class_schema(self, helpers):

        se_obj = helpers.get_schema_explorer("example.model.jsonld")

        mock_class = se_obj.generate_class_template()
        mock_class["@id"] = "bts:MockClass"
        mock_class["@type"] = "rdfs:Class"
        mock_class["@rdfs:comment"] = "This is a mock class"
        mock_class["@rdfs:label"] = "MockClass"
        mock_class["rdfs:subClassOf"]["@id"] = "bts:Patient"

        actual = validate_utils.validate_class_schema(mock_class)

        assert actual is None

    def test_validate_property_schema(self, helpers):

        se_obj = helpers.get_schema_explorer("example.model.jsonld")

        mock_class = se_obj.generate_property_template()
        mock_class["@id"] = "bts:MockProperty"
        mock_class["@type"] = "rdf:Property"
        mock_class["@rdfs:comment"] = "This is a mock Patient class"
        mock_class["@rdfs:label"] = "MockProperty"
        mock_class["schema:domainIncludes"]["@id"] = "bts:Patient"

        actual = validate_utils.validate_property_schema(mock_class)

        assert actual is None


class TestCsvUtils:
    def test_csv_to_schemaorg(self, helpers, tmp_path):
        """Test the CSV-to-JSON-LD conversion.

        This test also ensures that the CSV and JSON-LD
        files for the example data model stay in sync.
        """
        csv_path = helpers.get_data_path("example.model.csv")

        base_se = df_parser._convert_csv_to_data_model(csv_path)

        # saving updated schema.org schema
        actual_jsonld_path = tmp_path / "example.from_csv.model.jsonld"
        base_se.export_schema(actual_jsonld_path)

        # Compare both JSON-LD files
        expected_jsonld_path = helpers.get_data_path("example.model.jsonld")
        expected_jsonld = open(expected_jsonld_path).read()
        actual_jsonld = open(actual_jsonld_path).read()
        assert expected_jsonld == actual_jsonld
