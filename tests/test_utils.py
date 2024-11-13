import copy
import json
import logging
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Union

import numpy as np
import pandas as pd
import pytest
import synapseclient.core.cache as cache
from _pytest.fixtures import FixtureRequest
from pandas.testing import assert_frame_equal
from synapseclient.core.exceptions import SynapseHTTPError

from schematic.models.metadata import MetadataModel
from schematic.models.validate_manifest import ValidateManifest
from schematic.schemas.data_model_graph import DataModelGraph
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.schemas.data_model_jsonld import (
    ClassTemplate,
    PropertyTemplate,
    convert_graph_to_jsonld,
)
from schematic.schemas.data_model_parser import DataModelParser
from schematic.utils import cli_utils, df_utils, general, io_utils, validate_utils
from schematic.utils.df_utils import load_df
from schematic.utils.general import (
    calculate_datetime,
    check_synapse_cache_size,
    clear_synapse_cache,
    entity_type_mapping,
)
from schematic.utils.schema_utils import (
    check_for_duplicate_components,
    check_if_display_name_is_valid_label,
    export_schema,
    extract_component_validation_rules,
    get_class_label_from_display_name,
    get_component_name_rules,
    get_individual_rules,
    get_json_schema_log_file_path,
    get_label_from_display_name,
    get_property_label_from_display_name,
    get_schema_label,
    get_stripped_label,
    parse_single_set_validation_rules,
    parse_validation_rules,
    strip_context,
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS")

RULE_MODIFIERS = ["error", "warning", "strict", "like", "set", "value"]
VALIDATION_EXPECTATION = {
    "int": "expect_column_values_to_be_in_type_list",
    "float": "expect_column_values_to_be_in_type_list",
    "str": "expect_column_values_to_be_of_type",
    "num": "expect_column_values_to_be_in_type_list",
    "date": "expect_column_values_to_be_dateutil_parseable",
    "recommended": "expect_column_values_to_not_be_null",
    "protectAges": "expect_column_values_to_be_between",
    "unique": "expect_column_values_to_be_unique",
    "inRange": "expect_column_values_to_be_between",
    "IsNA": "expect_column_values_to_match_regex_list",
    # To be implemented rules with possible expectations
    # "list": "expect_column_values_to_not_match_regex_list",
    # "regex": "expect_column_values_to_match_regex",
    # "url": "expect_column_values_to_be_valid_urls",
    # "matchAtLeastOne": "expect_foreign_keys_in_column_a_to_exist_in_column_b",
    # "matchExactlyOne": "expect_foreign_keys_in_column_a_to_exist_in_column_b",
    # "matchNone": "expect_compound_columns_to_be_unique",
}

MULTI_RULE_DICT = {
    "multi_rule": {
        "starting_rule": "unique::list::num",
        "parsed_rule": [["unique", "list", "num"]],
    },
    "double_rule": {
        "starting_rule": "unique::list",
        "parsed_rule": [["unique", "list"]],
    },
    "single_rule": {"starting_rule": "unique", "parsed_rule": ["unique"]},
}

TEST_VALIDATION_RULES = {
    "multi_component_rule": {
        "validation_rules": [
            "#Patient int^^#Biospecimen unique error^^#BulkRNA-seqAssay int"
        ],
        "parsed_rules": {
            "Patient": "int",
            "Biospecimen": "unique error",
            "BulkRNA-seqAssay": "int",
        },
        "extracted_rules": {
            "Patient": ["int"],
            "Biospecimen": ["unique error"],
            "BulkRNA-seqAssay": ["int"],
        },
    },
    "double_component_rule": {
        "validation_rules": ["#Patient int^^#Biospecimen unique error"],
        "parsed_rules": {"Patient": "int", "Biospecimen": "unique error"},
        "extracted_rules": {"Patient": ["int"], "Biospecimen": ["unique error"]},
    },
    "single_component_rule_1": {
        "validation_rules": ["#Patient int^^"],
        "parsed_rules": {"Patient": "int"},
        "extracted_rules": {"Patient": ["int"]},
    },
    "single_component_rule_2": {
        "validation_rules": ["^^#Patient int"],
        "parsed_rules": {"Patient": "int"},
        "extracted_rules": {"Patient": ["int"]},
    },
    "single_component_exclusion": {
        "validation_rules": ["int::inRange 100 900^^#Patient"],
        "parsed_rules": {
            "all_other_components": ["int", "inRange 100 900"],
            "Patient": "",
        },
        "extracted_rules": {
            "all_other_components": ["int", "inRange 100 900"],
            "Patient": [],
        },
    },
    "dictionary_rule": {
        "validation_rules": {"BiospecimenManifest": "unique error", "Patient": "int"},
        "parsed_rules": {"BiospecimenManifest": "unique error", "Patient": "int"},
        "extracted_rules": {
            "BiospecimenManifest": ["unique error"],
            "Patient": ["int"],
        },
    },
    "str_rule": {
        "validation_rules": "#Patient int^^#Biospecimen unique error",
        "parsed_rules": "raises_exception",
    },
    "simple_rule": {
        "validation_rules": ["int"],
        "parsed_rules": ["int"],
    },
    "double_rule": {
        "validation_rules": ["list::regex match \(\d{3}\) \d{3}-\d{4}"],
        "parsed_rules": ["list", "regex match \(\d{3}\) \d{3}-\d{4}"],
    },
    "duplicated_component": {
        "validation_rules": ["#Patient unique^^#Patient int"],
        "parsed_rules": "raises_exception",
    },
}

TEST_DN_DICT = {
    "Bio Things": {"class": "BioThings", "property": "bioThings"},
    "bio things": {"class": "Biothings", "property": "biothings"},
    "BioThings": {"class": "BioThings", "property": "bioThings"},
    "Bio-things": {"class": "Biothings", "property": "biothings"},
    "bio_things": {"class": "BioThings", "property": "bioThings"},
}

DATA_MODEL_DICT = {"example.model.csv": "CSV", "example.model.jsonld": "JSONLD"}

test_disk_storage = [
    (2, 2),
    (1000, 1000),
    (2000000, 2000000),
    (1073741825, 1073741825),
]


def get_metadataModel(helpers, model_name: str):
    metadataModel = MetadataModel(
        inputMModelLocation=helpers.get_data_path(model_name),
        inputMModelLocationType="local",
        data_model_labels="class_label",
    )
    return metadataModel


# create temporary files with various size based on request
@pytest.fixture()
def create_temp_query_file(
    tmp_path: Path, request: FixtureRequest
) -> Generator[tuple[Path, Path, Path], None, None]:
    """create temporary files of various size based on request parameter.

    Args:
        tmp_path (Path): temporary file path
        request (any): a request for a fixture from a test

    Yields:
        Generator[Tuple[Path, Path, Path]]: return path of mock synapse cache directory, mock table query folder and csv
    """
    # define location of mock synapse cache
    mock_synapse_cache_dir = tmp_path / ".synapseCache/"
    mock_synapse_cache_dir.mkdir()
    mock_sub_folder = mock_synapse_cache_dir / "123"
    mock_sub_folder.mkdir()
    mock_table_query_folder = mock_sub_folder / "456"
    mock_table_query_folder.mkdir()

    # create mock table query csv
    mock_synapse_table_query_csv = (
        mock_table_query_folder / "mock_synapse_table_query.csv"
    )
    with open(mock_synapse_table_query_csv, "wb") as f:
        f.write(b"\0" * request.param)
    yield mock_synapse_cache_dir, mock_table_query_folder, mock_synapse_table_query_csv


class TestGeneral:
    @pytest.mark.parametrize("create_temp_query_file", [3, 1000], indirect=True)
    def test_clear_synapse_cache(self, create_temp_query_file) -> None:
        # define location of mock synapse cache
        (
            mock_synapse_cache_dir,
            mock_table_query_folder,
            mock_synapse_table_query_csv,
        ) = create_temp_query_file
        # create a mock cache map
        mock_cache_map = mock_table_query_folder / ".cacheMap"
        mock_cache_map.write_text(
            f"{mock_synapse_table_query_csv}: '2022-06-13T19:24:27.000Z'"
        )

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

    def test_calculate_datetime_before_minutes(self):
        input_date = datetime.strptime("07/20/23 17:36:34", "%m/%d/%y %H:%M:%S")
        minutes_before = calculate_datetime(
            input_date=input_date, minutes=10, before_or_after="before"
        )
        expected_result_date_before = datetime.strptime(
            "07/20/23 17:26:34", "%m/%d/%y %H:%M:%S"
        )
        assert minutes_before == expected_result_date_before

    def test_calculate_datetime_after_minutes(self):
        input_date = datetime.strptime("07/20/23 17:36:34", "%m/%d/%y %H:%M:%S")
        minutes_after = calculate_datetime(
            input_date=input_date, minutes=10, before_or_after="after"
        )
        expected_result_date_after = datetime.strptime(
            "07/20/23 17:46:34", "%m/%d/%y %H:%M:%S"
        )
        assert minutes_after == expected_result_date_after

    def test_calculate_datetime_raise_error(self):
        with pytest.raises(ValueError):
            input_date = datetime.strptime("07/20/23 17:36:34", "%m/%d/%y %H:%M:%S")
            minutes = calculate_datetime(
                input_date=input_date, minutes=10, before_or_after="error"
            )

    # this test might fail for windows machine
    @pytest.mark.parametrize(
        "create_temp_query_file,local_disk_size",
        test_disk_storage,
        indirect=["create_temp_query_file"],
    )
    def test_check_synapse_cache_size(
        self,
        create_temp_query_file,
        local_disk_size: int,
    ) -> None:
        mock_synapse_cache_dir, _, _ = create_temp_query_file
        disk_size = check_synapse_cache_size(mock_synapse_cache_dir)
        assert disk_size == local_disk_size

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

    @pytest.mark.parametrize(
        "entity_id,expected_type",
        [
            ("syn27600053", "folder"),
            ("syn29862078", "file"),
            ("syn23643253", "asset view"),
            ("syn30988314", "folder"),
            ("syn51182432", "org.sagebionetworks.repo.model.table.TableEntity"),
        ],
    )
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

        expected_ctx_keys = [
            "brick",
            "csvw",
            "dc",
            "dcam",
            "dcat",
            "dcmitype",
            "dcterms",
            "doap",
            "foaf",
            "odrl",
            "org",
            "owl",
            "prof",
            "prov",
            "qb",
            "rdf",
            "rdfs",
            "schema",
            "sh",
            "skos",
            "sosa",
            "ssn",
            "time",
            "vann",
            "void",
            "xsd",
        ]
        actual_ctx_keys = list(schema_org_schema["@context"].keys())
        assert expected_ctx_keys == actual_ctx_keys

        expected_graph_keys = 2801
        actual_graph_keys = len(schema_org_schema["@graph"])
        assert expected_graph_keys == actual_graph_keys


class TestDfUtils:
    @pytest.mark.parametrize(
        "preserve_raw_input",
        [True, False],
        ids=["Do not infer datatypes", "Infer datatypes"],
    )
    def test_load_df(self, helpers, preserve_raw_input):
        test_col = "Check NA"
        file_path = helpers.get_data_path("mock_manifests", "Invalid_Test_Manifest.csv")

        unprocessed_df = pd.read_csv(file_path, encoding="utf8")
        df = df_utils.load_df(
            file_path, preserve_raw_input=preserve_raw_input, data_model=False
        )

        assert df["Component"].dtype == "object"

        n_unprocessed_rows = unprocessed_df.shape[0]
        n_processed_rows = df.shape[0]

        assert n_unprocessed_rows == 4
        assert n_processed_rows == 3

        if preserve_raw_input:
            assert isinstance(df[test_col].iloc[0], str)
            assert isinstance(df[test_col].iloc[1], str)
            assert isinstance(df[test_col].iloc[2], str)
        else:
            assert isinstance(df[test_col].iloc[0], np.int64)
            assert isinstance(df[test_col].iloc[1], float)
            assert isinstance(df[test_col].iloc[2], str)

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

        df_with_nans = pd.concat([local_manifest, nan_row], ignore_index=True)

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
            {"column1": ["col1Val", "col1Val"], "column2": [None, None]}
        )

        output_df = df_utils.populate_df_col_with_another_col(
            input_df, "column1", "column2"
        )
        assert (output_df["column2"].values == ["col1Val", "col1Val"]).all()


class TestSchemaUtils:
    def test_get_property_label_from_display_name(self, helpers):
        # tests where strict_camel_case is the same
        assert get_property_label_from_display_name("howToAcquire") == "howToAcquire"
        assert (
            get_property_label_from_display_name("howToAcquire", strict_camel_case=True)
            == "howToAcquire"
        )
        assert get_property_label_from_display_name("how_to_acquire") == "howToAcquire"
        assert (
            get_property_label_from_display_name(
                "how_to_acquire", strict_camel_case=True
            )
            == "howToAcquire"
        )
        assert get_property_label_from_display_name("howtoAcquire") == "howtoAcquire"
        assert (
            get_property_label_from_display_name("howtoAcquire", strict_camel_case=True)
            == "howtoAcquire"
        )
        assert get_property_label_from_display_name("How To Acquire") == "howToAcquire"
        assert (
            get_property_label_from_display_name(
                "How To Acquire", strict_camel_case=True
            )
            == "howToAcquire"
        )
        assert (
            get_property_label_from_display_name("Model Of Manifestation")
            == "modelOfManifestation"
        )
        assert (
            get_property_label_from_display_name(
                "Model Of Manifestation", strict_camel_case=True
            )
            == "modelOfManifestation"
        )
        assert (
            get_property_label_from_display_name("ModelOfManifestation")
            == "modelOfManifestation"
        )
        assert (
            get_property_label_from_display_name(
                "ModelOfManifestation", strict_camel_case=True
            )
            == "modelOfManifestation"
        )
        assert (
            get_property_label_from_display_name("model Of Manifestation")
            == "modelOfManifestation"
        )
        assert (
            get_property_label_from_display_name(
                "model Of Manifestation", strict_camel_case=True
            )
            == "modelOfManifestation"
        )

        # tests where strict_camel_case changes the result
        assert get_property_label_from_display_name("how to Acquire") == "howtoAcquire"
        assert (
            get_property_label_from_display_name(
                "how to Acquire", strict_camel_case=True
            )
            == "howToAcquire"
        )
        assert get_property_label_from_display_name("How to Acquire") == "howtoAcquire"
        assert (
            get_property_label_from_display_name(
                "How to Acquire", strict_camel_case=True
            )
            == "howToAcquire"
        )
        assert get_property_label_from_display_name("how to acquire") == "howtoacquire"
        assert (
            get_property_label_from_display_name(
                "how to acquire", strict_camel_case=True
            )
            == "howToAcquire"
        )
        assert (
            get_property_label_from_display_name("model of manifestation")
            == "modelofmanifestation"
        )
        assert (
            get_property_label_from_display_name(
                "model of manifestation", strict_camel_case=True
            )
            == "modelOfManifestation"
        )
        assert (
            get_property_label_from_display_name("model of manifestation")
            == "modelofmanifestation"
        )
        assert (
            get_property_label_from_display_name(
                "model of manifestation", strict_camel_case=True
            )
            == "modelOfManifestation"
        )

    def test_get_class_label_from_display_name(self, helpers):
        # tests where strict_camel_case is the same
        assert get_class_label_from_display_name("howToAcquire") == "HowToAcquire"
        assert (
            get_class_label_from_display_name("howToAcquire", strict_camel_case=True)
            == "HowToAcquire"
        )
        assert get_class_label_from_display_name("how_to_acquire") == "HowToAcquire"
        assert (
            get_class_label_from_display_name("how_to_acquire", strict_camel_case=True)
            == "HowToAcquire"
        )
        assert get_class_label_from_display_name("howtoAcquire") == "HowtoAcquire"
        assert (
            get_class_label_from_display_name("howtoAcquire", strict_camel_case=True)
            == "HowtoAcquire"
        )
        assert get_class_label_from_display_name("How To Acquire") == "HowToAcquire"
        assert (
            get_class_label_from_display_name("How To Acquire", strict_camel_case=True)
            == "HowToAcquire"
        )
        assert (
            get_class_label_from_display_name("Model Of Manifestation")
            == "ModelOfManifestation"
        )
        assert (
            get_class_label_from_display_name(
                "Model Of Manifestation", strict_camel_case=True
            )
            == "ModelOfManifestation"
        )
        assert (
            get_class_label_from_display_name("ModelOfManifestation")
            == "ModelOfManifestation"
        )
        assert (
            get_class_label_from_display_name(
                "ModelOfManifestation", strict_camel_case=True
            )
            == "ModelOfManifestation"
        )
        assert (
            get_class_label_from_display_name("model Of Manifestation")
            == "ModelOfManifestation"
        )
        assert (
            get_class_label_from_display_name(
                "model Of Manifestation", strict_camel_case=True
            )
            == "ModelOfManifestation"
        )

        # tests where strict_camel_case changes the result
        assert get_class_label_from_display_name("how to Acquire") == "HowtoAcquire"
        assert (
            get_class_label_from_display_name("how to Acquire", strict_camel_case=True)
            == "HowToAcquire"
        )
        assert get_class_label_from_display_name("How to Acquire") == "HowtoAcquire"
        assert (
            get_class_label_from_display_name("How to Acquire", strict_camel_case=True)
            == "HowToAcquire"
        )
        assert get_class_label_from_display_name("how to acquire") == "Howtoacquire"
        assert (
            get_class_label_from_display_name("how to acquire", strict_camel_case=True)
            == "HowToAcquire"
        )
        assert (
            get_class_label_from_display_name("model of manifestation")
            == "Modelofmanifestation"
        )
        assert (
            get_class_label_from_display_name(
                "model of manifestation", strict_camel_case=True
            )
            == "ModelOfManifestation"
        )
        assert (
            get_class_label_from_display_name("model of manifestation")
            == "Modelofmanifestation"
        )
        assert (
            get_class_label_from_display_name(
                "model of manifestation", strict_camel_case=True
            )
            == "ModelOfManifestation"
        )

    @pytest.mark.parametrize(
        "context_value", ["@id", "sms:required"], ids=["remove_at", "remove_sms"]
    )
    def test_strip_context(self, helpers, context_value):
        stripped_contex = strip_context(context_value=context_value)
        if "@id" == context_value:
            assert stripped_contex == ("", "id")
        elif "sms:required" == context_value:
            assert stripped_contex == ("sms", "required")

    @pytest.mark.parametrize(
        "test_multi_rule",
        list(MULTI_RULE_DICT.keys()),
        ids=list(MULTI_RULE_DICT.keys()),
    )
    def test_get_individual_rules(self, test_multi_rule):
        validation_rules = []
        test_rule = MULTI_RULE_DICT[test_multi_rule]["starting_rule"]
        expected_rule = MULTI_RULE_DICT[test_multi_rule]["parsed_rule"]
        parsed_rule = get_individual_rules(
            rule=test_rule,
            validation_rules=validation_rules,
        )
        assert expected_rule == parsed_rule

    @pytest.mark.parametrize(
        "test_individual_component_rule",
        [
            ["#Patient int", [["Patient"], "int"]],
            ["int", [["all_other_components"], "int"]],
        ],
        ids=["Patient_component", "no_component"],
    )
    def test_get_component_name_rules(self, test_individual_component_rule):
        component_names = []

        component, parsed_rule = get_component_name_rules(
            component_names=[], component_rule=test_individual_component_rule[0]
        )
        expected_rule = test_individual_component_rule[1][1]
        expected_component = test_individual_component_rule[1][0]

        assert expected_rule == parsed_rule
        assert expected_component == component

    @pytest.mark.parametrize(
        "test_individual_rule_set",
        [
            ["#Patient int::inRange 100 900", []],
            ["int::inRange 100 900", ["int", "inRange 100 900"]],
            ["int", ["int"]],
        ],
        ids=["improper_format", "double_rule", "single_rule"],
    )
    def test_parse_single_set_validation_rules(self, test_individual_rule_set):
        validation_rule_string = test_individual_rule_set[0]
        try:
            parsed_rule = parse_single_set_validation_rules(
                validation_rule_string=validation_rule_string
            )
            expected_rule = test_individual_rule_set[1]
            assert parsed_rule == expected_rule
        except:
            assert validation_rule_string == "#Patient int::inRange 100 900"

    @pytest.mark.parametrize(
        "component_names",
        [
            ["duplicated_component", ["Patient", "Biospecimen", "Patient"]],
            ["individual_component", ["Patient", "Biospecimen"]],
            ["no_component", []],
        ],
        ids=["duplicated_component", "individual_component", "no_component"],
    )
    def test_check_for_duplicate_components(self, component_names):
        """Test that we are properly identifying duplicates in a list.
        Exception should only be triggered when the duplicate component list is passed.
        """
        try:
            check_for_duplicate_components(
                component_names=component_names[1], validation_rule_string="dummy_str"
            )
        except:
            assert component_names[0] == "duplicated_component"

    @pytest.mark.parametrize(
        "test_rule_name",
        list(TEST_VALIDATION_RULES.keys()),
        ids=list(TEST_VALIDATION_RULES.keys()),
    )
    def test_parse_validation_rules(self, test_rule_name):
        """
        The test dictionary tests the following:
            A dictionary rule is simply returned.
            A string rule, raises an exception.
            A single rule, a double rule, component rules, with a single component in either orientation,
            double rules, multiple rules, creating a rule for all components except one.
        """
        validation_rules = TEST_VALIDATION_RULES[test_rule_name]["validation_rules"]
        expected_parsed_rules = TEST_VALIDATION_RULES[test_rule_name]["parsed_rules"]

        try:
            parsed_validation_rules = parse_validation_rules(
                validation_rules=validation_rules
            )
            assert expected_parsed_rules == parsed_validation_rules
        except:
            assert test_rule_name in ["str_rule", "duplicated_component"]

    @pytest.mark.parametrize(
        "test_rule_name",
        list(TEST_VALIDATION_RULES.keys()),
        ids=list(TEST_VALIDATION_RULES.keys()),
    )
    def test_extract_component_validation_rules(self, test_rule_name):
        """
        Test that a component validation rule dictionary is parsed properly
        """
        attribute_rules_set = TEST_VALIDATION_RULES[test_rule_name]["parsed_rules"]
        if isinstance(attribute_rules_set, dict):
            for component in attribute_rules_set.keys():
                extracted_rules = extract_component_validation_rules(
                    component, attribute_rules_set
                )
                assert isinstance(extracted_rules, list)
                assert (
                    extracted_rules
                    == TEST_VALIDATION_RULES[test_rule_name]["extracted_rules"][
                        component
                    ]
                )

    @pytest.mark.parametrize(
        "test_dn",
        list(TEST_DN_DICT.keys()),
        ids=list(TEST_DN_DICT.keys()),
    )
    def test_check_if_display_name_is_valid_label(self, test_dn):
        display_name = test_dn
        blacklisted_chars = ["(", ")", ".", " ", "-"]
        for entry_type, expected_result in TEST_DN_DICT[test_dn].items():
            valid_label = check_if_display_name_is_valid_label(
                test_dn, blacklisted_chars
            )
            if test_dn in ["Bio-things", "bio things", "Bio Things"]:
                assert valid_label == False
            else:
                assert valid_label == True

    @pytest.mark.parametrize(
        "test_dn",
        list(TEST_DN_DICT.keys())[-2:],
        ids=list(TEST_DN_DICT.keys())[-2:],
    )
    def test_get_stripped_label(self, test_dn: str):
        display_name = test_dn
        blacklisted_chars = ["(", ")", ".", " ", "-"]
        for entry_type, expected_result in TEST_DN_DICT[test_dn].items():
            label = ""

            label = get_stripped_label(
                entry_type=entry_type,
                display_name=display_name,
                blacklisted_chars=blacklisted_chars,
            )
            assert label == expected_result

    @pytest.mark.parametrize(
        "test_dn",
        list(TEST_DN_DICT.keys()),
        ids=list(TEST_DN_DICT.keys()),
    )
    def test_get_schema_label(self, test_dn: str):
        display_name = test_dn
        for entry_type, expected_result in TEST_DN_DICT[test_dn].items():
            label = ""

            label = get_schema_label(
                entry_type=entry_type,
                display_name=display_name,
                strict_camel_case=False,
            )

            if "-" in display_name:
                # In this case, biothings will not strip the blacklisted character,
                # so it will not match the dictionary.
                if entry_type == "class":
                    assert label == display_name.capitalize()
                else:
                    assert label == display_name[0].lower() + display_name[1:]
            else:
                assert label == expected_result

    @pytest.mark.parametrize(
        "test_dn",
        list(TEST_DN_DICT.keys()),
        ids=list(TEST_DN_DICT.keys()),
    )
    @pytest.mark.parametrize(
        "data_model_labels",
        ["display_label", "class_label"],
        ids=["display_label", "class_label"],
    )
    def test_get_label_from_display_name(self, test_dn: str, data_model_labels: str):
        display_name = test_dn
        for entry_type, expected_result in TEST_DN_DICT[test_dn].items():
            label = ""

            try:
                label = get_label_from_display_name(
                    entry_type=entry_type,
                    display_name=display_name,
                    data_model_labels=data_model_labels,
                )
            except:
                # Under these conditions should only fail if the display name cannot be used as a label.
                assert test_dn in [
                    "Bio Things",
                    "bio things",
                    "Bio-things",
                    "bio_things",
                ]
            if label:
                if data_model_labels == "display_label":
                    if test_dn in ["Bio Things", "bio things", "Bio-things"]:
                        assert label == expected_result

                    else:
                        assert label == test_dn
                else:
                    # The dash has an odd handling
                    if display_name == "Bio-things":
                        if entry_type == "property":
                            assert label == "bio-things"
                        else:
                            assert label == "Bio-things"
                    else:
                        assert label == expected_result

            else:
                return
        return

    @pytest.mark.parametrize(
        "data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values())
    )
    @pytest.mark.parametrize(
        "source_node",
        ["Biospecimen", "Patient"],
        ids=["biospecimen_source", "patient_source"],
    )
    def test_get_json_schema_log_file_path(
        self, helpers, data_model: str, source_node: str
    ):
        data_model_path = helpers.get_data_path(path=data_model)
        json_schema_log_file_path = get_json_schema_log_file_path(
            data_model_path=data_model_path, source_node=source_node
        )

        # Check that model is not included in the json_schema_log_file_path
        assert ".model" not in "data_model"

        # Check the file suffixs are what is expected.
        assert ["schema", "json"] == json_schema_log_file_path.split(".")[-2:]


class TestValidateUtils:
    def test_validate_schema(self, helpers):
        """ """

        # Get data model path
        data_model_path = helpers.get_data_path("example.model.jsonld")
        schema = io_utils.load_json(data_model_path)
        # need to pass the jsonschema
        actual = validate_utils.validate_schema(schema)

        assert actual is None

    def test_validate_class_schema(self, helpers):
        """
        Get a class template, fill it out with mock data, and validate against a JSON Schema

        """
        class_template = ClassTemplate()
        self.class_template = json.loads(class_template.to_json())

        mock_class = copy.deepcopy(self.class_template)
        mock_class["@id"] = "bts:MockClass"
        mock_class["@type"] = "rdfs:Class"
        mock_class["@rdfs:comment"] = "This is a mock class"
        mock_class["@rdfs:label"] = "MockClass"
        mock_class["rdfs:subClassOf"].append({"@id": "bts:Patient"})

        error = validate_utils.validate_class_schema(mock_class)

        assert error is None

    def test_validate_property_schema(self, helpers):
        """
        Get a property template, fill it out with mock data, and validate against a JSON Schema

        """
        property_template = PropertyTemplate()
        self.property_template = json.loads(property_template.to_json())

        mock_class = copy.deepcopy(self.property_template)
        mock_class["@id"] = "bts:MockProperty"
        mock_class["@type"] = "rdf:Property"
        mock_class["@rdfs:comment"] = "This is a mock Patient class"
        mock_class["@rdfs:label"] = "MockProperty"
        mock_class["schema:domainIncludes"].append({"@id": "bts:Patient"})

        error = validate_utils.validate_property_schema(mock_class)

        assert error is None

    @pytest.mark.single_process_execution
    @pytest.mark.parametrize(
        ("manifest", "model", "root_node"),
        [
            (
                "mock_manifests/Patient_test_no_entry_for_cond_required_column.manifest.csv",
                "example.model.csv",
                "Patient",
            ),
            (
                "mock_manifests/Valid_Test_Manifest_with_nones.csv",
                "example_test_nones.model.csv",
                "MockComponent",
            ),
        ],
    )
    def test_convert_nan_entries_to_empty_strings(
        self, helpers, manifest, model, root_node
    ):
        # Get manifest and data model path
        manifest_path = helpers.get_data_path(manifest)
        model_path = helpers.get_data_path(model)

        ## Gather parmeters needed to run validate_manifest_rules
        errors = []
        load_args = {
            "dtype": "string",
        }

        dmge = helpers.get_data_model_graph_explorer(path=model)

        self.data_model_js = DataModelJSONSchema(
            jsonld_path=model_path, graph=dmge.graph
        )
        json_schema = self.data_model_js.get_json_validation_schema(
            root_node, root_node + "_validation"
        )

        manifest = load_df(
            manifest_path,
            preserve_raw_input=False,
            allow_na_values=True,
            **load_args,
        )

        metadataModel = get_metadataModel(helpers, model)

        # Instantiate Validate manifest, and run manifest validation
        # In this step the manifest is modified while running rule
        # validation so need to do this step to get the updated manfest.
        vm = ValidateManifest(errors, manifest, manifest_path, dmge, json_schema)
        manifest, vmr_errors, vmr_warnings = vm.validate_manifest_rules(
            manifest,
            dmge,
            restrict_rules=False,
            project_scope=["syn54126707"],
        )

        # Run convert nan function
        output = validate_utils.convert_nan_entries_to_empty_strings(manifest=manifest)

        # Compare post rule validation manifest with output manifest looking
        # for expected nan to empty string conversion
        if root_node == "Patient":
            assert manifest["Family History"][0] == ["<NA>"]
            assert output["Family History"][0] == [""]
        elif root_node == "MockComponent":
            assert manifest["Check List"][2] == ["<NA>"]
            assert manifest["Check List Like Enum"][2] == []
            assert type(manifest["Check NA"][2]) == type(pd.NA)

            assert output["Check List"][2] == [""]
            assert output["Check List Like Enum"][2] == []

    def test_get_list_robustness(self, helpers):
        return

    def parse_str_series_to_list(self, helpers):
        return

    @pytest.mark.parametrize(
        "rule",
        [
            "required warning",
            "strict warning set required",
            "required strict warning set",
            "unique warning required",
            "unique required warning",
            "list strict",
            "required",
        ],
        ids=[
            "required_with_modifier",
            "required_last_with_multiple_modifiers",
            "required_first_with_multiple_modifiers",
            "rule_with_modifier_and_required_last",
            "rule_with_modifier_and_required_middle",
            "no_required",
            "only_required",
        ],
    )
    def test_required_is_only_rule(self, rule: str) -> None:
        """Verify that function required_is_only_rule is working as expected.
        Args:
            rule: str, various strings that we expect to behave a certain way during parsing.
        """

        output = validate_utils.required_is_only_rule(
            rule=rule,
            attribute="Patient ID",
            rule_modifiers=RULE_MODIFIERS,
            validation_expectation=VALIDATION_EXPECTATION,
        )

        if rule in [
            "required warning",
            "strict warning set required",
            "required strict warning set",
            "required",
        ]:
            assert output == True
        else:
            assert output == False


class TestCsvUtils:
    def test_csv_to_schemaorg(self, helpers, tmp_path):
        """Test the CSV-to-JSON-LD conversion.

        This test also ensures that the CSV and JSON-LD
        files for the example data model stay in sync.
        TODO: This probably should be moved out of here and to test_schemas
        """
        csv_path = helpers.get_data_path("example.model.csv")

        # Instantiate DataModelParser
        data_model_parser = DataModelParser(path_to_data_model=csv_path)

        # Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate DataModelGraph
        data_model_grapher = DataModelGraph(parsed_data_model)

        # Generate graph
        graph_data_model = data_model_grapher.graph

        # Convert graph to JSONLD
        jsonld_data_model = convert_graph_to_jsonld(graph=graph_data_model)

        # saving updated schema.org schema
        actual_jsonld_path = tmp_path / "example.from_csv.model.jsonld"
        export_schema(jsonld_data_model, actual_jsonld_path)

        # Compare both JSON-LD files
        expected_jsonld_path = helpers.get_data_path("example.model.jsonld")
        expected_jsonld = open(expected_jsonld_path).read()
        actual_jsonld = open(actual_jsonld_path).read()

        assert expected_jsonld == actual_jsonld
