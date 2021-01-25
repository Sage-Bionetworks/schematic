import logging
import pytest
import json
import os

from schematic.utils import general
from schematic.utils import cli_utils
from schematic.utils import io_utils
from schematic.exceptions import MissingConfigValueError, MissingConfigAndArgumentValueError
from schematic import LOADER

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestGeneral:

    def test_find_duplicates(self):

        mock_list = ['foo', 'bar', 'foo']
        mock_dups = {'foo'}

        test_dups = general.find_duplicates(mock_list)
        assert test_dups == mock_dups

    def test_dict2list_with_dict(self):

        mock_dict = {'foo': 'bar'}
        mock_list = [{'foo': 'bar'}]

        test_list = general.dict2list(mock_dict)
        assert test_list == mock_list

    def test_dict2list_with_list(self):

        # mock_dict = {'foo': 'bar'}
        mock_list = [{'foo': 'bar'}]

        test_list = general.dict2list(mock_list)
        assert test_list == mock_list


class TestCliUtils:

    def test_query_dict(self):

        mock_dict = {"k1": {"k2": {"k3": "foobar"}}}
        mock_keys_valid = ["k1", "k2", "k3"]
        mock_keys_invalid = ["k1", "k2", "k4"]

        test_result_valid = cli_utils.query_dict(mock_dict, mock_keys_valid)
        test_result_invalid = cli_utils.query_dict(mock_dict, mock_keys_invalid)

        assert test_result_valid == "foobar"
        assert test_result_invalid is None


    def test_get_from_config(self):

        mock_dict = {"k1": {"k2": {"k3": "foobar"}}}
        mock_keys_valid = ["k1", "k2", "k3"]
        mock_keys_invalid = ["k1", "k2", "k4"]

        test_result_valid = cli_utils.get_from_config(mock_dict, mock_keys_valid)
        
        assert test_result_valid == "foobar"

        with pytest.raises(MissingConfigValueError):
            cli_utils.get_from_config(mock_dict, mock_keys_invalid)
                        

    def test_fill_in_from_config(self, mocker):

        jsonld = "/path/to/one"
        jsonld_none = None

        mock_config = {"model": {"path": "/path/to/two"}}
        mock_keys = ["model", "path"]
        mock_keys_invalid = ["model", "file"]
        
        mocker.patch("schematic.CONFIG.DATA", mock_config)

        result1 = cli_utils.fill_in_from_config(
            "jsonld", jsonld, mock_keys
        )
        result2 = cli_utils.fill_in_from_config(
            "jsonld", jsonld, mock_keys
        )
        result3 = cli_utils.fill_in_from_config(
            "jsonld_none", jsonld_none, mock_keys
        )

        assert result1 == "/path/to/one"
        assert result2 == "/path/to/one"
        assert result3 == "/path/to/two"

        with pytest.raises(MissingConfigAndArgumentValueError):
            cli_utils.fill_in_from_config(
                "jsonld_none", jsonld_none, mock_keys_invalid
            )


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

    def test_json_load(self, tmpdir, mocker):

        json_file = tmpdir.join("example.json")
        json_file.write_text(json.dumps([
            {'k1': 'v1'},
            {'k2': 'v2'}
        ]), encoding="utf-8")

        with open(json_file, encoding='utf-8') as f:
            expected = json.load(f)

        result = io_utils.load_json(str(json_file))

        assert result == expected

        mock_urlopen = mocker.patch("urllib.request.urlopen",
                                    return_value = FakeResponse(data=json.dumps([
                                        {'k1': 'v1'}, 
                                        {'k2': 'v2'}]
                                    ))
                                    )
        
        res = io_utils.load_json("http://example.com")
        assert res == [
            {'k1': 'v1'},
            {'k2': 'v2'}
        ]

        assert mock_urlopen.call_count == 1

    
    def test_export_json(self, tmpdir):

        json_str = json.dumps([
            {'k1': 'v1'},
            {'k2': 'v2'}
        ])

        export_file = tmpdir.join("export_json_expected.json")
        io_utils.export_json(json_str, export_file)

        with open(export_file, encoding='utf-8') as f:
            expected = json.load(f)

        assert expected == json_str


    def test_load_schema(self, tmpdir, mocker):

        jsonld_file = tmpdir.join("example_schema.jsonld")
        jsonld_str = {
            '@context': {'k': 'v'},
            '@graph': [
                {'k1': 'v1'},
                {'k2': 'v2'}
        ]}
        jsonld_file.write_text(json.dumps(jsonld_str), encoding="utf-8")

        mocker.patch("schematic.LOADER.filename",
                    return_value=os.path.normpath(jsonld_file))

        expected_default = io_utils.load_default()
        expected_schemaorg = io_utils.load_schemaorg()

        assert expected_default == jsonld_str
        assert expected_schemaorg == jsonld_str
