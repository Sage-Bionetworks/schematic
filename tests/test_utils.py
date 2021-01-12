import logging
import pytest

from schematic.utils import general
from schematic.utils import cli_utils

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


    def test_fill_in_from_config(self, mocker):

        jsonld = "/path/to/one"
        jsonld_none = None

        mock_config = {"model": {"path": "/path/to/two"}}
        mock_keys = ["model", "path"]
        mock_keys_invalid = ["model", "file"]
        
        mocker.patch("schematic.CONFIG.DATA", mock_config)

        result1 = cli_utils.fill_in_from_config(
            "jsonld", mock_keys, jsonld
        )
        result2 = cli_utils.fill_in_from_config(
            "jsonld", mock_keys, jsonld
        )
        result3 = cli_utils.fill_in_from_config(
            "jsonld_none", mock_keys, jsonld_none
        )

        assert result1 == "/path/to/one"
        assert result2 == "/path/to/one"
        assert result3 == "/path/to/two"

        with pytest.raises(AssertionError):
            cli_utils.fill_in_from_config(
                "jsonld_none", mock_keys_invalid, jsonld_none
            )
