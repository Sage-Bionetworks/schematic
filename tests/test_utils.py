import logging
import pytest

from schematic.utils import general
from schematic.utils import config_utils

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


class TestConfigUtils:

    def test_load_yaml_valid(self, tmpdir):
        mock_contents = """
        section:
            key: value
        """
        mock_file = tmpdir.join('mock.yml')
        mock_file.write(mock_contents)
        mock_object = {'section': {'key': 'value'}}

        test_object = config_utils.load_yaml(str(mock_file))
        assert test_object == mock_object


    def test_load_yaml_invalid(self, tmpdir):
        mock_contents = """
        section:
            key: bad-value:
        """
        mock_file = tmpdir.join('mock.yml')
        mock_file.write(mock_contents)

        test_object = config_utils.load_yaml(str(mock_file))
        assert test_object is None