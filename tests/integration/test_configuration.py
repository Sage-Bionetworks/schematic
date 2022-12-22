import logging
import pytest

from schematic.configuration import Configuration

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestConfiguration:
    def test_load_yaml_valid(self, tmpdir):
        mock_contents = """
        section:
            key: value
        """
        mock_file = tmpdir.join("mock.yml")
        mock_file.write(mock_contents)
        mock_object = {"section": {"key": "value"}}

        test_object = Configuration.load_yaml(str(mock_file))
        assert test_object == mock_object

    def test_load_yaml_invalid(self, tmpdir):
        mock_contents = """
        section:
            key: bad-value:
        """
        mock_file = tmpdir.join("mock.yml")
        mock_file.write(mock_contents)

        test_object = Configuration.load_yaml(str(mock_file))
        assert test_object is None
