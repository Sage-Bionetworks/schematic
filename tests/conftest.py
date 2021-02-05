import os
import logging
import pytest
import pandas as pd

from schematic.configuration import CONFIG


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Silence some very verbose loggers
logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("googleapiclient").setLevel(logging.INFO)
logging.getLogger("google_auth_httplib2").setLevel(logging.INFO)


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "test_config.yml")
CONFIG.load_config(CONFIG_PATH)


@pytest.fixture()
def mock_creds():
    mock_creds = {
        'sheet_service': 'mock_sheet_service',
        'drive_service': 'mock_drive_service',
        'creds': 'mock_creds'
    }
    yield mock_creds


# This class serves as a container for helper functions that can be
# passed to individual tests using the `helpers` fixture. This approach
# was required because fixture functions cannot take arguments.
class Helpers:
    @staticmethod
    def get_data_file(path):
        return os.path.join(DATA_DIR, path)


@pytest.fixture
def helpers():
    return Helpers


@pytest.fixture
def config():
    return CONFIG


@pytest.fixture()
def synapse_manifest(helpers):
    get_data_file = helpers.get_data_file
    manifest_path = get_data_file("mock_manifests", "synapse_manifest.csv")
    manifest_df = pd.read_csv(manifest_path)
    return manifest_df


@pytest.fixture()
def local_manifest(helpers):
    get_data_file = helpers.get_data_file
    manifest_path = get_data_file("mock_manifests", "local_manifest.csv")
    manifest_df = pd.read_csv(manifest_path)
    return manifest_df
