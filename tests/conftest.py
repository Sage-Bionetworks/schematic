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


# This class serves as a container for helper functions that can be
# passed to individual tests using the `helpers` fixture. This approach
# was required because fixture functions cannot take arguments.
class Helpers:
    @staticmethod
    def get_data_path(path, *paths):
        return os.path.join(DATA_DIR, path, *paths)

    @staticmethod
    def get_data_file(path, *paths, **kwargs):
        fullpath = os.path.join(DATA_DIR, path, *paths)
        return open(fullpath, **kwargs)

    @staticmethod
    def get_data_frame(path, *paths, **kwargs):
        fullpath = os.path.join(DATA_DIR, path, *paths)
        return pd.read_csv(fullpath, **kwargs)


@pytest.fixture
def helpers():
    yield Helpers


@pytest.fixture
def config():
    yield CONFIG


@pytest.fixture()
def synapse_manifest(helpers):
    yield helpers.get_data_frame("mock_manifests", "synapse_manifest.csv")


@pytest.fixture()
def local_manifest(helpers):
    yield helpers.get_data_frame("mock_manifests", "local_manifest.csv")
