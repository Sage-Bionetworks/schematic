from multiprocessing.sharedctypes import Value
import os
import logging
import sys

import shutil
import pytest
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from time import perf_counter

from schematic.schemas.explorer import SchemaExplorer
from schematic.configuration.configuration import CONFIG
from schematic.utils.df_utils import load_df
from schematic.store.synapse import SynapseStorage

load_dotenv()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Silence some very verbose loggers
logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("googleapiclient").setLevel(logging.INFO)
logging.getLogger("google_auth_httplib2").setLevel(logging.INFO)


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")

@pytest.fixture(scope="session")
def dataset_id():
    yield "syn25614635"


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
        return load_df(fullpath, **kwargs)

    @staticmethod
    def get_schema_explorer(path=None, *paths):
        if path is None:
            return SchemaExplorer()

        fullpath = Helpers.get_data_path(path, *paths)

        se = SchemaExplorer()
        se.load_schema(fullpath)
        return se

    @staticmethod
    def get_python_version():
        version=sys.version
        base_version=".".join(version.split('.')[0:2])

        return base_version

    @staticmethod
    def get_python_project(self):

        version = self.get_python_version()

        python_projects = {
            "3.7":  "syn47217926",
            "3.8":  "syn47217967",
            "3.9":  "syn47218127",
            "3.10": "syn47218347",
        }

        return python_projects[version]

@pytest.fixture(scope="session")
def helpers():
    yield Helpers

@pytest.fixture(scope="session")
def config():
    yield CONFIG

@pytest.fixture(scope="session")
def synapse_store(request):
    # Add timer to measure setup time
    t_s = perf_counter()

    # Add a counter for how many times the fixture is used
    if not hasattr(request.session, "_fixture_count"):
        request.session._fixture_count = {}

    fixture_name = request.fixturename
    request.session._fixture_count[fixture_name] = (
        request.session._fixture_count.get(fixture_name, 0) + 1
    )

    
    access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")
    if access_token:
        synapse_store = SynapseStorage(access_token=access_token)
    else:
        synapse_store = SynapseStorage()

    # Measure elapsed time
    t_e = perf_counter() - t_s

    # Print marking end of fixture setup and elapsed setup time
    print(f"\nstore setup\nElapsed Time: {t_e=}\n")
    yield synapse_store
    
    # Print marking fixture teardown
    print("\nstore teardown\n")