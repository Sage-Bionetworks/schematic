"""Fixtures and helpers for use across all tests"""
import os
import logging
import sys
from typing import Generator

import shutil
import pytest
from dotenv import load_dotenv

from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer

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
    def get_data_model_graph_explorer(
        path=None, data_model_labels: str = "class_label", *paths
    ):
        # commenting this now bc we dont want to have multiple instances
        if path is None:
            return

        fullpath = Helpers.get_data_path(path, *paths)

        # Instantiate DataModelParser
        data_model_parser = DataModelParser(
            path_to_data_model=fullpath,
        )

        # Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate DataModelGraph
        data_model_grapher = DataModelGraph(
            parsed_data_model, data_model_labels=data_model_labels
        )

        # Generate graph
        graph_data_model = data_model_grapher.graph

        # Instantiate DataModelGraphExplorer
        DMGE = DataModelGraphExplorer(graph_data_model)

        return DMGE

    @staticmethod
    def get_python_version():
        version = sys.version
        base_version = ".".join(version.split(".")[0:2])

        return base_version

    @staticmethod
    def get_python_project(self):
        version = self.get_python_version()

        python_projects = {
            "3.7": "syn47217926",
            "3.8": "syn47217967",
            "3.9": "syn47218127",
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
    access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")
    if access_token:
        synapse_store = SynapseStorage(access_token=access_token)
    else:
        synapse_store = SynapseStorage()

    yield synapse_store


# These fixtures make copies of existing test manifests.
# These copies can the be altered by a given test, and the copy will eb destroyed at the 
# end of the test

@pytest.fixture(scope="function")
def test_bulkrnaseq(helpers: Helpers) -> Generator[str, None, None]:
    """create temporary copy of test_BulkRNAseq.csv
    This fixture creates a temporary copy of the original 'test_BulkRNAseq.csv' file
    After test, the copied file is removed.
    Args:
        helpers (Helpers): Helpers fixture

    Yields:
        Generator[Path, None, None]: temporary file path of the copied version test_BulkRNAseq.csv
    """
    # original bulkrnaseq csv
    original_test_path = helpers.get_data_path("mock_manifests/test_BulkRNAseq.csv")
    # Copy the original CSV file to a temporary directory
    temp_csv_path = helpers.get_data_path("mock_manifests/test_BulkRNAseq2.csv")
    shutil.copyfile(original_test_path, temp_csv_path)
    yield temp_csv_path
    # Teardown
    if os.path.exists(temp_csv_path):
        os.remove(temp_csv_path)


@pytest.fixture(scope="function")
def test_annotations_manifest(helpers: Helpers) -> Generator[str, None, None]:
    """
    Create temporary copy of annotations_test_manifest.csv
    This fixture creates a temporary copy of the original 'test_BulkRNAseq.csv' file
    After test, the copied file is removed.
    Args:
        helpers (Helpers): Helpers fixture

    Yields:
        Generator[Path, None, None]: temporary file path of the copied manifest
    """
    original_test_path = helpers.get_data_path("mock_manifests/annotations_test_manifest.csv")
    temp_csv_path = helpers.get_data_path("mock_manifests/annotations_test_manifest2.csv")
    shutil.copyfile(original_test_path, temp_csv_path)
    yield temp_csv_path
    if os.path.exists(temp_csv_path):
        os.remove(temp_csv_path)
