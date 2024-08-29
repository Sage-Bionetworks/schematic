"""Fixtures and helpers for use across all tests"""
import logging
import os
import shutil
import sys
from typing import Callable, Generator, Set

import pytest
from dotenv import load_dotenv
from pytest_asyncio import is_async_test

from schematic.configuration.configuration import CONFIG
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_parser import DataModelParser
from schematic.store.synapse import SynapseStorage
from schematic.utils.df_utils import load_df
from tests.utils import CleanupAction, CleanupItem

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
def synapse_store():
    yield SynapseStorage()


# These fixtures make copies of existing test manifests.
# These copies can the be altered by a given test, and the copy will eb destroyed at the
# end of the test


@pytest.fixture(scope="function")
def temporary_file_copy(request, helpers: Helpers) -> Generator[str, None, None]:
    file_name = request.param
    # original file copy
    original_test_path = helpers.get_data_path(f"mock_manifests/{file_name}")
    # get filename without extension
    file_name_no_extension = file_name.split(".")[0]
    # Copy the original CSV file to a temporary directory
    temp_csv_path = helpers.get_data_path(
        f"mock_manifests/{file_name_no_extension}_copy.csv"
    )

    shutil.copyfile(original_test_path, temp_csv_path)
    yield temp_csv_path
    # Teardown
    if os.path.exists(temp_csv_path):
        os.remove(temp_csv_path)


@pytest.fixture(name="dmge", scope="function")
def DMGE(helpers: Helpers) -> DataModelGraphExplorer:
    """Fixture to instantiate a DataModelGraphExplorer object."""
    dmge = helpers.get_data_model_graph_explorer(path="example.model.jsonld")
    return dmge


@pytest.fixture(scope="function")
def schedule_for_cleanup(
    request, synapse_store: SynapseStorage
) -> Callable[[CleanupItem], None]:
    """Returns a closure that takes an item that should be scheduled for cleanup."""

    items: Set[CleanupItem] = set()

    def _append_cleanup(item: CleanupItem):
        print(f"Added {item} to cleanup list")
        items.add(item)

    def cleanup_scheduled_items() -> None:
        for item in items:
            print(f"Cleaning up {item}")
            try:
                if item.action == CleanupAction.DELETE:
                    if item.synapse_id:
                        synapse_store.syn.delete(obj=item.synapse_id)
                    elif item.name and item.parent_id:
                        synapse_id = synapse_store.syn.findEntityId(
                            name=item.name, parent=item.parent_id
                        )
                        if synapse_id:
                            synapse_store.syn.delete(obj=synapse_id)
                    else:
                        logger.error(f"Invalid cleanup item {item}")
                else:
                    logger.error(f"Invalid cleanup action {item.action}")
            except Exception as ex:
                logger.exception(f"Failed to delete {item}")

    request.addfinalizer(cleanup_scheduled_items)

    return _append_cleanup


def pytest_collection_modifyitems(items) -> None:
    """Taken from docs at:
    https://pytest-asyncio.readthedocs.io/en/latest/how-to-guides/run_session_tests_in_same_loop.html

    Used to run all tests within the same event loop. This will allow any underlying
    logic use the same event loop for all tests. It is important for the underlying
    usage of the synapse python client which uses HTTPX for their async library. HTTPX
    connection pools cannot be shared across event loops. This will allow us to use the
    same connection pool for all tests.
    """
    pytest_asyncio_tests = (item for item in items if is_async_test(item))
    session_scope_marker = pytest.mark.asyncio(scope="session")
    for async_test in pytest_asyncio_tests:
        async_test.add_marker(session_scope_marker, append=False)
