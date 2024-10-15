"""Fixtures and helpers for use across all tests"""
import configparser
import logging
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from typing import Callable, Generator, Set

import flask
import pytest
from dotenv import load_dotenv
from flask.testing import FlaskClient
from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from synapseclient.client import Synapse

from schematic.configuration.configuration import CONFIG, Configuration
from schematic.models.metadata import MetadataModel
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_parser import DataModelParser
from schematic.store.synapse import SynapseStorage
from schematic.utils.df_utils import load_df
from schematic.utils.general import create_temp_folder
from schematic_api.api import create_app
from tests.utils import CleanupAction, CleanupItem

tracer = trace.get_tracer("Schematic-Tests")

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


@pytest.fixture(scope="class")
def flask_app() -> flask.Flask:
    """Create a Flask app for testing."""
    app = create_app()
    return app


@pytest.fixture(scope="class")
def flask_client(flask_app: flask.Flask) -> Generator[FlaskClient, None, None]:
    flask_app.config["SCHEMATIC_CONFIG"] = None

    with flask_app.test_client() as client:
        yield client


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


@dataclass
class ConfigurationForTesting:
    """Variables that are specific to testing. Specifically these are used to control
    the flags used during manual verification of some integration test results.

    Attributes:
        manual_test_verification_enabled (bool): Whether manual verification is enabled.
        manual_test_verification_path (str): The path to the directory where manual test
            verification files are stored.
        use_deployed_schematic_api_server (bool): Used to determine if a local flask
            instance is created during integration testing. If this is true schematic
            tests will use a schematic API server running outside of the context of the
            integration test.
        schematic_api_server_url (str): The URL of the schematic API server. Defaults to
            http://localhost:3001.
        local_flask_instance (bool): TEMPORARY: Whether the Flask instance is running
            locally. This is temporary per dicsussion here: https://github.com/Sage-Bionetworks/schematic/pull/1512#discussion_r1799924847

    """

    manual_test_verification_enabled: bool
    manual_test_verification_path: str
    use_deployed_schematic_api_server: bool
    schematic_api_server_url: str
    local_flask_instance: bool


@pytest.fixture(scope="session")
def testing_config(config: Configuration) -> ConfigurationForTesting:
    """Configuration variables that are specific to testing."""
    manual_test_verification_enabled = (
        os.environ.get("MANUAL_TEST_VERIFICATION", "false").lower() == "true"
    )
    use_deployed_schematic_api_server = (
        os.environ.get("USE_DEPLOYED_SCHEMATIC_API_SERVER", "false").lower() == "true"
    )
    schematic_api_server_url = os.environ.get(
        "SCHEMATIC_API_SERVER_URL", "http://localhost:3001"
    )
    local_flask_instance = not use_deployed_schematic_api_server or (
        use_deployed_schematic_api_server and "localhost" in schematic_api_server_url
    )

    if manual_test_verification_enabled:
        manual_test_verification_path = os.path.join(
            config.manifest_folder, "manual_test_verification"
        )
        os.makedirs(manual_test_verification_path, exist_ok=True)
    else:
        manual_test_verification_path = ""

    return ConfigurationForTesting(
        manual_test_verification_enabled=manual_test_verification_enabled,
        manual_test_verification_path=manual_test_verification_path,
        use_deployed_schematic_api_server=use_deployed_schematic_api_server,
        schematic_api_server_url=schematic_api_server_url,
        local_flask_instance=local_flask_instance,
    )


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


@pytest.fixture(scope="class")
def syn_token(config: Configuration):
    synapse_config_path = config.synapse_configuration_path
    config_parser = configparser.ConfigParser()
    config_parser.read(synapse_config_path)
    # try using synapse access token
    if "SYNAPSE_ACCESS_TOKEN" in os.environ:
        token = os.environ["SYNAPSE_ACCESS_TOKEN"]
    else:
        token = config_parser["authentication"]["authtoken"]
    return token


@pytest.fixture(scope="class")
def syn(syn_token) -> Synapse:
    syn = Synapse()
    syn.login(authToken=syn_token, silent=True)
    return syn


@pytest.fixture(scope="session")
def download_location() -> Generator[str, None, None]:
    download_location = create_temp_folder(path=tempfile.gettempdir())
    yield download_location

    # Cleanup after tests have used the temp folder
    if os.path.exists(download_location):
        shutil.rmtree(download_location)


def metadata_model(helpers, data_model_labels):
    metadata_model = MetadataModel(
        inputMModelLocation=helpers.get_data_path("example.model.jsonld"),
        data_model_labels=data_model_labels,
        inputMModelLocationType="local",
    )

    return metadata_model


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


active_span_processors = []


@pytest.fixture(scope="session", autouse=True)
def set_up_tracing() -> None:
    """Set up tracing for the API."""
    tracing_export = os.environ.get("TRACING_EXPORT_FORMAT", None)
    tracing_service_name = os.environ.get("TRACING_SERVICE_NAME", "schematic-tests")
    if tracing_export == "otlp":
        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource(attributes={SERVICE_NAME: tracing_service_name})
            )
        )
        processor = BatchSpanProcessor(OTLPSpanExporter())
        active_span_processors.append(processor)
        trace.get_tracer_provider().add_span_processor(processor)
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))


@pytest.fixture(autouse=True, scope="function")
def wrap_with_otel(request):
    """Start a new OTEL Span for each test function."""
    with tracer.start_as_current_span(request.node.name):
        try:
            yield
        finally:
            for processor in active_span_processors:
                processor.force_flush()


@pytest.fixture(scope="session", autouse=True)
def set_up_logging() -> None:
    """Set up logging to export to OTLP."""
    logging_export = os.environ.get("LOGGING_EXPORT_FORMAT", None)
    logging_service_name = os.environ.get("LOGGING_SERVICE_NAME", "schematic-tests")
    logging_instance_name = os.environ.get("LOGGING_INSTANCE_NAME", "local")
    if logging_export == "otlp":
        resource = Resource.create(
            {
                "service.name": logging_service_name,
                "service.instance.id": logging_instance_name,
            }
        )

        logger_provider = LoggerProvider(resource=resource)
        set_logger_provider(logger_provider=logger_provider)

        # TODO: Add support for secure connections
        exporter = OTLPLogExporter(insecure=True)
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
        handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
        logging.getLogger().addHandler(handler)
