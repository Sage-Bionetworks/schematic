import logging
import os
import pathlib
import pickle
import shutil
import tempfile
import time
import urllib.request
from functools import wraps
from typing import List, Tuple

import connexion
import pandas as pd
from flask import current_app as app
from flask import request, send_from_directory
from flask_cors import cross_origin
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    Span,
)
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF

from schematic.configuration.configuration import CONFIG
from schematic.manifest.generator import ManifestGenerator
from schematic.models.metadata import MetadataModel
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_parser import DataModelParser
from schematic.store.synapse import ManifestDownload, SynapseStorage
from schematic.utils.general import entity_type_mapping
from schematic.utils.schema_utils import (
    DisplayLabelType,
    get_property_label_from_display_name,
)
from schematic.utils.io_utils import read_pickle
from schematic.visualization.attributes_explorer import AttributesExplorer
from schematic.visualization.tangled_tree import TangledTree

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

trace.set_tracer_provider(
    TracerProvider(resource=Resource(attributes={SERVICE_NAME: "schematic-api"}))
)


# borrowed from: https://github.com/Sage-Bionetworks/synapsePythonClient/blob/develop/tests/integration/conftest.py
class FileSpanExporter(ConsoleSpanExporter):
    """Create an exporter for OTEL data to a file."""

    def __init__(self, file_path: str) -> None:
        """Init with a path."""
        self.file_path = file_path

    def export(self, spans: List[Span]) -> None:
        """Export the spans to the file."""
        with open(self.file_path, "a", encoding="utf-8") as f:
            for span in spans:
                span_json_one_line = span.to_json().replace("\n", "") + "\n"
                f.write(span_json_one_line)


def set_up_tracing() -> None:
    """Set up tracing for the API."""
    tracing_export = os.environ.get("TRACING_EXPORT_FORMAT", None)
    if tracing_export == "otlp":
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter())
        )
    elif tracing_export == "file":
        timestamp_millis = int(time.time() * 1000)
        file_name = f"otel_spans_integration_testing_{timestamp_millis}.ndjson"
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_name)
        processor = SimpleSpanProcessor(FileSpanExporter(file_path))
        trace.get_tracer_provider().add_span_processor(processor)
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))


set_up_tracing()
tracer = trace.get_tracer("Schematic")


def trace_function_params():
    """capture all the parameters of API requests"""

    def decorator(func):
        """create a decorator"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            """create a wrapper function. Any number of positional arguments and keyword arguments can be passed here."""
            tracer = trace.get_tracer(__name__)
            # Start a new span with the function's name
            with tracer.start_as_current_span(func.__name__) as span:
                # Set values of parameters as tags
                for i, arg in enumerate(args):
                    span.set_attribute(f"arg{i}", arg)

                for name, value in kwargs.items():
                    span.set_attribute(name, value)
                # Call the actual function
                result = func(*args, **kwargs)
                return result

        return wrapper

    return decorator


def config_handler(asset_view: str = None):
    # check if path to config is provided
    path_to_config = app.config["SCHEMATIC_CONFIG"]
    if path_to_config is not None and os.path.isfile(path_to_config):
        CONFIG.load_config(path_to_config)
    if asset_view is not None:
        CONFIG.synapse_master_fileview_id = asset_view


class JsonConverter:
    """
    Mainly handle converting json str or json file to csv
    """

    def readJson(self, json_str=None, manifest_file=None):
        """
        The purpose of this function is to read either json str or json file
        input:
            json_str: json object
            manifest_file: manifest file object
        output:
            return a dataframe
        """
        if json_str:
            df = pd.read_json(json_str)
        elif manifest_file:
            df = pd.read_json(manifest_file.read())
        return df

    def get_file(self, file_key):
        """
        The purpose of this function is to get the file uploaded by user
        input:
            file_key: Defined in api.yaml. This key refers to the files uploaded.
            manifest_file: manifest file object
        output:
            return file object
        """

        manifest_file = connexion.request.files[file_key]
        return manifest_file

    def IsJsonFile(self, manifest_file):
        """
        The purpose of this function is check if the manifest file that gets uploaded is a json or not
        input:
            manifest_file: manifest file object
        output:
            return True if it is json
        """

        file_type = manifest_file.content_type
        if file_type == "application/json":
            return True
        else:
            return False

    def convert_df_to_csv(self, df, file_name):
        """
        The purpose of this function is to convert dataframe to a temporary CSV file
        input:
            df: dataframe
            file_name: file name of the output csv
        output:
            return temporary file path of the output csv
        """

        # convert dataframe to a temporary csv file
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, file_name)
        df.to_csv(temp_path, encoding="utf-8", index=False)
        return temp_path

    def convert_json_str_to_csv(self, json_str, file_name):
        """
        The purpose of this function is to convert json str to a temporary csv file
        input:
            json_str: json object
            file_name: file name of the output csv
        output:
            return temporary file path of the output csv
        """

        # convert json to df
        df = self.readJson(json_str=json_str)

        # convert dataframe to a temporary csv file
        temp_path = self.convert_df_to_csv(df, file_name)

        return temp_path

    def convert_json_file_to_csv(self, file_key):
        """
        The purpose of this function is to convert json str to a temporary csv file
        input:
            file_key: Defined in api.yaml. This key refers to the files uploaded.
        output:
            return temporary file path of the output csv
        """

        # get manifest file
        manifest_file = self.get_file(file_key)

        if self.IsJsonFile(manifest_file):
            # read json as dataframe
            df = self.readJson(manifest_file=manifest_file)
            # get base file name
            base = os.path.splitext(manifest_file.filename)[0]
            # name the new csv file
            new_file_name = base + ".csv"
            # convert to csv
            temp_path = self.convert_df_to_csv(df, new_file_name)
            return temp_path
        else:
            temp_path = save_file(file_key="file_name")
            return temp_path


def get_access_token() -> str:
    """Get access token from header"""
    bearer_token = None
    # Check if the Authorization header is present
    if "Authorization" in request.headers:
        auth_header = request.headers["Authorization"]

        # Ensure the header starts with 'Bearer ' and retrieve the token
        if auth_header.startswith("Bearer "):
            bearer_token = auth_header.split(" ")[1]
    return bearer_token


def parse_bool(str_bool):
    if str_bool.lower().startswith("t"):
        return True
    elif str_bool.lower().startswith("f"):
        return False
    else:
        raise ValueError(
            "String boolean does not appear to be true or false. Please verify input."
        )


def return_as_json(manifest_local_file_path):
    manifest_csv = pd.read_csv(manifest_local_file_path)
    manifest_json = manifest_csv.to_dict(orient="records")
    return manifest_json


def save_file(file_key="csv_file"):
    """
    input:
        file_key: Defined in api.yaml. This key refers to the files uploaded. By default, set to "csv_file"
    Return a temporary file path for the uploaded a given file
    """
    manifest_file = connexion.request.files[file_key]

    # save contents of incoming manifest CSV file to temp file
    temp_dir = tempfile.gettempdir()
    # path to temp file where manifest file contents will be saved
    temp_path = os.path.join(temp_dir, manifest_file.filename)
    # save content
    manifest_file.save(temp_path)

    return temp_path


def initalize_metadata_model(schema_url, data_model_labels):
    # get path to temp data model file (csv or jsonld) as appropriate
    data_model = get_temp_model_path(schema_url)

    metadata_model = MetadataModel(
        inputMModelLocation=data_model,
        inputMModelLocationType="local",
        data_model_labels=data_model_labels,
    )
    return metadata_model


def get_temp_jsonld(schema_url):
    # retrieve a JSON-LD via URL and store it in a temporary location
    with urllib.request.urlopen(schema_url) as response:
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".model.jsonld"
        ) as tmp_file:
            shutil.copyfileobj(response, tmp_file)

    # get path to temporary JSON-LD file
    return tmp_file.name


def get_temp_csv(schema_url):
    # retrieve a CSV via URL and store it in a temporary location
    with urllib.request.urlopen(schema_url) as response:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".model.csv") as tmp_file:
            shutil.copyfileobj(response, tmp_file)

    # get path to temporary csv file
    return tmp_file.name


def get_temp_pickle(graph_url: str) -> str:
    # retrieve a pickle via URL and store it in a temporary location
    with urllib.request.urlopen(graph_url) as response:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pickle") as tmp_file:
            shutil.copyfileobj(response, tmp_file)

    return tmp_file.name


def get_temp_model_path(schema_url):
    # Get model type:
    model_extension = pathlib.Path(schema_url).suffix.replace(".", "").upper()
    if model_extension == "CSV":
        temp_path = get_temp_csv(schema_url)
    elif model_extension == "JSONLD":
        temp_path = get_temp_jsonld(schema_url)
    elif model_extension == "PICKLE":
        temp_path = get_temp_pickle(schema_url)
    else:
        raise ValueError(
            "Did not provide a valid model type CSV or JSONLD or PICKLE, please check submission and try again."
        )
    return temp_path


# @before_request
@trace_function_params()
def get_manifest_route(
    schema_url: str,
    use_annotations: bool,
    dataset_id=None,
    asset_view=None,
    output_format=None,
    title=None,
    strict_validation: bool = True,
    data_model_labels: DisplayLabelType = "class_label",
    data_type: str = None,
    graph_url: str = None,
):
    """Get the immediate dependencies that are related to a given source node.
    Args:
        schema_url: link to data model in json ld or csv format
        title: title of a given manifest.
        dataset_id: Synapse ID of the "dataset" entity on Synapse (for a given center/project).
        data_type: data model components.
        output_format: contains three option: "excel", "google_sheet", and "dataframe". if set to "excel", return an excel spreadsheet
        use_annotations: Whether to use existing annotations during manifest generation
        asset_view: ID of view listing all project data assets. For example, for Synapse this would be the Synapse ID of the fileview listing all data assets for a given project.
        strict: bool, strictness with which to apply validation rules to google sheets.
        graph_url: str, URL to a pickled graph object.
    Returns:
        Googlesheet URL (if sheet_url is True), or pandas dataframe (if sheet_url is False).
    """
    # Get access token from request header
    access_token = get_access_token()

    config_handler(asset_view=asset_view)

    graph_data_model = None
    if graph_url is not None:
        graph_path = get_temp_model_path(graph_url)
        graph_data_model = read_pickle(graph_path)

    all_results = ManifestGenerator.create_manifests(
        path_to_data_model=schema_url,
        output_format=output_format,
        data_types=data_type,
        title=title,
        access_token=access_token,
        dataset_ids=dataset_id,
        strict=strict_validation,
        use_annotations=use_annotations,
        data_model_labels=data_model_labels,
        graph_data_model=graph_data_model,
    )

    # return an excel file if output_format is set to "excel"
    if output_format == "excel":
        # should only contain one excel spreadsheet path
        if len(all_results) > 0:
            result = all_results[0]
            dir_name = os.path.dirname(result)
            file_name = os.path.basename(result)
            mimetype = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            return send_from_directory(
                directory=dir_name,
                path=file_name,
                as_attachment=True,
                mimetype=mimetype,
                max_age=0,
            )

    return all_results


@trace_function_params()
def validate_manifest_route(
    schema_url,
    data_type,
    data_model_labels,
    restrict_rules=None,
    json_str=None,
    asset_view=None,
    project_scope=None,
):
    # Access token now stored in request header
    access_token = get_access_token()

    # if restrict rules is set to None, default it to False
    if not restrict_rules:
        restrict_rules = False

    # call config_handler()
    config_handler(asset_view=asset_view)

    # If restrict_rules parameter is set to None, then default it to False
    if not restrict_rules:
        restrict_rules = False

    # Get path to temp file where manifest file contents will be saved
    jsc = JsonConverter()

    if json_str:
        temp_path = jsc.convert_json_str_to_csv(
            json_str=json_str, file_name="example_json"
        )
    else:
        temp_path = jsc.convert_json_file_to_csv("file_name")

    # get path to temp data model file (csv or jsonld) as appropriate
    data_model = get_temp_model_path(schema_url)

    metadata_model = MetadataModel(
        inputMModelLocation=data_model,
        inputMModelLocationType="local",
        data_model_labels=data_model_labels,
    )

    errors, warnings = metadata_model.validateModelManifest(
        manifestPath=temp_path,
        rootNode=data_type,
        restrict_rules=restrict_rules,
        project_scope=project_scope,
        access_token=access_token,
    )

    res_dict = {"errors": errors, "warnings": warnings}

    return res_dict


# profile validate manifest route function
@trace_function_params()
def submit_manifest_route(
    schema_url,
    data_model_labels: str,
    asset_view=None,
    manifest_record_type=None,
    json_str=None,
    table_manipulation=None,
    data_type=None,
    hide_blanks=False,
    project_scope=None,
    table_column_names=None,
    annotation_keys=None,
    file_annotations_upload: bool = True,
):
    # call config_handler()
    config_handler(asset_view=asset_view)

    # convert Json file to CSV if applicable
    jsc = JsonConverter()
    if json_str:
        temp_path = jsc.convert_json_str_to_csv(
            json_str=json_str, file_name="example_json.csv"
        )
    else:
        temp_path = jsc.convert_json_file_to_csv("file_name")

    # Get/parse parameters from the API

    dataset_id = connexion.request.args["dataset_id"]

    restrict_rules = parse_bool(connexion.request.args["restrict_rules"])

    if not table_manipulation:
        table_manipulation = "replace"

    if not manifest_record_type:
        manifest_record_type = "table_file_and_entities"

    if data_type == "None":
        validate_component = None
    else:
        validate_component = data_type

    # get path to temp data model file (csv or jsonld) as appropriate
    data_model = get_temp_model_path(schema_url)

    if not table_column_names:
        table_column_names = "class_label"

    if not annotation_keys:
        annotation_keys = "class_label"

    metadata_model = initalize_metadata_model(schema_url, data_model_labels)

    # Access token now stored in request header
    access_token = get_access_token()

    manifest_id = metadata_model.submit_metadata_manifest(
        manifest_path=temp_path,
        dataset_id=dataset_id,
        validate_component=validate_component,
        access_token=access_token,
        manifest_record_type=manifest_record_type,
        restrict_rules=restrict_rules,
        hide_blanks=hide_blanks,
        table_manipulation=table_manipulation,
        project_scope=project_scope,
        table_column_names=table_column_names,
        annotation_keys=annotation_keys,
        file_annotations_upload=file_annotations_upload,
    )

    return manifest_id


def populate_manifest_route(
    schema_url, data_model_labels: str, title=None, data_type=None, return_excel=None
):
    # call config_handler()
    config_handler()

    # Get path to temp file where manifest file contents will be saved
    temp_path = save_file()

    # get path to temp data model file (csv or jsonld) as appropriate
    data_model = get_temp_model_path(schema_url)

    # Initalize MetadataModel
    metadata_model = MetadataModel(
        inputMModelLocation=data_model,
        inputMModelLocationType="local",
        data_model_labels=data_model_labels,
    )

    # Call populateModelManifest class
    populated_manifest_link = metadata_model.populateModelManifest(
        title=title,
        manifestPath=temp_path,
        rootNode=data_type,
        return_excel=return_excel,
    )

    return populated_manifest_link


def get_storage_projects(asset_view):
    # Access token now stored in request header
    access_token = get_access_token()

    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse storage
    store = SynapseStorage(access_token=access_token)

    # call getStorageProjects function
    lst_storage_projects = store.getStorageProjects()

    return lst_storage_projects


def get_storage_projects_datasets(asset_view, project_id):
    # Access token now stored in request header
    access_token = get_access_token()

    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(access_token=access_token)

    # call getStorageDatasetsInProject function
    sorted_dataset_lst = store.getStorageDatasetsInProject(projectId=project_id)

    return sorted_dataset_lst


def get_files_storage_dataset(
    asset_view: str, dataset_id: str, full_path: bool, file_names: List[str] = None
) -> List[Tuple[str, str]]:
    # Access token now stored in request header
    access_token = get_access_token()

    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(access_token=access_token)

    # no file names were specified (file_names = [''])
    if file_names and not all(file_names):
        file_names = None

    # call getFilesInStorageDataset function
    file_lst = store.getFilesInStorageDataset(
        datasetId=dataset_id, fileNames=file_names, fullpath=full_path
    )
    return file_lst


def check_if_files_in_assetview(asset_view, entity_id):
    # Access token now stored in request header
    access_token = get_access_token()

    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(access_token=access_token)

    # call function and check if a file or a folder is in asset view
    if_exists = store.checkIfinAssetView(entity_id)

    return if_exists


def check_entity_type(entity_id):
    # Access token now stored in request header
    access_token = get_access_token()

    # call config handler
    config_handler()

    syn = SynapseStorage.login(access_token=access_token)
    entity_type = entity_type_mapping(syn, entity_id)

    return entity_type


def get_component_requirements(
    schema_url, source_component, as_graph, data_model_labels
):
    metadata_model = initalize_metadata_model(schema_url, data_model_labels)
    req_components = metadata_model.get_component_requirements(
        source_component=source_component, as_graph=as_graph
    )

    return req_components


@cross_origin(["http://localhost", "https://sage-bionetworks.github.io"])
def get_viz_attributes_explorer(schema_url, data_model_labels):
    # call config_handler()
    config_handler()

    # get path to temp data model file (csv or jsonld) as appropriate
    data_model = get_temp_model_path(schema_url)

    attributes_csv = AttributesExplorer(data_model, data_model_labels).parse_attributes(
        save_file=False
    )

    return attributes_csv


def get_viz_component_attributes_explorer(
    schema_url, component, include_index, data_model_labels
):
    # call config_handler()
    config_handler()

    # get path to temp data model file (csv or jsonld) as appropriate
    data_model = get_temp_model_path(schema_url)

    attributes_csv = AttributesExplorer(
        data_model, data_model_labels
    )._parse_component_attributes(
        component, save_file=False, include_index=include_index
    )

    return attributes_csv


@cross_origin(["http://localhost", "https://sage-bionetworks.github.io"])
def get_viz_tangled_tree_text(schema_url, figure_type, text_format, data_model_labels):
    # get path to temp data model file (csv or jsonld) as appropriate
    data_model = get_temp_model_path(schema_url)

    # Initialize TangledTree
    tangled_tree = TangledTree(data_model, figure_type, data_model_labels)

    # Get text for tangled tree.
    text_df = tangled_tree.get_text_for_tangled_tree(text_format, save_file=False)

    return text_df


@cross_origin(["http://localhost", "https://sage-bionetworks.github.io"])
def get_viz_tangled_tree_layers(schema_url, figure_type, data_model_labels):
    # call config_handler()
    config_handler()

    # get path to temp data model file (csv or jsonld) as appropriate
    data_model = get_temp_model_path(schema_url)

    # Initialize Tangled Tree
    tangled_tree = TangledTree(data_model, figure_type, data_model_labels)

    # Get tangled trees layers JSON.
    layers = tangled_tree.get_tangled_tree_layers(save_file=False)

    return layers[0]


def download_manifest(manifest_id, new_manifest_name="", as_json=True):
    """
    Download a manifest based on a given manifest id.
    Args:
        manifest_syn_id: syn id of a manifest
        newManifestName: new name of a manifest that gets downloaded.
        as_json: boolean; If true, return a manifest as a json. Default to True
    Return:
        file path of the downloaded manifest
    """
    # Access token now stored in request header
    access_token = get_access_token()

    # call config_handler()
    config_handler()

    # use login method in synapse storage
    syn = SynapseStorage.login(access_token=access_token)
    try:
        md = ManifestDownload(syn, manifest_id)
        manifest_data = ManifestDownload.download_manifest(md, new_manifest_name)
        # return local file path
        manifest_local_file_path = manifest_data["path"]
    except TypeError as e:
        raise TypeError(f"Failed to download manifest {manifest_id}.")
    if as_json:
        manifest_json = return_as_json(manifest_local_file_path)
        return manifest_json
    else:
        return manifest_local_file_path


def download_dataset_manifest(dataset_id, asset_view, as_json, new_manifest_name=""):
    # Access token now stored in request header
    access_token = get_access_token()

    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(access_token=access_token)

    # download existing file
    manifest_data = store.getDatasetManifest(
        datasetId=dataset_id, downloadFile=True, newManifestName=new_manifest_name
    )

    # return local file path
    try:
        manifest_local_file_path = manifest_data["path"]

    except KeyError as e:
        raise KeyError(f"Failed to download manifest from dataset: {dataset_id}") from e

    # return a json (if as_json = True)
    if as_json:
        manifest_json = return_as_json(manifest_local_file_path)
        return manifest_json

    return manifest_local_file_path


def get_asset_view_table(asset_view, return_type):
    # Access token now stored in request header
    access_token = get_access_token()

    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(access_token=access_token)

    # get file view table
    file_view_table_df = store.getStorageFileviewTable()

    # return different results based on parameter
    if return_type == "json":
        json_res = file_view_table_df.to_json()
        return json_res
    else:
        path = os.getcwd()
        export_path = os.path.join(path, "tests/data/file_view_table.csv")
        file_view_table_df.to_csv(export_path, index=False)
        return export_path


def get_project_manifests(project_id, asset_view):
    # Access token now stored in request header
    access_token = get_access_token()

    # use the default asset view from config
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(access_token=access_token, project_scope=[project_id])

    # call getprojectManifest function
    lst_manifest = store.getProjectManifests(projectId=project_id)

    return lst_manifest


def get_manifest_datatype(manifest_id, asset_view):
    # Access token now stored in request header
    access_token = get_access_token()

    # use the default asset view from config
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(access_token=access_token)

    # get data types of an existing manifest
    manifest_dtypes_dict = store.getDataTypeFromManifest(manifest_id)

    return manifest_dtypes_dict


def get_schema_pickle(schema_url, data_model_labels):
    data_model_parser = DataModelParser(path_to_data_model=schema_url)
    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    # write to local pickle file
    path = os.getcwd()
    export_path = os.path.join(path, "tests/data/schema.gpickle")

    with open(export_path, "wb") as file:
        pickle.dump(graph_data_model, file)
    return export_path


def get_subgraph_by_edge_type(schema_url, relationship, data_model_labels):
    data_model_parser = DataModelParser(path_to_data_model=schema_url)

    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    dmge = DataModelGraphExplorer(graph_data_model)

    # relationship subgraph
    relationship_subgraph = dmge.get_subgraph_by_edge_type(relationship)
    # return relationship
    Arr = []
    for t in relationship_subgraph.edges:
        lst = list(t)
        Arr.append(lst)

    return Arr


def find_class_specific_properties(schema_url, schema_class, data_model_labels):
    data_model_parser = DataModelParser(path_to_data_model=schema_url)
    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    dmge = DataModelGraphExplorer(graph_data_model)

    # return properties
    properties = dmge.find_class_specific_properties(schema_class)

    return properties


def get_node_dependencies(
    schema_url: str,
    source_node: str,
    data_model_labels: str,
    return_display_names: bool = True,
    return_schema_ordered: bool = True,
) -> list[str]:
    """Get the immediate dependencies that are related to a given source node.

    Args:
        schema_url (str): Data Model URL
        source_node (str): The node whose dependencies are needed.
        return_display_names (bool, optional):
            If True, return list of display names of each of the dependencies.
            If False, return list of node labels of each of the dependencies.
            Defaults to True.
        return_schema_ordered (bool, optional):
            If True, return the dependencies of the node following the order of the schema (slower).
            If False, return dependencies from graph without guaranteeing schema order (faster).
            Defaults to True.

    Returns:
        list[str]: List of nodes that are dependent on the source node.
    """
    data_model_parser = DataModelParser(path_to_data_model=schema_url)
    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    dmge = DataModelGraphExplorer(graph_data_model)

    dependencies = dmge.get_node_dependencies(
        source_node, return_display_names, return_schema_ordered
    )
    return dependencies


def get_property_label_from_display_name_route(
    display_name: str, strict_camel_case: bool = False
) -> str:
    """Converts a given display name string into a proper property label string

    Args:
        schema_url (str): Data Model URL
        display_name (str): The display name to be converted
        strict_camel_case (bool, optional): If true the more strict way of
            converting to camel case is used.

    Returns:
        str: The property label of the display name
    """
    label = get_property_label_from_display_name(
        display_name=display_name, strict_camel_case=strict_camel_case
    )
    return label


def get_node_range(
    schema_url: str,
    node_label: str,
    data_model_labels: str,
    return_display_names: bool = True,
) -> list[str]:
    """Get the range, i.e., all the valid values that are associated with a node label.

    Args:
        schema_url (str): Data Model URL
        node_label (str): Node / term for which you need to retrieve the range.
        return_display_names (bool, optional): If true returns the display names of the nodes.
            Defaults to True.

    Returns:
        list[str]: A list of nodes
    """
    data_model_parser = DataModelParser(path_to_data_model=schema_url)
    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    dmge = DataModelGraphExplorer(graph_data_model)

    node_range = dmge.get_node_range(node_label, return_display_names)
    return node_range


def get_if_node_required(
    schema_url: str, node_display_name: str, data_model_labels: str
) -> bool:
    """Check if the node is required

    Args:
        schema_url (str): Data Model URL
        node_display_name (str): display name

    Returns:
        True: If the given node is a "required" node.
        False: If the given node is not a "required" (i.e., an "optional") node.
    """
    data_model_parser = DataModelParser(path_to_data_model=schema_url)
    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    dmge = DataModelGraphExplorer(graph_data_model)

    is_required = dmge.get_node_required(node_display_name)

    return is_required


def get_node_validation_rules(
    schema_url: str, node_display_name: str, data_model_labels: str
) -> list:
    """
    Args:
        schema_url (str): Data Model URL
        node_display_name (str): node display name
    Returns:
        List of valiation rules for a given node.
    """
    # Instantiate DataModelParser
    data_model_parser = DataModelParser(path_to_data_model=schema_url)

    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    # Instantiate DataModelGraphExplorer
    dmge = DataModelGraphExplorer(graph_data_model)

    node_validation_rules = dmge.get_node_validation_rules(node_display_name)

    return node_validation_rules


def get_nodes_display_names(
    schema_url: str, node_list: list[str], data_model_labels: str
) -> list:
    """From a list of node labels retrieve their display names, return as list.

    Args:
        schema_url (str): Data Model URL
        node_list (List[str]): List of node labels.

    Returns:
        node_display_names (List[str]): List of node display names.

    """
    # Instantiate DataModelParser
    data_model_parser = DataModelParser(path_to_data_model=schema_url)

    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    # Instantiate DataModelGraphExplorer
    dmge = DataModelGraphExplorer(graph_data_model)

    node_display_names = dmge.get_nodes_display_names(node_list)
    return node_display_names


def get_schematic_version() -> str:
    """
    Return the current version of schematic
    """
    if "VERSION" in os.environ:
        version = os.environ["VERSION"]
    else:
        raise NotImplementedError(
            "Using this endpoint to check the version of schematic is only supported when the API is running in a docker container."
        )
    return version
