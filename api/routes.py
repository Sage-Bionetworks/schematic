import os
import shutil
import tempfile
import shutil
import urllib.request

import connexion
from connexion.decorators.uri_parsing import Swagger2URIParser
from flask import current_app as app, request, g, jsonify
from werkzeug.debug import DebuggedApplication

from schematic import CONFIG

from schematic.manifest.generator import ManifestGenerator
from schematic.models.metadata import MetadataModel
from schematic.schemas.generator import SchemaGenerator

from schematic.store.synapse import SynapseStorage


# def before_request(var1, var2):
#     # Do stuff before your route executes
#     pass
# def after_request(var1, var2):
#     # Do stuff after your route executes
#     pass


def config_handler():
    path_to_config = app.config["SCHEMATIC_CONFIG"]

    # check if file exists at the path created, i.e., app.config['SCHEMATIC_CONFIG']
    if os.path.isfile(path_to_config):
        CONFIG.load_config(path_to_config)
    else:
        raise FileNotFoundError(
            f"No configuration file was found at this path: {path_to_config}"
        )

def csv_path_handler():
    manifest_file = connexion.request.files["csv_file"]

    # save contents of incoming manifest CSV file to temp file
    temp_dir = tempfile.gettempdir()
    # path to temp file where manifest file contents will be saved
    temp_path = os.path.join(temp_dir, manifest_file.filename)
    # save content
    manifest_file.save(temp_path)

    return temp_path

def get_temp_jsonld(schema_url):
    # retrieve a JSON-LD via URL and store it in a temporary location
    with urllib.request.urlopen(schema_url) as response:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonld") as tmp_file:
            shutil.copyfileobj(response, tmp_file)

    # get path to temporary JSON-LD file
    return tmp_file.name


# @before_request
def get_manifest_route(schema_url, title, oauth, use_annotations):
    # call config_handler()
    config_handler()

    # get path to temporary JSON-LD file
    jsonld = get_temp_jsonld(schema_url)

    # Gather all data_types to make manifests for.
    all_args = connexion.request.args
    args_dict = dict(all_args.lists())
    data_type = args_dict['data_type']

    def create_single_manifest(data_type):
        # create object of type ManifestGenerator
        manifest_generator = ManifestGenerator(
            path_to_json_ld=jsonld,
            title=t,
            root=data_type,
            oauth=oauth,
            use_annotations=use_annotations,
        )

        dataset_id = connexion.request.args["dataset_id"]
        if dataset_id == 'None':
            dataset_id = None

        result = manifest_generator.get_manifest(
            dataset_id=dataset_id, sheet_url=True,
        )
        return result

    # Gather all returned result urls
    all_results = []
    if data_type[0] == 'all manifests':
        sg = SchemaGenerator(path_to_json_ld=jsonld)
        component_digraph = sg.se.get_digraph_by_edge_type('requiresComponent')
        components = component_digraph.nodes()
        for component in components:
            t = f'{title}.{component}.manifest'
            result = create_single_manifest(data_type = component)
            all_results.append(result)
    else:
        for dt in data_type:
            if len(data_type) > 1:
                t = f'{title}.{dt}.manifest'
            else:
                t = title
            result = create_single_manifest(data_type = dt)
            all_results.append(result)

    return all_results


def validate_manifest_route(schema_url, data_type):
    # call config_handler()
    config_handler()

    # manifest_file = connexion.request.files["csv_file"]

    # # save contents of incoming manifest CSV file to temp file
    # temp_dir = tempfile.gettempdir()
    # # path to temp file where manifest file contents will be saved
    # temp_path = os.path.join(temp_dir, manifest_file.filename)
    # # save content
    # manifest_file.save(temp_path)
    temp_path = csv_path_handler()

    # get path to temporary JSON-LD file
    jsonld = get_temp_jsonld(schema_url)

    metadata_model = MetadataModel(
        inputMModelLocation=jsonld, inputMModelLocationType="local"
    )

    errors = metadata_model.validateModelManifest(
        manifestPath=temp_path, rootNode=data_type
    )

    return errors


def submit_manifest_route(schema_url):
    # call config_handler()
    config_handler()

    # manifest_file = connexion.request.files["csv_file"]

    # # save contents of incoming manifest CSV file to temp file
    # temp_dir = tempfile.gettempdir()
    # # path to temp file where manifest file contents will be saved
    # temp_path = os.path.join(temp_dir, manifest_file.filename)
    # # save content
    # manifest_file.save(temp_path)
    temp_path = csv_path_handler()

    # get path to temporary JSON-LD file
    jsonld = get_temp_jsonld(schema_url)

    dataset_id = connexion.request.args["dataset_id"]

    data_type = connexion.request.args["data_type"]

    metadata_model = MetadataModel(
        inputMModelLocation=jsonld, inputMModelLocationType="local"
    )

    success = metadata_model.submit_metadata_manifest(
        manifest_path=temp_path, dataset_id=dataset_id, validate_component=data_type,
    )

    # if data_type == 'None':
    #     success = metadata_model.submit_metadata_manifest(
    #         manifest_path=temp_path, dataset_id=dataset_id, validate_component=None,
    #     )

    return success

def get_storage_projects(input_token, syn_master_file_view, syn_master_file_name):
    store = SynapseStorage(input_token=input_token, syn_master_file_view= syn_master_file_view, syn_master_file_name= syn_master_file_name)

    lst_storage_projects = store.getStorageProjects()
    

    return lst_storage_projects

def get_storage_projects_datasets(input_token, syn_master_file_view, syn_master_file_name, project_id):
    store = SynapseStorage(input_token=input_token, syn_master_file_view= syn_master_file_view, syn_master_file_name= syn_master_file_name)

    sorted_dataset_lst = store.getStorageDatasetsInProject(projectId = project_id)
    

    return sorted_dataset_lst


def get_files_storage_dataset(input_token, syn_master_file_view, syn_master_file_name, dataset_id, full_path, file_names=None):
    store = SynapseStorage(input_token=input_token, syn_master_file_view= syn_master_file_view, syn_master_file_name= syn_master_file_name)

    # no file names were specified (file_names = [''])
    if file_names and not all(file_names): 
        file_names=None
    file_lst = store.getFilesInStorageDataset(datasetId=dataset_id, fileNames=file_names, fullpath=full_path)
    return file_lst


def get_associate_meta_data_with_files(input_token, syn_master_file_view, syn_master_file_name, dataset_id, use_schema_label):
    store = SynapseStorage(input_token=input_token, syn_master_file_view= syn_master_file_view, syn_master_file_name= syn_master_file_name)

    temp_path = csv_path_handler()

    #config_handler()

    # get path to temporary JSON-LD file
    #jsonld = get_temp_jsonld("https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld")

    # metadata_model = MetadataModel(
    #     inputMModelLocation=jsonld, inputMModelLocationType="local"
    # )

    manifestSynapseFileId = store.associateMetadataWithFiles(metadataManifestPath=temp_path, datasetId=dataset_id, useSchemaLabel=use_schema_label, hideBlanks=False)

    return manifestSynapseFileId