import os
import shutil
import tempfile
import shutil
import urllib.request

import connexion
from flask import current_app as app, request, g, jsonify

from schematic import CONFIG

from schematic.manifest.generator import ManifestGenerator
from schematic.models.metadata import MetadataModel


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

    # request.data[]
    data_type = connexion.request.args["data_type"]

    # create object of type ManifestGenerator
    manifest_generator = ManifestGenerator(
        path_to_json_ld=jsonld,
        title=title,
        root=data_type,
        oauth=oauth,
        use_annotations=use_annotations,
    )

    dataset_id = connexion.request.args["dataset_id"]

    # call get_manifest() on manifest_generator
    result = manifest_generator.get_manifest(sheet_url=True, dataset_id=dataset_id)

    return result


def validate_manifest_route(schema_url, data_type):
    # call config_handler()
    config_handler()

    manifest_file = connexion.request.files["csv_file"]

    # save contents of incoming manifest CSV file to temp file
    temp_dir = tempfile.gettempdir()
    # path to temp file where manifest file contents will be saved
    temp_path = os.path.join(temp_dir, manifest_file.filename)
    # save content
    manifest_file.save(temp_path)

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

    manifest_file = connexion.request.files["csv_file"]

    # save contents of incoming manifest CSV file to temp file
    temp_dir = tempfile.gettempdir()
    # path to temp file where manifest file contents will be saved
    temp_path = os.path.join(temp_dir, manifest_file.filename)
    # save content
    manifest_file.save(temp_path)

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

    return success
