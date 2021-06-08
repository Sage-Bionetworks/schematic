import os

import connexion
from flask import current_app, request, g

from schematic import CONFIG

from schematic.utils.cli_utils import get_from_config
from schematic.manifest.generator import ManifestGenerator


# def before_request(var1, var2):
#     # Do stuff before your route executes
#     pass
# def after_request(var1, var2):
#     # Do stuff after your route executes
#     pass

# @before_request
def get_manifest_route(title, oauth, use_annotations):
    CONFIG.load_config("./config.yml")
    jsonld = get_from_config(CONFIG.DATA, ("model", "input", "location"))

    # current_app.config['model']
    # current_app.config['input']
    # current_app.config['location']

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

    # call get_manifest() on manifest_generator
    result = manifest_generator.get_manifest(
        sheet_url=True, dataset_id=connexion.request.args["dataset_id"]
    )

    return result
