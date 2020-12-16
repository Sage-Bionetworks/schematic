#!/usr/bin/env python3

import click
from schematic.manifest.generator import ManifestGenerator
from schematic.utils.cli_utils import fill_in_from_config
from schematic import CONFIG

CONTEXT_SETTINGS = dict(help_option_names=['--help', '-h'])  # help options

# invoke_without_command=True -> forces the application not to show aids before losing them with a --h
@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
def manifest(): # use as `schematic manifest ...`
    """
    Sub-commands with Manifest Generation utilities/methods.
    """
    pass


# prototype based on getModelManifest() and get_manifest()
# use as `schematic config get positional_args --optional_args`
@manifest.command('get', short_help='Prepares the manifest URL based on provided schema.')
# define the optional arguments
@click.option('-t', '--title', help='Title of generated manifest file.')
@click.option('-d', '--data_type', help='Data type/component from JSON-LD schema to be used for manifest generation.')
@click.option('-p', '--path_to_json_ld', help='Path to JSON-LD schema.')
@click.option('-d', '--dataset_id', help='SynID of existing dataset on Synapse.')
@click.option('-s', '--sheet_url', type=bool, help='Enable/disable URL generation.')
@click.option('-j', '--json_schema', help='Path to JSON Schema (validation schema).')
@click.option('-c', '--config', help='Path to schematic configuration file.')
def get_manifest(title, data_type, path_to_json_ld, 
                 dataset_id, sheet_url, json_schema, 
                 config):
    """
    Running CLI with manifest generation options.
    """
    config_data = CONFIG.load_config(config)

    # optional parameters that need to be passed to ManifestGenerator()
    # can be read from config.yml as well
    title = fill_in_from_config(
        "title", title, CONFIG, ("manifest", "title")
    )
    data_type = fill_in_from_config(
        "data_type", data_type, CONFIG, ("manifest", "data_type")
    )
    path_to_json_ld = fill_in_from_config(
        "path_to_json_ld", path_to_json_ld, CONFIG, ("model", "input", "location")
    )
    json_schema = fill_in_from_config(
        "json_schema", json_schema, CONFIG, ("model", "input", "validation_schema")
    )

    # create object of type ManifestGenerator
    manifest_generator = ManifestGenerator(title=title,
                                           path_to_json_ld=path_to_json_ld,
                                           root=data_type)
        
    # call get_manifest() on manifest_generator
    click.echo(manifest_generator.get_manifest(dataset_id=dataset_id, 
                                               sheet_url=sheet_url, 
                                               json_schema=json_schema))
