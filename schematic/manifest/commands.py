#!/usr/bin/env python3

import click
from schematic.manifest.generator import ManifestGenerator

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
@manifest.command('get', short_help='Prepares the manifest URL based on provided schema')
# define the positional arguments
@click.argument('title', type=str)
@click.argument('path_to_json_ld', type=str)
@click.argument('data_type', type=str)
# define the optional arguments
@click.option('--dataset_id', type=str)
@click.option('--sheet_url', type=bool)
@click.option('--json_schema', type=str)
def get_manifest(title, path_to_json_ld, data_type, 
                 dataset_id, sheet_url, json_schema):
    """
    Running CLI with manifest generation options.
    """
    # create object of type ManifestGenerator
    manifest_generator = ManifestGenerator(title=title, 
                                           path_to_json_ld=path_to_json_ld, 
                                           root=data_type)

    # call get_manifest() on manifest_generator
    click.echo(manifest_generator.get_manifest(dataset_id=dataset_id, 
                                               sheet_url=sheet_url, 
                                               json_schema=json_schema))
