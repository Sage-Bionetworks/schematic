#!/usr/bin/env python3

import click
import click_log
import logging
import sys
from jsonschema import ValidationError

from schematic.models.metadata import MetadataModel
from schematic.utils.cli_utils import query_dict, fill_in_from_config
from schematic import CONFIG

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

CONTEXT_SETTINGS = dict(help_option_names=['--help', '-h'])  # help options

# invoke_without_command=True -> forces the application not to show aids before losing them with a --h
@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
@click_log.simple_verbosity_option(logger)
@click.option('-c', '--config', envvar='SCHEMATIC_CONFIG', help='Path to schematic configuration file.')
@click.pass_context
def model(ctx, config): # use as `schematic model ...`
    """
    Sub-commands for Metadata Model related utilities/methods.
    """
    try:
        logger.debug(f"Loading config file contents in '{config}'")
        ctx.obj = CONFIG.load_config(config)
    except ValueError as e:
        logger.error("'--config' not provided or environment variable not set.")
        logger.exception(e)
        sys.exit(1)


# prototype based on submit_metadata_manifest()
@model.command('submit', short_help='Validation (optional) and submission of manifest files.')
@click_log.simple_verbosity_option(logger)
@click.option('-mp', '--manifest_path', help='Path to the user-populated manifest file.', required=True)
@click.option('-d', '--dataset_id', help='SynID of existing dataset on Synapse.', required=True)
@click.option('-vc', '--validate_component', help='Component to be used for validation', default=None)
@click.pass_obj
def submit_manifest(ctx, manifest_path, dataset_id, validate_component):
    """
    Running CLI with manifest validation (optional) and submission options.
    """
    jsonld = query_dict(CONFIG.DATA, ("model", "input", "location"))

    model_file_type = query_dict(CONFIG.DATA, ("model", "input", "file_type"))

    metadata_model = MetadataModel(inputMModelLocation=jsonld, 
                                   inputMModelLocationType=model_file_type)

    try:
        success = metadata_model.submit_metadata_manifest(manifest_path=manifest_path,
                                                          dataset_id=dataset_id,
                                                          validate_component=validate_component)

        if success:
            logger.info(f"File at '{manifest_path}' was successfully associated "
                        f"with dataset '{dataset_id}'.")
    except ValueError:
        logger.error(f"Component '{validate_component}' is not present in '{jsonld}', or is invalid.")
    except ValidationError:
        logger.error(f"Validation errors resulted while validating with '{validate_component}'.")
