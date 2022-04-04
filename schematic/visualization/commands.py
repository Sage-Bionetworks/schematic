#!/usr/bin/env python3

import logging
import sys

import click
import click_log

from schematic.visualization.attributes_explorer import AttributesExplorer
from schematic.visualization.tangled_tree import TangledTree
from schematic.utils.cli_utils import get_from_config, fill_in_from_config, query_dict
from schematic.help import viz_commands
from schematic.help import model_commands
from schematic import CONFIG

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

CONTEXT_SETTINGS = dict(help_option_names=["--help", "-h"])  # help options

# invoke_without_command=True -> forces the application not to show aids before losing them with a --h
@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
@click_log.simple_verbosity_option(logger)
@click.option(
    "-c",
    "--config",
    type=click.Path(),
    envvar="SCHEMATIC_CONFIG",
    help=query_dict(model_commands, ("model", "config")),
)
@click.pass_context
def viz(ctx, config):  # use as `schematic model ...`
    """
    Sub-commands for Visualization methods.
    """
    try:
        logger.debug(f"Loading config file contents in '{config}'")
        ctx.obj = CONFIG.load_config(config)
    except ValueError as e:
        logger.error("'--config' not provided or environment variable not set.")
        logger.exception(e)
        sys.exit(1)

@viz.command(
    "attributes",
)
@click_log.simple_verbosity_option(logger)

@click.pass_obj
def get_attributes(ctx):
    """
    
    """
    # Get JSONLD file path
    path_to_jsonld = get_from_config(CONFIG.DATA, ("model", "input", "location"))
    # Run attributes explorer
    AttributesExplorer(path_to_jsonld).parse_attributes(save_file=True)
    return

@viz.command(
    "tangled_tree_text"
)
@click_log.simple_verbosity_option(logger)
@click.option(
    "-ft",
    "--figure_type",
    type=click.Choice(['component', 'dependency'], case_sensitive=False),
    help=query_dict(viz_commands, ("visualization", "tangled_tree", "figure_type")),
)
@click.option(
    "-tf",
    "--text_format",
    type=click.Choice(['plain', 'highlighted'], case_sensitive=False),
    help=query_dict(viz_commands, ("visualization", "tangled_tree", "text_format")),
)

@click.pass_obj
def get_tangled_tree_text(ctx, figure_type, text_format):
    """ Get text to be placed on the tangled tree visualization.
    """
    # Get JSONLD file path
    path_to_jsonld = get_from_config(CONFIG.DATA, ("model", "input", "location"))
    
    # Initialize TangledTree
    tangled_tree = TangledTree(path_to_jsonld, figure_type)

    # Get text for tangled tree.
    text_df = tangled_tree.get_text_for_tangled_tree(text_format, save_file=True)
    return

@viz.command(
    "tangled_tree_layers"
)
@click_log.simple_verbosity_option(logger)
@click.option(
    "-ft",
    "--figure_type",
    type=click.Choice(['component', 'dependency'], case_sensitive=False),
    help=query_dict(viz_commands, ("visualization", "tangled_tree", "figure_type")),
)

@click.pass_obj
def get_tangled_tree_component_layers(ctx, figure_type):
    ''' Get the components that belong in each layer of the tangled tree visualization.
    '''
    # Get JSONLD file path
    path_to_jsonld = get_from_config(CONFIG.DATA, ("model", "input", "location"))
    
    # Initialize Tangled Tree
    tangled_tree = TangledTree(path_to_jsonld, figure_type)
    
    # Get tangled trees layers JSON.
    layers = tangled_tree.get_tangled_tree_layers(save_file=True)

    return
