"""visualization commands"""
# pylint: disable=unused-argument
# pylint: disable=useless-return
# pylint: disable=unused-variable
# pylint: disable=logging-fstring-interpolation

import logging
import sys
from typing import Any, get_args, cast

import click
import click_log  # type: ignore

from schematic.visualization.attributes_explorer import AttributesExplorer
from schematic.visualization.tangled_tree import TangledTree, FigureType, TextType
from schematic.utils.cli_utils import log_value_from_config, query_dict
from schematic.utils.schema_utils import DisplayLabelType
from schematic.help import viz_commands
from schematic.help import model_commands
from schematic.configuration.configuration import CONFIG

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

CONTEXT_SETTINGS = {"help_option_names": ["--help", "-h"]}  # help options


# invoke_without_command=True -> forces the application not to show aids before
# losing them with a --h
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
def viz(ctx: Any, config: str) -> None:  # use as `schematic model ...`
    """
    Sub-commands for Visualization methods.
    """
    try:
        logger.debug(f"Loading config file contents in '{config}'")
        CONFIG.load_config(config)
        ctx.obj = CONFIG
    except ValueError as exc:
        logger.error("'--config' not provided or environment variable not set.")
        logger.exception(exc)
        sys.exit(1)


@viz.command(
    "attributes",
)
@click_log.simple_verbosity_option(logger)
@click.option(
    "--data_model_labels",
    "-dml",
    default="class_label",
    type=click.Choice(list(get_args(DisplayLabelType)), case_sensitive=True),
    help=query_dict(
        viz_commands, ("visualization", "tangled_tree", "data_model_labels")
    ),
)
@click.pass_obj
def get_attributes(
    ctx: Any,
    data_model_labels: str,
) -> None:
    """Gets attributes"""
    # Get JSONLD file path
    path_to_jsonld = CONFIG.model_location
    log_value_from_config("jsonld", path_to_jsonld)

    # Validate inputs are literals
    assert data_model_labels in get_args(DisplayLabelType)
    data_model_labels = cast(DisplayLabelType, data_model_labels)

    # Run attributes explorer
    AttributesExplorer(path_to_jsonld, data_model_labels).parse_attributes(
        save_file=True
    )
    return


@viz.command("tangled_tree_text")
@click_log.simple_verbosity_option(logger)
@click.option(
    "-ft",
    "--figure_type",
    required=True,
    type=click.Choice(list(get_args(FigureType)), case_sensitive=False),
    help=query_dict(viz_commands, ("visualization", "tangled_tree", "figure_type")),
)
@click.option(
    "-tf",
    "--text_format",
    required=True,
    type=click.Choice(list(get_args(TextType)), case_sensitive=False),
    help=query_dict(viz_commands, ("visualization", "tangled_tree", "text_format")),
)
@click.option(
    "--data_model_labels",
    "-dml",
    default="class_label",
    type=click.Choice(list(get_args(DisplayLabelType)), case_sensitive=True),
    help=query_dict(
        viz_commands, ("visualization", "tangled_tree", "data_model_labels")
    ),
)
@click.pass_obj
def get_tangled_tree_text(
    ctx: Any,
    figure_type: str,
    text_format: str,
    data_model_labels: str,
) -> None:
    """Get text to be placed on the tangled tree visualization."""
    # Get JSONLD file path
    path_to_jsonld = CONFIG.model_location
    log_value_from_config("jsonld", path_to_jsonld)

    # Validate inputs are literals
    figure_type = figure_type.lower()
    text_format = text_format.lower()
    assert figure_type in get_args(FigureType)
    assert text_format in get_args(TextType)
    assert data_model_labels in get_args(DisplayLabelType)
    figure_type = cast(FigureType, figure_type)
    text_format = cast(TextType, text_format)
    data_model_labels = cast(DisplayLabelType, data_model_labels)

    # Initialize TangledTree
    tangled_tree = TangledTree(path_to_jsonld, figure_type, data_model_labels)

    # Get text for tangled tree.
    text_df = tangled_tree.get_text_for_tangled_tree(text_format, save_file=True)
    return


@viz.command("tangled_tree_layers")
@click_log.simple_verbosity_option(logger)
@click.option(
    "-ft",
    "--figure_type",
    required=True,
    type=click.Choice(list(get_args(FigureType)), case_sensitive=False),
    help=query_dict(viz_commands, ("visualization", "tangled_tree", "figure_type")),
)
@click.option(
    "--data_model_labels",
    "-dml",
    default="class_label",
    type=click.Choice(list(get_args(DisplayLabelType)), case_sensitive=True),
    help=query_dict(
        viz_commands, ("visualization", "tangled_tree", "data_model_labels")
    ),
)
@click.pass_obj
def get_tangled_tree_component_layers(
    ctx: Any,
    figure_type: str,
    data_model_labels: str,
) -> None:
    """Get the components that belong in each layer of the tangled tree visualization."""
    # Get JSONLD file path
    path_to_jsonld = CONFIG.model_location
    log_value_from_config("jsonld", path_to_jsonld)

    # Validate inputs are literal types
    figure_type = figure_type.lower()
    assert figure_type in get_args(FigureType)
    assert data_model_labels in get_args(DisplayLabelType)
    figure_type = cast(FigureType, figure_type)
    data_model_labels = cast(DisplayLabelType, data_model_labels)

    # Initialize Tangled Tree
    tangled_tree = TangledTree(path_to_jsonld, figure_type, data_model_labels)

    # Get tangled trees layers JSON.
    layers = tangled_tree.get_tangled_tree_layers(save_file=True)

    return
