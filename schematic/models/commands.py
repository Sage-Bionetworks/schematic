#!/usr/bin/env python3

import logging
import sys

import click
import click_log

from jsonschema import ValidationError

from schematic.models.metadata import MetadataModel
from schematic.utils.cli_utils import get_from_config, fill_in_from_config, query_dict
from schematic.help import model_commands
from schematic.exceptions import MissingConfigValueError
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
def model(ctx, config):  # use as `schematic model ...`
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
@model.command(
    "submit", short_help=query_dict(model_commands, ("model", "submit", "short_help"))
)
@click_log.simple_verbosity_option(logger)
@click.option(
    "-mp",
    "--manifest_path",
    help=query_dict(model_commands, ("model", "submit", "manifest_path")),
)
@click.option(
    "-d",
    "--dataset_id",
    help=query_dict(model_commands, ("model", "submit", "dataset_id")),
)
@click.option(
    "-vc",
    "--validate_component",
    help=query_dict(model_commands, ("model", "submit", "validate_component")),
)
@click.option(
    "--use_schema_label/--use_display_label",
    "-sl/-dl",
    default=True,
    help=query_dict(model_commands, ("model", "submit", "use_schema_label")),
)
@click.option(
    "--hide_blanks",
    "-hb",
    is_flag=True,
    help=query_dict(model_commands,("model","submit","hide_blanks")),
)
@click.option(
    "--manifest_record_type",
    "-mrt",
    default='both',
    type=click.Choice(['table', 'entity', 'both'], case_sensitive=True),
    help=query_dict(model_commands, ("model", "submit", "manifest_record_type")))
@click.option(
    "-rr",
    "--restrict_rules",
    is_flag=True,
    help=query_dict(model_commands,("model","validate","restrict_rules")),
)
@click.pass_obj
def submit_manifest(
    ctx, manifest_path, dataset_id, validate_component, manifest_record_type, use_schema_label, hide_blanks, restrict_rules,
):
    """
    Running CLI with manifest validation (optional) and submission options.
    """
    jsonld = get_from_config(CONFIG.DATA, ("model", "input", "location"))

    model_file_type = get_from_config(CONFIG.DATA, ("model", "input", "file_type"))

    metadata_model = MetadataModel(
        inputMModelLocation=jsonld, inputMModelLocationType=model_file_type
    )

    try:
        manifest_id = metadata_model.submit_metadata_manifest(
            manifest_path=manifest_path,
            dataset_id=dataset_id,
            validate_component=validate_component,
            manifest_record_type=manifest_record_type,
            restrict_rules=restrict_rules,
            use_schema_label=use_schema_label,
            hide_blanks=hide_blanks,
        )

        '''
        if censored_manifest_id:
            logger.info(
                f"File at '{manifest_path}' was censored and successfully associated "
                f"with dataset '{dataset_id}'. "
                f"An uncensored version has also been associated with dataset '{dataset_id}' "
                f"and submitted to the Synapse Access Control Team to begin the process "
                f"of adding terms of use or review board approval."
            )
        '''
        if manifest_id:
            logger.info(
                f"File at '{manifest_path}' was successfully associated "
                f"with dataset '{dataset_id}'."
            )
    except ValueError:
        logger.error(
            f"Component '{validate_component}' is not present in '{jsonld}', or is invalid."
        )
    except ValidationError:
        logger.error(
            f"Validation errors resulted while validating with '{validate_component}'."
        )


# prototype based on validateModelManifest()
@model.command(
    "validate",
    short_help=query_dict(model_commands, ("model", "validate", "short_help")),
)
@click_log.simple_verbosity_option(logger)
@click.option(
    "-mp",
    "--manifest_path",
    type=click.Path(exists=True),
    required=True,
    help=query_dict(model_commands, ("model", "validate", "manifest_path")),
)
@click.option(
    "-dt",
    "--data_type",
    help=query_dict(model_commands, ("model", "validate", "data_type")),
)
@click.option(
    "-js",
    "--json_schema",
    help=query_dict(model_commands, ("model", "validate", "json_schema")),
)
@click.option(
    "-rr",
    "--restrict_rules",
    is_flag=True,
    help=query_dict(model_commands,("model","validate","restrict_rules")),
)
@click.pass_obj
def validate_manifest(ctx, manifest_path, data_type, json_schema, restrict_rules):
    """
    Running CLI for manifest validation.
    """
    data_type = fill_in_from_config("data_type", data_type, ("manifest", "data_type"))

    json_schema = fill_in_from_config(
        "json_schema",
        json_schema,
        ("model", "input", "validation_schema"),
        allow_none=True,
    )

    jsonld = get_from_config(CONFIG.DATA, ("model", "input", "location"))

    model_file_type = get_from_config(CONFIG.DATA, ("model", "input", "file_type"))

    metadata_model = MetadataModel(
        inputMModelLocation=jsonld, inputMModelLocationType=model_file_type
    )

    errors, warnings = metadata_model.validateModelManifest(
        manifestPath=manifest_path, rootNode=data_type, jsonSchema=json_schema, restrict_rules=restrict_rules,
    )

    if not errors:
        click.echo(
            "Your manifest has been validated successfully. "
            "There are no errors in your manifest, and it can "
            "be submitted without any modifications."
        )
    else:
        click.echo(errors)
