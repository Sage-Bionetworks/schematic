#!/usr/bin/env python3

from gc import callbacks
import logging
import sys
from time import perf_counter

import click
import click_log

from jsonschema import ValidationError

from schematic.models.metadata import MetadataModel
from schematic.utils.cli_utils import get_from_config, fill_in_from_config, query_dict, parse_synIDs, parse_comma_str_to_list
from schematic.help import model_commands
from schematic.exceptions import MissingConfigValueError
from schematic import CONFIG

logger = logging.getLogger('schematic')
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
    default='table_file_and_entities',
    type=click.Choice(['table_and_file', 'file_only', 'file_and_entities', 'table_file_and_entities'], case_sensitive=True),
    help=query_dict(model_commands, ("model", "submit", "manifest_record_type")))
@click.option(
    "-rr",
    "--restrict_rules",
    is_flag=True,
    help=query_dict(model_commands,("model","validate","restrict_rules")),
)
@click.option(
    "-ps",
    "--project_scope",
    default=None,
    callback=parse_synIDs,
    help=query_dict(model_commands, ("model", "validate", "project_scope")),
)
@click.option(
    "--table_manipulation",
    "-tm",
    default='replace',
    type=click.Choice(['replace', 'upsert'], case_sensitive=True),
    help=query_dict(model_commands, ("model", "submit", "table_manipulation")))
@click.pass_obj
def submit_manifest(
    ctx, manifest_path, dataset_id, validate_component, manifest_record_type, use_schema_label, hide_blanks, restrict_rules, project_scope, table_manipulation,
):
    """
    Running CLI with manifest validation (optional) and submission options.
    """
    
    jsonld = get_from_config(CONFIG.DATA, ("model", "input", "location"))

    model_file_type = get_from_config(CONFIG.DATA, ("model", "input", "file_type"))

    metadata_model = MetadataModel(
        inputMModelLocation=jsonld, inputMModelLocationType=model_file_type
    )


    manifest_id = metadata_model.submit_metadata_manifest(
        path_to_json_ld = jsonld,
        manifest_path=manifest_path,
        dataset_id=dataset_id,
        validate_component=validate_component,
        manifest_record_type=manifest_record_type,
        restrict_rules=restrict_rules,
        use_schema_label=use_schema_label,
        hide_blanks=hide_blanks,
        project_scope=project_scope,
        table_manipulation=table_manipulation,
    )
    
    if manifest_id:
        logger.info(
            f"File at '{manifest_path}' was successfully associated "
            f"with dataset '{dataset_id}'."
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
    callback=parse_comma_str_to_list,
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
@click.option(
    "-ps",
    "--project_scope",
    default=None,
    callback=parse_synIDs,
    help=query_dict(model_commands, ("model", "validate", "project_scope")),
)
@click.pass_obj
def validate_manifest(ctx, manifest_path, data_type, json_schema, restrict_rules,project_scope):
    """
    Running CLI for manifest validation.
    """
    if not data_type:
        data_type = fill_in_from_config("data_type", data_type, ("manifest", "data_type"))
    
    try:
        len(data_type) == 1
    except:
        logger.error(
            f"Can only validate a single data_type at a time. Please provide a single data_type"
        )

    data_type = data_type[0]

    json_schema = fill_in_from_config(
        "json_schema",
        json_schema,
        ("model", "input", "validation_schema"),
        allow_none=True,
    )
    t_validate = perf_counter()
    jsonld = get_from_config(CONFIG.DATA, ("model", "input", "location"))

    model_file_type = get_from_config(CONFIG.DATA, ("model", "input", "file_type"))

    metadata_model = MetadataModel(
        inputMModelLocation=jsonld, inputMModelLocationType=model_file_type
    )

    errors, warnings = metadata_model.validateModelManifest(
        manifestPath=manifest_path, rootNode=data_type, jsonSchema=json_schema, restrict_rules=restrict_rules, project_scope=project_scope,
    )

    if not errors:
        click.echo(
            "Your manifest has been validated successfully. "
            "There are no errors in your manifest, and it can "
            "be submitted without any modifications."
        )
    else:
        click.echo(errors)

    logger.debug(
        f"Total elapsed time {perf_counter()-t_validate} seconds"
    )
