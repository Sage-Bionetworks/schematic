#!/usr/bin/env python3

import logging
import sys
from gc import callbacks
from time import perf_counter
from typing import Optional, get_args

import click
import click_log
from jsonschema import ValidationError

from schematic.configuration.configuration import CONFIG
from schematic.exceptions import MissingConfigValueError
from schematic.help import model_commands
from schematic.models.metadata import MetadataModel
from schematic.utils.cli_utils import (
    log_value_from_config,
    parse_comma_str_to_list,
    parse_syn_ids,
    query_dict,
)
from schematic.utils.schema_utils import DisplayLabelType

logger = logging.getLogger("schematic")
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
        CONFIG.load_config(config)
        ctx.obj = CONFIG
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
    "--hide_blanks",
    "-hb",
    is_flag=True,
    help=query_dict(model_commands, ("model", "submit", "hide_blanks")),
)
@click.option(
    "--manifest_record_type",
    "-mrt",
    default="table_file_and_entities",
    type=click.Choice(
        ["table_and_file", "file_only", "file_and_entities", "table_file_and_entities"],
        case_sensitive=True,
    ),
    help=query_dict(model_commands, ("model", "submit", "manifest_record_type")),
)
@click.option(
    "-rr",
    "--restrict_rules",
    is_flag=True,
    help=query_dict(model_commands, ("model", "validate", "restrict_rules")),
)
@click.option(
    "--file_annotations_upload/--no-file_annotations_upload",
    "-fa/-no-fa",
    default=True,
    is_flag=True,
    help=query_dict(model_commands, ("model", "submit", "file_annotations_upload")),
)
@click.option(
    "-ps",
    "--project_scope",
    default=None,
    callback=parse_syn_ids,
    help=query_dict(model_commands, ("model", "validate", "project_scope")),
)
@click.option(
    "-ds",
    "--dataset_scope",
    default=None,
    help=query_dict(model_commands, ("model", "validate", "dataset_scope")),
)
@click.option(
    "--table_manipulation",
    "-tm",
    default="replace",
    type=click.Choice(["replace", "upsert"], case_sensitive=True),
    help=query_dict(model_commands, ("model", "submit", "table_manipulation")),
)
@click.option(
    "--data_model_labels",
    "-dml",
    default="class_label",
    type=click.Choice(list(get_args(DisplayLabelType)), case_sensitive=True),
    help=query_dict(model_commands, ("model", "submit", "data_model_labels")),
)
@click.option(
    "--table_column_names",
    "-tcn",
    default="class_label",
    type=click.Choice(
        ["class_label", "display_label", "display_name"], case_sensitive=True
    ),
    help=query_dict(model_commands, ("model", "submit", "table_column_names")),
)
@click.option(
    "--annotation_keys",
    "-ak",
    default="class_label",
    type=click.Choice(["class_label", "display_label"], case_sensitive=True),
    help=query_dict(model_commands, ("model", "submit", "annotation_keys")),
)
@click.pass_obj
def submit_manifest(
    ctx,
    manifest_path: str,
    dataset_id: str,
    validate_component: Optional[str],
    manifest_record_type: Optional[str],
    hide_blanks: Optional[bool],
    restrict_rules: Optional[bool],
    project_scope: Optional[list[str]],
    dataset_scope: Optional[str],
    table_manipulation: Optional[str],
    data_model_labels: Optional[str],
    table_column_names: Optional[str],
    annotation_keys: Optional[str],
    file_annotations_upload: Optional[bool],
):
    """
    Running CLI with manifest validation (optional) and submission options.
    """

    jsonld = CONFIG.model_location
    log_value_from_config("jsonld", jsonld)

    metadata_model = MetadataModel(
        inputMModelLocation=jsonld,
        inputMModelLocationType="local",
        data_model_labels=data_model_labels,
    )

    manifest_id = metadata_model.submit_metadata_manifest(
        manifest_path=manifest_path,
        dataset_id=dataset_id,
        validate_component=validate_component,
        manifest_record_type=manifest_record_type,
        restrict_rules=restrict_rules,
        hide_blanks=hide_blanks,
        project_scope=project_scope,
        dataset_scope=dataset_scope,
        table_manipulation=table_manipulation,
        table_column_names=table_column_names,
        annotation_keys=annotation_keys,
        file_annotations_upload=file_annotations_upload,
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
    help=query_dict(model_commands, ("model", "validate", "restrict_rules")),
)
@click.option(
    "-ps",
    "--project_scope",
    default=None,
    callback=parse_syn_ids,
    help=query_dict(model_commands, ("model", "validate", "project_scope")),
)
@click.option(
    "-ds",
    "--dataset_scope",
    default=None,
    help=query_dict(model_commands, ("model", "validate", "dataset_scope")),
)
@click.option(
    "--data_model_labels",
    "-dml",
    default="class_label",
    type=click.Choice(list(get_args(DisplayLabelType)), case_sensitive=True),
    help=query_dict(model_commands, ("model", "validate", "data_model_labels")),
)
@click.pass_obj
def validate_manifest(
    ctx,
    manifest_path: str,
    data_type: Optional[list[str]],
    json_schema: Optional[str],
    restrict_rules: Optional[bool],
    project_scope: Optional[list[str]],
    dataset_scope: Optional[str],
    data_model_labels: Optional[str],
):
    """
    Running CLI for manifest validation.
    """
    if data_type is None:
        data_type = CONFIG.manifest_data_type
        log_value_from_config("data_type", data_type)

    try:
        len(data_type) == 1
    except:
        logger.error(
            f"Can only validate a single data_type at a time. Please provide a single data_type"
        )

    data_type = data_type[0]

    t_validate = perf_counter()

    jsonld = CONFIG.model_location
    log_value_from_config("jsonld", jsonld)

    metadata_model = MetadataModel(
        inputMModelLocation=jsonld,
        inputMModelLocationType="local",
        data_model_labels=data_model_labels,
    )

    errors, warnings = metadata_model.validateModelManifest(
        manifestPath=manifest_path,
        rootNode=data_type,
        jsonSchema=json_schema,
        restrict_rules=restrict_rules,
        project_scope=project_scope,
        dataset_scope=dataset_scope,
    )

    if not errors:
        click.echo(
            "Your manifest has been validated successfully. "
            "There are no errors in your manifest, and it can "
            "be submitted without any modifications."
        )
    else:
        click.echo(errors)

    logger.debug(f"Total elapsed time {perf_counter()-t_validate} seconds")
