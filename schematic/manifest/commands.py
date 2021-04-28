#!/usr/bin/env python3

import os
import logging

import click
import click_log
import logging
import sys
import pandas as pd

from schematic.manifest.generator import ManifestGenerator
from schematic.utils.cli_utils import fill_in_from_config, query_dict
from schematic.help import manifest_commands
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
    envvar="SCHEMATIC_CONFIG",
    help=query_dict(manifest_commands, ("manifest", "config")),
)
@click.pass_context
def manifest(ctx, config):  # use as `schematic manifest ...`
    """
    Sub-commands with Manifest Generation utilities/methods.
    """
    try:
        logger.debug(f"Loading config file contents in '{config}'")
        ctx.obj = CONFIG.load_config(config)
    except ValueError as e:
        logger.error("'--config' not provided or environment variable not set.")
        logger.exception(e)
        sys.exit(1)


# prototype based on getModelManifest() and get_manifest()
# use as `schematic config get positional_args --optional_args`
@manifest.command(
    "get", short_help=query_dict(manifest_commands, ("manifest", "get", "short_help"))
)
@click_log.simple_verbosity_option(logger)
# define the optional arguments
@click.option(
    "-t", "--title", help=query_dict(manifest_commands, ("manifest", "get", "title"))
)
@click.option(
    "-dt",
    "--data_type",
    help=query_dict(manifest_commands, ("manifest", "get", "data_type")),
)
@click.option(
    "-p", "--jsonld", help=query_dict(manifest_commands, ("manifest", "get", "jsonld"))
)
@click.option(
    "-d",
    "--dataset_id",
    help=query_dict(manifest_commands, ("manifest", "get", "dataset_id")),
)
@click.option(
    "-s",
    "--sheet_url",
    is_flag=True,
    help=query_dict(manifest_commands, ("manifest", "get", "sheet_url")),
)
@click.option(
    "-o",
    "--output_csv",
    help=query_dict(manifest_commands, ("manifest", "get", "output_csv")),
)
@click.option(
    "-a",
    "--use_annotations",
    is_flag=True,
    help=query_dict(manifest_commands, ("manifest", "get", "use_annotations")),
)
@click.option(
    "-oa",
    "--oauth",
    is_flag=True,
    help=query_dict(manifest_commands, ("manifest", "get", "oauth")),
)
@click.option(
    "-j",
    "--json_schema",
    help=query_dict(manifest_commands, ("manifest", "get", "json_schema")),
)
@click.pass_obj
def get_manifest(
    ctx,
    title,
    data_type,
    jsonld,
    dataset_id,
    sheet_url,
    output_csv,
    use_annotations,
    oauth,
    json_schema,
):
    """
    Running CLI with manifest generation options.
    """
    # optional parameters that need to be passed to ManifestGenerator()
    # can be read from config.yml as well
    data_type = fill_in_from_config("data_type", data_type, ("manifest", "data_type"))
    jsonld = fill_in_from_config("jsonld", jsonld, ("model", "input", "location"))
    title = fill_in_from_config("title", title, ("manifest", "title"), allow_none=True)
    json_schema = fill_in_from_config(
        "json_schema",
        json_schema,
        ("model", "input", "validation_schema"),
        allow_none=True,
    )

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
        dataset_id=dataset_id,
        sheet_url=sheet_url,
        json_schema=json_schema,
    )

    if sheet_url:
        logger.info("Find the manifest template using this Google Sheet URL:")
        click.echo(result)

    elif isinstance(result, pd.DataFrame):
        if output_csv is None:
            prefix, _ = os.path.splitext(jsonld)
            prefix_root, prefix_ext = os.path.splitext(prefix)
            if prefix_ext == ".model":
                prefix = prefix_root
            output_csv = f"{prefix}.{data_type}.manifest.csv"

        logger.info(
            f"Find the manifest template using this CSV file path: {output_csv}"
        )

        result.to_csv(output_csv, index=False)

    return result
