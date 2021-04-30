#!/usr/bin/env python3

import click
import click_log
import logging
import sys
import re

from schematic.schemas.df_parser import _convert_csv_to_data_model
from schematic.utils.cli_utils import query_dict
from schematic.help import schema_commands

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

CONTEXT_SETTINGS = dict(help_option_names=["--help", "-h"])  # help options

# invoke_without_command=True -> forces the application not to show aids before losing them with a --h
@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
def schema():  # use as `schematic model ...`
    """
    Sub-commands for Schema related utilities/methods.
    """
    pass


# prototype based on submit_metadata_manifest()
@schema.command(
    "convert",
    options_metavar="<options>",
    short_help=query_dict(schema_commands, ("schema", "convert", "short_help")),
)
@click_log.simple_verbosity_option(logger)
@click.argument(
    "schema_csv", type=click.Path(exists=True), metavar="<DATA_MODEL_CSV>", nargs=1
)
@click.option(
    "--base_schema",
    "-b",
    type=click.Path(exists=True),
    metavar="<JSON-LD_SCHEMA>",
    help=query_dict(schema_commands, ("schema", "convert", "base_schema")),
)
@click.option(
    "--output_jsonld",
    "-o",
    type=click.Path(exists=True),
    metavar="<OUTPUT_PATH>",
    help=query_dict(schema_commands, ("schema", "convert", "output_jsonld")),
)
def convert(schema_csv, base_schema, output_jsonld):
    """
    Running CLI to convert data model specification in CSV format to
    data model in JSON-LD format.
    """
    # convert RFC to Data Model
    base_se = _convert_csv_to_data_model(schema_csv, base_schema)

    # output JSON-LD file alongside CSV file by default
    if output_jsonld is None:
        csv_no_ext = re.sub("[.]csv$", "", schema_csv)
        output_jsonld = csv_no_ext + ".jsonld"

        logger.info(
            "By default, the JSON-LD output will be stored alongside the first "
            f"input CSV file. In this case, it will appear here: '{output_jsonld}'. "
            "You can use the `--output_jsonld` argument to specify another file path."
        )

    # saving updated schema.org schema
    base_se.export_schema(output_jsonld)
    click.echo(f"The Data Model was created and saved to '{output_jsonld}' location.")
