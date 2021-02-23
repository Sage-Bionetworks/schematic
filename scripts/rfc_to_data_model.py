#!/usr/bin/env python3

import os
import sys
import re
import json
import logging

import pandas as pd
import click
import click_log

from schematic.schemas.explorer import SchemaExplorer
from schematic.utils.csv_utils import create_schema_classes, create_nx_schema_objects
from schematic import CONFIG, LOADER


logger = logging.getLogger(__name__)
click_log.basic_config(logger)


def _get_base_schema_path(base_schema):
    biothings_schema_path = LOADER.filename('data_models/biothings.model.jsonld')
    base_schema_path = biothings_schema_path if base_schema is None else base_schema
    
    return base_schema_path


def _convert_rfc_to_data_model(rfc_df, base_schema_path):
    # instantiate schema explorer
    base_se = SchemaExplorer()

    # load base schema (BioThings)
    base_se.load_schema(base_schema_path)

    # call parser code that converts a dataframe of the RFC
    # specs. into a JSON-LD data model
    base_se = create_nx_schema_objects(rfc_df, base_se)
    
    return base_se


@click.command(options_metavar="<options>")
@click_log.simple_verbosity_option(logger)
@click.argument("schema_csv", type=click.Path(exists=True), 
                metavar="<RFC_CSV>", 
                nargs=1)
@click.option("--base_schema", "-b", type=click.Path(exists=True), 
              metavar="<JSON-LD_SCHEMA>", 
              help="Base JSON-LD schema file. Defaults to BioThings.")
@click.option("--output_jsonld", "-o", type=click.Path(exists=True), 
              metavar="<OUTPUT_PATH>", 
              help="Output JSON-LD schema file.")
@click.option("--config", "-c", type=click.Path(exists=True), 
              metavar="<CONFIG_PATH>", 
              envvar='SCHEMATIC_CONFIG', 
              required=True,
              help="Configuration YAML file.")
def rfc_to_data_model(schema_csv, base_schema, output_jsonld, config):
    """The CLI utility that runs the parser code from schematic.utils.csv_utils
    to convert specs. from within RFCs to a JSON-LD data model.

    <RFC_CSV>: Input CSV schema file.
    """
    try:
        logger.debug(f"Loading config file contents in '{config}'")
        config_dict = CONFIG.load_config(config)
    except ValueError as e:
        logger.error("'--config' not provided or environment variable not set.")
        logger.exception(e)
        sys.exit(1)
        
    # if base_schema argument is provided, load that schema
    # else load the default BioThings schema
    base_schema_path = _get_base_schema_path(base_schema)
    
    # create data model from provided RFC
    rfc_df = pd.read_csv(schema_csv)
    base_se = _convert_rfc_to_data_model(rfc_df, base_schema_path)

    # output JSON-LD file alongside CSV file by default
    if output_jsonld is None:
        csv_no_ext = re.sub("[.]csv$", "", schema_csv)
        output_jsonld = csv_no_ext + ".jsonld"

        logger.info("By default, the JSON-LD output will be stored alongside the first "
                    f"input CSV file. In this case, it will appear here: '{output_jsonld}'. "
                    "You can use the `--output_jsonld` argument to specify another file path.")
                    
    # saving updated schema.org schema
    base_se.export_schema(output_jsonld)
    click.echo(f"The Data Model was created and saved to '{output_jsonld}' location.")


if __name__ == "__main__":
    rfc_to_data_model(prog_name="rfc_to_data_model")
    