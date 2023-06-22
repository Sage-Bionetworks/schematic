#!/usr/bin/env python3

import click
import click_log
import logging
import sys
#TODO Remove timing after development
import time
import re

from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph
from schematic.schemas.data_model_validator import DataModelValidator
from schematic.schemas.data_model_jsonld import DataModelJsonLD, convert_graph_to_jsonld

from schematic.utils.cli_utils import query_dict
from schematic.utils.schema_util import export_schema
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
    "schema", type=click.Path(exists=True), metavar="<DATA_MODEL_CSV>", nargs=1
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
    metavar="<OUTPUT_PATH>",
    help=query_dict(schema_commands, ("schema", "convert", "output_jsonld")),
)
def convert(schema, base_schema, output_jsonld):
    """
    Running CLI to convert data model specification in CSV format to
    data model in JSON-LD format.
    """

    # TO DO: Throw these steps into their own function
    
    # get the start time
    st = time.time()

    # Instantiate Parser
    data_model_parser = DataModelParser(schema, base_schema)

    #Parse Model
    logger.info("Parsing data model.")
    parsed_data_model = data_model_parser.parse_model()

    # Convert parsed model to graph
    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model)

    # Generate graph
    logger.info("Generating data model graph.")
    graph_data_model = data_model_grapher.generate_data_model_graph()

    # Validate generated data model.
    logger.info("Validating the data model internally.")
    data_model_validator = DataModelValidator(data_model=graph_data_model)
    data_model_errors = data_model_validator.run_checks()
    
    # If there are errors log them.
    if data_model_errors:
        for err in data_model_errors:
            if isinstance(err, str):
                logger.error(err)
            elif isinstance(err, list):
                for e in err:
                    logger.error(e)
        # Actually raise error here with message.

    #data_model_jsonld_converter = DataModelJsonLD()
    logger.info("Converting data model to JSON-LD")
    jsonld_data_model = convert_graph_to_jsonld(Graph=graph_data_model)

    # output JSON-LD file alongside CSV file by default
    if output_jsonld is None:
        csv_no_ext = re.sub("[.]csv$", "", schema)
        output_jsonld = csv_no_ext + ".jsonld"

        logger.info(
            "By default, the JSON-LD output will be stored alongside the first "
            f"input CSV file. In this case, it will appear here: '{output_jsonld}'. "
            "You can use the `--output_jsonld` argument to specify another file path."
        )

    # saving updated schema.org schema
    try:
        export_schema(jsonld_data_model, output_jsonld)
        click.echo(f"The Data Model was created and saved to '{output_jsonld}' location.")
    except:
        click.echo(f"The Data Model could not be created by using '{output_jsonld}' location. Please check your file path again")

    # get the end time
    et = time.time()

    # get the execution time
    elapsed_time = time.strftime("%M:%S", time.gmtime(et - st))
    click.echo(f"Execution time: {elapsed_time} (M:S)")

    '''
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
    try:
        base_se.export_schema(output_jsonld)
        click.echo(f"The Data Model was created and saved to '{output_jsonld}' location.")
    except:
        click.echo(f"The Data Model could not be created by using '{output_jsonld}' location. Please check your file path again")
    '''
