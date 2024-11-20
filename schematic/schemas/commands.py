"""Schema Commands"""

import logging
import time
import re
from typing import get_args, Optional, Any, Literal

import click
import click_log  # type: ignore

# pylint: disable=logging-fstring-interpolation

from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph
from schematic.schemas.data_model_validator import DataModelValidator
from schematic.schemas.data_model_jsonld import convert_graph_to_jsonld

from schematic.utils.schema_utils import DisplayLabelType
from schematic.utils.cli_utils import query_dict
from schematic.utils.schema_utils import export_schema, export_graph
from schematic.help import schema_commands

logger = logging.getLogger("schematic")
click_log.basic_config(logger)

CONTEXT_SETTINGS = {"help_option_names": ["--help", "-h"]}  # help options


# invoke_without_command=True -> forces the application not to show aids before
# losing them with a --h
@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
def schema() -> None:  # use as `schematic model ...`
    """
    Sub-commands for Schema related utilities/methods.
    """
    pass  # pylint: disable=unnecessary-pass


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
    "--data_model_labels",
    "-dml",
    default="class_label",
    type=click.Choice(list(get_args(DisplayLabelType)), case_sensitive=True),
    help=query_dict(schema_commands, ("schema", "convert", "data_model_labels")),
)
@click.option(
    "--output_jsonld",
    "-o",
    metavar="<OUTPUT_PATH>",
    help=query_dict(schema_commands, ("schema", "convert", "output_jsonld")),
)
@click.option("--output_path", help="Alias for --output_jsonld")
@click.option(
    "--output_type",
    "-ot",
    type=click.Choice(["jsonld", "graph", "all"], case_sensitive=False),
    default="jsonld",
    help=query_dict(schema_commands, ("schema", "convert", "output_type")),
)
def convert(
    schema: Any,
    data_model_labels: DisplayLabelType,
    output_jsonld: Optional[str],
    output_type: Optional[Literal["jsonld", "graph", "all"]],
    output_path: Optional[str],
) -> int:
    """
    Running CLI to convert data model specification in CSV format to
    data model in JSON-LD format.

    Note: Currently, not configured to build off of base model, so removing --base_schema
      argument for now
    """
    # pylint: disable=too-many-locals
    # pylint: disable=redefined-outer-name
    # pylint: disable=too-many-branches

    # get the start time
    start_time = time.time()

    # Instantiate Parser
    data_model_parser = DataModelParser(schema)

    # Parse Model
    click.echo("Parsing data model.")
    parsed_data_model = data_model_parser.parse_model()

    # Convert parsed model to graph
    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graphschema
    click.echo("Generating data model graph.")
    graph_data_model = data_model_grapher.graph

    # Validate generated data model.
    click.echo("Validating the data model internally.")
    data_model_validator = DataModelValidator(graph=graph_data_model)
    data_model_errors, data_model_warnings = data_model_validator.run_checks()

    # If there are errors log them.
    if data_model_errors:
        for err in data_model_errors:
            if isinstance(err, str):
                logger.error(err)
            elif isinstance(err, list):
                for error in err:
                    logger.error(error)

    # If there are warnings log them.
    if data_model_warnings:
        for war in data_model_warnings:
            if isinstance(war, str):
                logger.warning(war)
            elif isinstance(war, list):
                for warning in war:
                    logger.warning(warning)

    if output_path:
        output_jsonld = output_path

    if output_jsonld is None:
        output_file_no_ext = re.sub("[.](jsonld|csv|pickle)$", "", schema)
    else:
        output_file_no_ext = re.sub("[.](jsonld|csv|pickle)$", "", output_jsonld)

    click.echo(
        "By default, the JSON-LD output will be stored alongside the first "
        f"input CSV or JSON-LD file. In this case, it will appear here: '{output_jsonld}'. "
        "You can use the `--output_jsonld` argument to specify another file path."
    )

    if output_type in ["graph", "all"]:
        output_graph = output_file_no_ext + ".pickle"
        click.echo(f"Saving data model graph to '{output_graph}'.")
        export_graph(graph_data_model, output_graph)
        if output_type == "graph":
            return 0

    click.echo("Converting data model to JSON-LD")
    jsonld_data_model = convert_graph_to_jsonld(graph=graph_data_model)

    # output JSON-LD file alongside CSV file by default, get path.
    output_jsonld = output_file_no_ext + ".jsonld"

    # saving updated schema.org schema
    try:
        export_schema(jsonld_data_model, output_jsonld)
        click.echo(
            f"The Data Model was created and saved to '{output_jsonld}' location."
        )
    except Exception as exc:
        raise ValueError(
            f"The Data Model could not be created by using '{output_jsonld}' location. "
            "Please check your file path again"
        ) from exc

    # get the end time
    end_time = time.time()

    # get the execution time
    elapsed_time = time.strftime("%M:%S", time.gmtime(end_time - start_time))
    click.echo(f"Execution time: {elapsed_time} (M:S)")
    return 0
