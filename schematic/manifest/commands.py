import json
import os
import pandas as pd
import logging
from pathlib import Path
import sys
from typing import get_args, List
import click
import click_log

from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.manifest.generator import ManifestGenerator

from schematic.utils.schema_utils import DisplayLabelType
from schematic.utils.cli_utils import log_value_from_config, query_dict, parse_syn_ids
from schematic.utils.google_api_utils import export_manifest_csv

from schematic.help import manifest_commands

from schematic.store.synapse import SynapseStorage
from schematic.configuration.configuration import CONFIG

logger = logging.getLogger("schematic")
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
        CONFIG.load_config(config)
        ctx.obj = CONFIG
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
    "-p",
    "--path_to_data_model",
    help=query_dict(manifest_commands, ("manifest", "get", "path_to_data_model")),
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
    "-oxlsx",
    "--output_xlsx",
    help=query_dict(manifest_commands, ("manifest", "get", "output_xlsx")),
)
@click.option(
    "-a",
    "--use_annotations",
    is_flag=True,
    help=query_dict(manifest_commands, ("manifest", "get", "use_annotations")),
)
@click.option(
    "-js",
    "--json_schema",
    help=query_dict(manifest_commands, ("manifest", "get", "json_schema")),
)
@click.option(
    "-av",
    "--alphabetize_valid_values",
    default="ascending",
    help=query_dict(manifest_commands, ("manifest", "get", "alphabetize_valid_values")),
)
@click.option(
    "--data_model_labels",
    "-dml",
    default="class_label",
    type=click.Choice(list(get_args(DisplayLabelType)), case_sensitive=True),
    help=query_dict(manifest_commands, ("manifest", "get", "data_model_labels")),
)
@click.pass_obj
def get_manifest(
    ctx,
    title,
    data_type,
    path_to_data_model,
    dataset_id,
    sheet_url,
    output_csv,
    use_annotations,
    json_schema,
    output_xlsx,
    alphabetize_valid_values,
    data_model_labels,
):
    """
    Running CLI with manifest generation options.
    """
    # Optional parameters that need to be passed to ManifestGenerator()
    # If CLI parameters are None they are gotten from the CONFIG object and logged
    if data_type is None:
        data_type = CONFIG.manifest_data_type
        log_value_from_config("data_type", data_type)
    if path_to_data_model is None:
        path_to_data_model = CONFIG.model_location
        log_value_from_config("path_to_data_model", path_to_data_model)
    if title is None:
        title = CONFIG.manifest_title
        log_value_from_config("title", title)

    data_model_parser = DataModelParser(path_to_data_model=path_to_data_model)

    # Parse Model
    logger.info("Parsing data model.")
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    logger.info("Generating data model graph.")
    graph_data_model = data_model_grapher.graph

    def create_single_manifest(data_type, output_csv=None, output_xlsx=None):
        # create object of type ManifestGenerator
        manifest_generator = ManifestGenerator(
            path_to_data_model=path_to_data_model,
            graph=graph_data_model,
            title=t,
            root=data_type,
            use_annotations=use_annotations,
            alphabetize_valid_values=alphabetize_valid_values,
        )

        # call get_manifest() on manifest_generator
        # if output_xlsx gets specified, output_format = "excel"
        if output_xlsx:
            output_format = "excel"
            # if file name is in the path, and that file does not exist
            if not os.path.exists(output_xlsx):
                if ".xlsx" or ".xls" in output_xlsx:
                    path = Path(output_xlsx)
                    output_path = path.parent.absolute()
                    if not os.path.exists(output_path):
                        raise ValueError(
                            f"{output_path} does not exists. Please try a valid file path"
                        )
                else:
                    raise ValueError(
                        f"{output_xlsx} does not exists. Please try a valid file path"
                    )
            else:
                # Check if base path itself exists.
                if not os.path.exists(os.path.dirname(output_xlsx)):
                    raise ValueError(
                        f"{output_xlsx} does not exists. Please try a valid file path"
                    )
                output_path = output_xlsx
        else:
            output_format = None
            output_path = None

        result = manifest_generator.get_manifest(
            dataset_id=dataset_id,
            sheet_url=sheet_url,
            json_schema=json_schema,
            output_format=output_format,
            output_path=output_path,
        )

        if sheet_url:
            logger.info("Find the manifest template using this Google Sheet URL:")
            click.echo(result)
        if output_csv is None and output_xlsx is None:
            prefix, _ = os.path.splitext(path_to_data_model)
            prefix_root, prefix_ext = os.path.splitext(prefix)
            if prefix_ext == ".model":
                prefix = prefix_root
            output_csv = f"{prefix}.{data_type}.manifest.csv"

        elif output_xlsx:
            logger.info(
                f"Find the manifest template using this Excel file path: {output_xlsx}"
            )
            return result
        export_manifest_csv(file_path=output_csv, manifest=result)
        logger.info(
            f"Find the manifest template using this CSV file path: {output_csv}"
        )
        return result

    if type(data_type) is str:
        data_type = [data_type]

    if data_type[0] == "all manifests":
        # Feed graph into the data model graph explorer
        dmge = DataModelGraphExplorer(graph_data_model)
        component_digraph = dmge.get_digraph_by_edge_type("requiresComponent")
        components = component_digraph.nodes()
        for component in components:
            t = f"{title}.{component}.manifest"
            result = create_single_manifest(data_type=component)
    else:
        for dt in data_type:
            if len(data_type) > 1 and not output_xlsx:
                t = f"{title}.{dt}.manifest"
            elif output_xlsx:
                if ".xlsx" or ".xls" in output_xlsx:
                    title_with_extension = os.path.basename(output_xlsx)
                    t = title_with_extension.split(".")[0]
            else:
                t = title
            result = create_single_manifest(
                data_type=dt, output_csv=output_csv, output_xlsx=output_xlsx
            )

    return result


@manifest.command(
    "migrate",
    short_help=query_dict(manifest_commands, ("manifest", "migrate", "short_help")),
)
@click_log.simple_verbosity_option(logger)
# define the optional arguments
@click.option(
    "-ps",
    "--project_scope",
    default=None,
    callback=parse_syn_ids,
    help=query_dict(manifest_commands, ("manifest", "migrate", "project_scope")),
)
@click.option(
    "-ap",
    "--archive_project",
    default=None,
    help=query_dict(manifest_commands, ("manifest", "migrate", "archive_project")),
)
@click.option(
    "-p", "--jsonld", help=query_dict(manifest_commands, ("manifest", "get", "jsonld"))
)
@click.option(
    "-re",
    "--return_entities",
    is_flag=True,
    default=False,
    help=query_dict(manifest_commands, ("manifest", "migrate", "return_entities")),
)
@click.option(
    "-dr",
    "--dry_run",
    is_flag=True,
    default=False,
    help=query_dict(manifest_commands, ("manifest", "migrate", "dry_run")),
)
@click.pass_obj
def migrate_manifests(
    ctx,
    project_scope: List,
    archive_project: str,
    jsonld: str,
    return_entities: bool,
    dry_run: bool,
):
    """
    Running CLI with manifest migration options.
    """
    if jsonld is None:
        jsonld = CONFIG.model_location
        log_value_from_config("jsonld", jsonld)

    full_scope = project_scope + [archive_project]
    synStore = SynapseStorage(project_scope=full_scope)

    for project in project_scope:
        if not return_entities:
            logging.info("Re-uploading manifests as tables")
            synStore.upload_annotated_project_manifests_to_synapse(
                project, jsonld, dry_run
            )
        if archive_project:
            logging.info("Migrating entitites")
            synStore.move_entities_to_new_project(
                project, archive_project, return_entities, dry_run
            )
    return


@manifest.command(
    "download",
    short_help=query_dict(manifest_commands, ("manifest", "download", "short_help")),
)
@click_log.simple_verbosity_option(logger)
# define the optional arguments
@click.option(
    "-d",
    "--dataset_id",
    help=query_dict(manifest_commands, ("manifest", "download", "dataset_id")),
)
@click.option(
    "-nmn",
    "--new_manifest_name",
    default="",
    help=query_dict(manifest_commands, ("manifest", "download", "new_manifest_name")),
)
@click.pass_obj
def download_manifest(ctx, dataset_id, new_manifest_name):
    master_fileview = CONFIG["synapse"]["master_fileview"]

    # use Synapse Storage
    store = SynapseStorage()

    # download existing file
    manifest_data = store.getDatasetManifest(
        datasetId=dataset_id, downloadFile=True, newManifestName=new_manifest_name
    )

    if not manifest_data:
        logger.error(
            "'Dataset_id provided is not able to return a manifest, please check that the id is the parent folder containing the manifest."
        )
        sys.exit(1)

    # return local file path
    manifest_local_file_path = manifest_data["path"]
    logger.info(
        f"The manifest has been downloaded to the following location: {manifest_local_file_path}"
    )
    return manifest_local_file_path
