import os
import logging
from pathlib import Path
import click
import click_log
import logging
import sys
from typing import List

from schematic.manifest.generator import ManifestGenerator
from schematic.utils.cli_utils import fill_in_from_config, query_dict, parse_synIDs
from schematic.help import manifest_commands
from schematic import CONFIG
from schematic.schemas.generator import SchemaGenerator
from schematic.utils.google_api_utils import export_manifest_csv, export_manifest_excel, export_manifest_drive_service
from schematic.store.synapse import SynapseStorage

logger = logging.getLogger('schematic')
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
    default = 'ascending',
    help=query_dict(manifest_commands, ("manifest", "get", "alphabetize_valid_values")),
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
    json_schema,
    output_xlsx,
    alphabetize_valid_values,
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
    def create_single_manifest(data_type, output_csv=None, output_xlsx=None):
        # create object of type ManifestGenerator
        manifest_generator = ManifestGenerator(
            path_to_json_ld=jsonld,
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
            dataset_id=dataset_id, sheet_url=sheet_url, json_schema=json_schema, output_format = output_format, output_path = output_path
        )

        if sheet_url:
            logger.info("Find the manifest template using this Google Sheet URL:")
            click.echo(result)
        if output_csv is None and output_xlsx is None: 
            prefix, _ = os.path.splitext(jsonld)
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

    if data_type[0] == 'all manifests':
        sg = SchemaGenerator(path_to_json_ld=jsonld)
        component_digraph = sg.se.get_digraph_by_edge_type('requiresComponent')
        components = component_digraph.nodes()
        for component in components:
            t = f'{title}.{component}.manifest'
            result = create_single_manifest(data_type = component)
    else:
        for dt in data_type:
            if len(data_type) > 1 and not output_xlsx:
                t = f'{title}.{dt}.manifest'
            elif output_xlsx: 
                if ".xlsx" or ".xls" in output_xlsx:
                    title_with_extension = os.path.basename(output_xlsx)
                    t = title_with_extension.split('.')[0]
            else:
                t = title
            result = create_single_manifest(data_type = dt, output_csv=output_csv, output_xlsx=output_xlsx)

    return result

@manifest.command(
    "migrate", short_help=query_dict(manifest_commands, ("manifest", "migrate", "short_help"))
)
@click_log.simple_verbosity_option(logger)
# define the optional arguments
@click.option(
    "-ps",
    "--project_scope",
    default=None,
    callback=parse_synIDs,
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
    jsonld = fill_in_from_config("jsonld", jsonld, ("model", "input", "location"))

    
    full_scope = project_scope + [archive_project]
    synStore = SynapseStorage(project_scope = full_scope)  

    for project in project_scope:
        if not return_entities:
            logging.info("Re-uploading manifests as tables")
            synStore.upload_annotated_project_manifests_to_synapse(project, jsonld, dry_run)
        if archive_project:
            logging.info("Migrating entitites")
            synStore.move_entities_to_new_project(project, archive_project, return_entities, dry_run)
        
    return 
