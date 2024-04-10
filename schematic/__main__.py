#!/usr/bin/env python
import logging
import click
import click_log

from schematic.manifest.commands import (
    manifest as manifest_cli,
)  # get manifest commands
from schematic.models.commands import model as model_cli  # submit manifest commands
from schematic.schemas.commands import (
    schema as schema_cli,
)  # schema conversion commands
from schematic.visualization.commands import (
    viz as viz_cli,
)  # viz generation commands

logger = logging.getLogger()
click_log.basic_config(logger)

# dict() -> new empty dictionary
CONTEXT_SETTINGS = dict(help_option_names=["--help", "-h"])  # help options


# invoke_without_command=True -> forces the application not to show aids before losing them with a --h
@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
@click_log.simple_verbosity_option(logger)
def main():
    """
    Command line interface to the `schematic` backend services.
    """
    logger.info("Starting schematic...")
    logger.debug("Existing sub-commands need to be used with schematic.")


main.add_command(manifest_cli)  # add manifest commands
main.add_command(model_cli)  # add model commands
main.add_command(schema_cli)  # add schema commands
main.add_command(viz_cli)  # add viz commands


if __name__ == "__main__":
    main()
