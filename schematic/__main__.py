#!/usr/bin/env python

import logging
import click

from schematic.manifest.commands import manifest as manifest_cli    # get manifest commands

logger = logging.getLogger()
logger.info("Starting `schematic`")

# dict() -> new empty dictionary
CONTEXT_SETTINGS = dict(help_option_names=['--help', '-h'])  # help options

# invoke_without_command=True -> forces the application not to show aids before losing them with a --h
@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
@click.option('--quiet', 'verbosity', flag_value='quiet',
              help=("Only display printed outputs in the console - "
                    "i.e., no log messages."))
@click.option('--debug', 'verbosity', flag_value='debug',
              help="Include all debug log messages in the console.")
def main(verbosity):
    """
    Command line interface for the `schematic` library.
    """
    if verbosity == 'quiet':
        logger.setLevel(logging.ERROR)
    elif verbosity == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


main.add_command(manifest_cli) # add manifest commands


if __name__ == '__main__':
    main()
