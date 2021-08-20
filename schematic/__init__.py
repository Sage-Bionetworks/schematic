import sys
import logging

import click
import click_log

from schematic.configuration import CONFIG
from schematic.loader import LOADER
from schematic.utils.google_api_utils import download_creds_file, generate_token
from schematic.utils.cli_utils import query_dict
from schematic.help import init_command

logging.basicConfig(
    format=("%(levelname)s: [%(asctime)s] %(name)s" " - %(message)s"),
    level=logging.WARNING,
    datefmt="%Y-%m-%d %H:%M:%S",
)


# Suppress INFO-level logging from some dependencies
logging.getLogger("keyring").setLevel(logging.ERROR)
logging.getLogger("rdflib").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command("init", short_help=query_dict(init_command, ("init", "short_help")))
@click_log.simple_verbosity_option(logger)
@click.option(
    "-a",
    "--auth",
    default="token",
    type=click.Choice(["token", "service_account"], case_sensitive=False),
    help=query_dict(init_command, ("init", "auth")),
)
@click.option(
    "-c", "--config", required=True, help=query_dict(init_command, ("init", "config"))
)
def init(auth, config):
    """Initialize mode of authentication for schematic."""
    try:
        logger.debug(f"Loading config file contents in '{config}'")
        obj = CONFIG.load_config(config)
    except ValueError as e:
        logger.error("'--config' not provided or environment variable not set.")
        logger.exception(e)
        sys.exit(1)

    # download crdentials file based on selected mode of authentication
    download_creds_file(auth)

    # if authentication method is token-based
    # then create 'token.pickle' file from 'credentials.json' file
    if auth == "token":
        generate_token()
