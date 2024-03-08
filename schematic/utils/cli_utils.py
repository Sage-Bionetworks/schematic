"""CLI utils"""

# pylint: disable=logging-fstring-interpolation
# pylint: disable=anomalous-backslash-in-string

import logging

from typing import Any, Mapping, Sequence, Union, Optional
from functools import reduce
import re

logger = logging.getLogger(__name__)


def query_dict(dictionary: Mapping[Any, Any], keys: Sequence[Any]) -> Union[Any, None]:
    """Access a nested value in a dictionary corresponding
    to a series of keys.

    Args:
        dictionary: A dictionary containing anything.
        keys: A sequence of values corresponding to keys
            in `dictionary`

    Returns:
        The nested value corresponding to the given series
        of keys, or `None` is such a value doesn't exist.
    """

    def extract(dictionary: Any, key: Any) -> Union[Any, None]:
        """Get value associated with key, defaulting to None."""
        if dictionary is None or not isinstance(dictionary, dict):
            return None
        return dictionary.get(key)

    return reduce(extract, keys, dictionary)  # type: ignore


def log_value_from_config(arg_name: str, config_value: Any) -> None:
    """Logs when getting a value from the config

    Args:
        arg_name (str): Name of the argument. Used for logging.
        config_value (Any): The value in the config
    """
    logger.info(
        f"The {arg_name} argument is being taken from configuration file, i.e., {config_value}."
    )


def parse_syn_ids(
    ctx: Any,  # pylint: disable=unused-argument
    param: str,  # pylint: disable=unused-argument
    syn_ids: str,
) -> Optional[list[str]]:
    """Parse and validate a comma separated string of synapse ids

    Args:
        ctx (Any): click option context
        param (str): click option argument name
        syn_ids (str): comma separated string of synapse ids

    Raises:
        ValueError:  If the entire string does not match a regex for
            a valid comma separated string of SynIDs

    Returns:
        Optional[list[str]]:  List of synapse ids
    """
    if not syn_ids:
        return None

    project_regex = re.compile("(syn\d+\,?)+")
    valid = project_regex.fullmatch(syn_ids)

    if not valid:
        raise ValueError(
            f"The provided list of project synID(s): {syn_ids}, is not formatted correctly. "
            "\nPlease check your list of projects for errors."
        )

    return syn_ids.split(",")


def parse_comma_str_to_list(
    ctx: Any,  # pylint: disable=unused-argument
    param: str,  # pylint: disable=unused-argument
    comma_string: str,
) -> Optional[list[str]]:
    """Separates a comma separated sting into a list of strings

    Args:
        ctx (Any): click option context
        param (str): click option argument name
        comma_string (str): comma separated string

    Returns:
        Optional[list[str]]: _description_
    """
    if not comma_string:
        return None

    return comma_string.split(",")
