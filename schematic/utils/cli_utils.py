#!/usr/bin/env python3

import inspect
import logging

from typing import Any, Mapping, Sequence, Union, List
from functools import reduce
import re

logger = logging.getLogger(__name__)

# We are using fstrings in logger methods
# pylint: disable=logging-fstring-interpolation


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

    return reduce(extract, keys, dictionary)


def log_value_from_config(arg_name: str, config_value: Any):
    """Logs when getting a value from the config

    Args:
        arg_name (str): Name of the argument. Used for logging.
        config_value (Any): The value in the config
    """
    logger.info(
        f"The {arg_name} argument is being taken from configuration file, i.e., {config_value}."
    )

def parse_synIDs(
    ctx, param, synIDs,
) -> List[str]:
    """Parse and validate a comma separated string of synIDs

    Args:
        ctx:
            click option context
        param:
            click option argument name
        synIDs:
            comma separated string of synIDs

    Returns:
        List of synID strings

    Raises:
        ValueError: If the entire string does not match a regex for 
            a valid comma separated string of SynIDs
    """
    if synIDs:
        project_regex = re.compile("(syn\d+\,?)+")
        valid=project_regex.fullmatch(synIDs)

        if valid:
            synIDs = synIDs.split(",")

            return synIDs

        else:
            raise ValueError(
                        f"The provided list of project synID(s): {synIDs}, is not formatted correctly. "
                        "\nPlease check your list of projects for errors."
                    )
    else:
        return

def parse_comma_str_to_list(
    ctx, param, comma_string,
) -> List[str]:

    if comma_string:
        return comma_string.split(",")
    else:
        return None