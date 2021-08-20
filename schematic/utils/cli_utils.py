#!/usr/bin/env python3

import inspect
import logging

from typing import Any, Mapping, Sequence, Union
from functools import reduce

from schematic import CONFIG
from schematic.exceptions import (
    MissingConfigValueError,
    MissingConfigAndArgumentValueError,
)

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

    return reduce(extract, keys, dictionary)


def get_from_config(
    dictionary: Mapping[Any, Any], keys: Sequence[Any]
) -> Union[Any, None]:
    """Access a nested configuration value from a yaml
    configuration file.

    Args:
        dictionary: A dictionary containing anything.
        keys: A sequence of values corresponding to keys
            in `dictionary`.

    Returns:
        The nested value corresponding to the given series.

    Raises:
        MissingConfigValueError: When configuration value not
            found in config.yml file for given key.
    """
    # get configuration value from config file
    config_value = query_dict(dictionary, keys)

    # if configuration value not present then raise Exception
    if config_value is None:
        raise MissingConfigValueError(keys)

    config_keys_str = " > ".join(keys)

    logger.info(
        f"The ({config_keys_str}) argument with value "
        f"'{config_value}' is being read from the config file."
    )

    return config_value


def fill_in_from_config(
    arg_name: str, arg_value: Any, config_keys: Sequence[Any], allow_none: bool = False
) -> Any:
    """Fill in a missing value from a configuration object.

    Args:
        arg_name: Name of the argument. Used for logging.
        config_keys: List of keys used to access a nested
            value in `config` corresponding to `arg_name`.
        arg_value: Value of the argument provided at the
            command line.
        allow_none: Return None if argument value and
            configuration value are both None (rather
            than raising an error).

    Returns:
        The argument value, either from the calling context
        or the corresponding field in the configuration.

    Raises:
        AssertionError: If both the argument value and the
            configuration object are `None`.
    """

    # Avoid accessing config if argument value is provided
    if arg_value is not None:
        return arg_value

    # raise Exception if both, configuration value not present
    # in config file and CLI argument value is missing
    try:
        config_value = get_from_config(CONFIG.DATA, config_keys)
    except MissingConfigValueError:
        if allow_none:
            return None
        raise MissingConfigAndArgumentValueError(arg_name, config_keys)

    # Make sure argument value and
    config_keys_str = " > ".join(config_keys)

    logger.info(
        f"The '--{arg_name}' argument is being taken from configuration "
        f"file ({config_keys_str}), i.e., '{config_value}'."
    )

    return config_value
