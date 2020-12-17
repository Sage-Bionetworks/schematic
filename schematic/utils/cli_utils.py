#!/usr/bin/env python3

import inspect
import logging

from typing import Any, Mapping, Sequence, Union
from functools import reduce

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def query_dict(
    dictionary: Mapping[Any, Any], keys: Sequence[Any]
) -> Union[Any, None]:
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


def fill_in_from_config(
    arg_name: str,
    arg_value: Any,
    config: Mapping[Any, Any],
    config_keys: Sequence[Any]
) -> Any:
    """Fill in a missing value from a configuration object.

    Args:
        arg_name: Name of the argument. Used for logging.
        arg_value: Value of the argument provided at the
            command line.
        config: Object that behaves like a dictionary and
            contains configuration values.
        config_keys: List of keys used to access a nested
            value in `config` corresponding to `arg_name`.

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

    # Make sure argument value or config are set
    assert not (arg_value is None and config is None), (
        f"'--{arg_name}' and '--config' are both undefined. "
        "Please provide a value for either one."
    )

    config_value = query_dict(config, config_keys)

    # Make sure argument value and
    config_keys_str = ' > '.join(config_keys)
    assert config_value is not None, (
        "The configuration value corresponding to the argument "
        f"'--{arg_name}' ({config_keys_str}) doesn't exist. "
        "Please provide a value for either the CLI argument or "
        "in the configuration file."
    )

    logger.info(
        f"The '--{arg_name}' argument is being taken from configuration "
        f"file ({config_keys_str}), i.e., '{config_value}'."
    )

    return config_value
