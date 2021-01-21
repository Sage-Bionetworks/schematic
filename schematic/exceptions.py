from typing import Any, Sequence

class MissingConfigValueError(Exception):
    """Exception raised when configuration value not provided in config file.

    Args:
        config_keys: tuple of keys as present in config file.
        message: custom/pre-defined error message to be returned.

    Returns:
        message.
    """
    def __init__(self, config_keys: Sequence[Any], message: str = None) -> str:
        if message is not None:
            self.message = message
        else:
            config_keys_str = ' > '.join(config_keys)
            self.message = (
                "The configuration value corresponding to the argument "
                f"({config_keys_str}) doesn't exist. "
                "Please provide a value in the configuration file."
            )
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message}"


class MissingConfigAndArgumentValueError(Exception):
    """Exception raised when configuration value not provided in config file.

    Args:
        arg_name: CLI argument name.
        config_keys: tuple of keys as present in config file.
        message: custom/pre-defined error message to be returned.

    Returns:
        message.
    """
    def __init__(self, arg_name: str, 
                config_keys: Sequence[Any], message: str = None) -> str:
        if message is not None:
            self.message = message
        else:
            config_keys_str = ' > '.join(config_keys)
            self.message = f"The value corresponding to the CLI argument '--{arg_name}'" + \
                           " doesn't exist. " \
                           "Please provide a value for either the CLI argument or " \
                           f"({config_keys_str}) in the configuration file."
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message}"
