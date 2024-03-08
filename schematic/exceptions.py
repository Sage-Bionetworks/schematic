"""Schematic Exceptions"""
from typing import Optional, Any, Sequence


class MissingConfigValueError(Exception):
    """Exception raised when configuration value not provided in config file.

    Args:
        config_keys: tuple of keys as present in config file.
        message: custom/pre-defined error message to be returned.

    Returns:
        message.
    """

    def __init__(
        self, config_keys: Sequence[Any], message: Optional[str] = None
    ) -> None:
        config_keys_str = " > ".join(config_keys)
        self.message = (
            "The configuration value corresponding to the argument "
            f"({config_keys_str}) doesn't exist. "
            "Please provide a value in the configuration file."
        )

        if message:
            self.message = message

        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}"


class WrongEntityTypeError(Exception):
    """Exception raised when the entity type is not desired

    Args:
        entity id: For synapse, thi
        message: custom/pre-defined error message to be returned.

    Returns:
        message.
    """

    def __init__(self, syn_id: str, message: Optional[str] = None) -> None:
        self.message = (
            f"'{syn_id}'' is not a desired entity type"
            "Please ensure that you put in the right syn_id"
        )

        if message:
            self.message = message

        super().__init__(self.message)

    def __str__(self) -> str:
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

    def __init__(
        self, arg_name: str, config_keys: Sequence[Any], message: Optional[str] = None
    ) -> None:
        config_keys_str = " > ".join(config_keys)
        self.message = (
            f"The value corresponding to the CLI argument '--{arg_name}'"
            " doesn't exist. "
            "Please provide a value for either the CLI argument or "
            f"({config_keys_str}) in the configuration file."
        )

        if message:
            self.message = message

        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}"


class AccessCredentialsError(Exception):
    """Exception raised when provided access credentials cannot be resolved.

    Args:
        project: Platform/project (e.g., synID of a project)
        message: custom/pre-defined error message to be returned.

    Returns:
        message.
    """

    def __init__(self, project: str, message: Optional[str] = None) -> None:
        self.message = (
            f"Your access to '{project}'' could not be resolved. "
            "Please check your credentials and try again."
        )

        if message:
            self.message = message

        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}"
