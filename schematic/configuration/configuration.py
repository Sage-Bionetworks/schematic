"""Configuration singleton for the Schematic Package"""

from typing import Any, Optional
import os
import logging
import yaml
from .dataclasses import (
    FileNameConfig,
    SynapseConfig,
    ManifestConfig,
    ModelConfig,
    GoogleConfig,
)

# Create a logger for the configuration class
logger = logging.getLogger(__name__)


class Configuration:
    """
    This class is used as a singleton by the rest of the package.
    It is instantiated only once at the bottom of this file, and that
     instance is imported by other modules
    """

    def __init__(self) -> None:
        self.config_path: Optional[str] = None
        self._file_name_config = FileNameConfig()
        self._synapse_config = SynapseConfig()
        self._manifest_config = ManifestConfig()
        self._model_config = ModelConfig()
        self._google_config = GoogleConfig()

    def load_config(self, config_path: str) -> None:
        """Loads a user created config file and overwrites any defaults  listed in the file

        Args:
            config_path (str): The path to the config file
        """
        config_path = os.path.expanduser(config_path)
        config_path = os.path.abspath(config_path)
        self.config_path = config_path
        with open(config_path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
        self._file_name_config = FileNameConfig(**data.get("definitions", {}))
        self._synapse_config = SynapseConfig(**data.get("synapse", {}))
        self._manifest_config = ManifestConfig(**data.get("manifest", {}))
        self._model_config = ModelConfig(**data.get("model", {}))
        self._google_config = GoogleConfig(**data.get("google", {}))

    def _normalize_path(self, path: str) -> str:
        """

        Args:
            path (str): The path to normalize

        Returns:
            str: The normalized path
        """

        if self.config_path:
            # Retrieve parent directory of the config to decode relative paths
            parent_dir = os.path.dirname(self.config_path)
        else:
            # assume the parent dir would be the current work dir
            parent_dir = os.getcwd()

        # Ensure absolute file paths
        if not os.path.isabs(path):
            path = os.path.join(parent_dir, path)
        # And lastly, normalize file paths
        return os.path.normpath(path)

    def _log_config_value_access(
        self, value_name: str, config_value: Any
    ) -> None:
        """Logs when a configuration value is being accessed

        Args:
            value_name (str): The name of the value to log
            config_value (Any): The value from the configuration
        """
        logger.info(
            "The '%s' value is being taken from the user specified configuration file: '%s'.",
            value_name,
            config_value,
        )

    @property
    def service_account_credentials_path(self) -> str:
        """
        Returns:
            str:
        """
        value = self._normalize_path(self._file_name_config.service_acct_creds)
        self._log_config_value_access("service_account_credentials_path", value)
        return value

    @property
    def synapse_configuration_path(self) -> str:
        """
        Returns:
            str: The path to the synapse configuration file
        """
        value = self._normalize_path(self._file_name_config.synapse_config)
        self._log_config_value_access("synapse_configuration_path", value)
        return value

    @property
    def google_required_background_color(self) -> dict[str, float]:
        """
        Returns:
            dict[str, float]:
        """
        value = {
            "red": 0.9215,
            "green": 0.9725,
            "blue": 0.9803,
        }
        self._log_config_value_access("google_required_background_color", value)
        return value

    @property
    def google_optional_background_color(self) -> dict[str, float]:
        """
        Returns:
            dict[str, float]:
        """
        value = {
            "red": 1.0,
            "green": 1.0,
            "blue": 0.9019,
        }
        self._log_config_value_access("google_required_background_color", value)
        return value

    @property
    def synapse_master_file_view_id(self) -> str:
        """
        Returns:
            str: The Synapse ID of the master file view
        """
        value = self._synapse_config.master_fileview
        self._log_config_value_access("synapse_master_fileview", value)
        return value

    @property
    def synapse_service_account_credentials_id(self) -> str:
        """
        Returns:
            str:
        """
        value = self._synapse_config.service_acct_creds
        self._log_config_value_access("synapse_service_account_credentials_id", value)
        return value
