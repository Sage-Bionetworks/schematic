"""Configuration singleton for the Schematic Package"""

from typing import Optional, Any
import os
import yaml
from schematic.utils.general import normalize_path
from .dataclasses import (
    SynapseConfig,
    ManifestConfig,
    ModelConfig,
    GoogleSheetsConfig,
)


class ConfigNonAllowedFieldError(Exception):
    """Raised when a user submitted config file contains non allowed fields"""

    def __init__(
        self, message: str, fields: list[str], allowed_fields: list[str]
    ) -> None:
        """
        Args:
            message (str):  A message describing the error
            fields (list[str]): The fields in the config
            allowed_fields (list[str]): The allowed fields in the config
        """
        self.message = message
        self.fields = fields
        self.allowed_fields = allowed_fields
        super().__init__(self.message)

    def __str__(self) -> str:
        """String representation"""
        return (
            f"{self.message}; "
            f"config contains fields: {self.fields}; "
            f"allowed fields: {self.allowed_fields}"
        )


class Configuration:
    """
    This class is used as a singleton by the rest of the package.
    It is instantiated only once at the bottom of this file, and that
     instance is imported by other modules
    """

    def __init__(self) -> None:
        self.config_path: Optional[str] = None
        self._parent_directory = os.getcwd()
        self._synapse_config = SynapseConfig()
        self._manifest_config = ManifestConfig()
        self._model_config = ModelConfig()
        self._google_sheets_config = GoogleSheetsConfig()

    def load_config(self, config_path: str) -> None:
        """Loads a user created config file and overwrites any defaults  listed in the file

        Args:
            config_path (str): The path to the config file

        Raises:
            ConfigNonAllowedFieldError: If there are non allowed fields in the config file
        """
        allowed_config_fields = {"asset_store", "manifest", "model", "google_sheets"}
        config_path = os.path.expanduser(config_path)
        config_path = os.path.abspath(config_path)
        self.config_path = config_path

        self._parent_directory = os.path.dirname(config_path)

        with open(config_path, "r", encoding="utf-8") as file:
            config: dict[str, Any] = yaml.safe_load(file)
        if not set(config.keys()).issubset(allowed_config_fields):
            raise ConfigNonAllowedFieldError(
                "Non allowed fields in top level of configuration file.",
                list(config.keys()),
                list(allowed_config_fields),
            )

        self._manifest_config = ManifestConfig(**config.get("manifest", {}))
        self._model_config = ModelConfig(**config.get("model", {}))
        self._google_sheets_config = GoogleSheetsConfig(
            **config.get("google_sheets", {})
        )
        asset_store_config = config.get("asset_store", None)
        if asset_store_config:
            self._set_asset_store(asset_store_config)

    def _set_asset_store(self, config: dict[str, Any]) -> None:
        allowed_config_fields = {"synapse"}
        all_fields_are_valid = set(config.keys()).issubset(allowed_config_fields)
        if not all_fields_are_valid:
            raise ConfigNonAllowedFieldError(
                "Non allowed fields in asset_store of configuration file.",
                list(config.keys()),
                list(allowed_config_fields),
            )
        self._synapse_config = SynapseConfig(**config["synapse"])

    @property
    def synapse_configuration_path(self) -> str:
        """
        Returns:
            str: The path to the synapse configuration file
        """
        return normalize_path(self._synapse_config.config, self._parent_directory)

    @property
    def synapse_manifest_basename(self) -> str:
        """
        Returns:
            str:
        """
        return self._synapse_config.manifest_basename

    @property
    def synapse_master_fileview_id(self) -> str:
        """
        Returns:
            str:
        """
        return self._synapse_config.master_fileview_id

    @synapse_master_fileview_id.setter
    def synapse_master_fileview_id(self, synapse_id: str) -> None:
        """Sets the Synapse master fileview ID

        Args:
            synapse_id (str): The synapse id to set
        """
        self._synapse_config.master_fileview_id = synapse_id

    @property
    def manifest_folder(self) -> str:
        """
        Returns:
            str:  Location where manifests will saved to
        """
        return self._manifest_config.manifest_folder

    @property
    def manifest_title(self) -> str:
        """
        Returns:
            str: Title or title prefix given to generated manifest(s)
        """
        return self._manifest_config.title

    @property
    def manifest_data_type(self) -> list[str]:
        """
        Returns:
            list[str]: Data types of manifests to be generated or data type (singular) to validate
              manifest against
        """
        return self._manifest_config.data_type

    @property
    def model_location(self) -> str:
        """
        Returns:
            str: The path to the model.jsonld
        """
        return self._model_config.location

    @property
    def service_account_credentials_path(self) -> str:
        """
        Returns:
            str: The path of the Google service account credentials.
        """
        return normalize_path(
            self._google_sheets_config.service_acct_creds, self._parent_directory
        )

    @service_account_credentials_path.setter
    def service_account_credentials_path(self, path: str) -> None:
        """Sets the path of the Google service account credentials.

        Args:
            path (str): The path of the Google service account credentials.
        """
        self._google_sheets_config.service_acct_creds = path

    @property
    def google_sheets_master_template_id(self) -> str:
        """
        Returns:
            str: The template id of the google sheet.
        """
        return "1LYS5qE4nV9jzcYw5sXwCza25slDfRA1CIg3cs-hCdpU"

    @property
    def google_sheets_strict_validation(self) -> bool:
        """
        Returns:
            bool: Weather or not to disallow bad values in the google sheet
        """
        return self._google_sheets_config.strict_validation

    @property
    def google_required_background_color(self) -> dict[str, float]:
        """
        Returns:
            dict[str, float]: Background color for google sheet
        """
        return {
            "red": 0.9215,
            "green": 0.9725,
            "blue": 0.9803,
        }

    @property
    def google_optional_background_color(self) -> dict[str, float]:
        """
        Returns:
            dict[str, float]: Background color for google sheet
        """
        return {
            "red": 1.0,
            "green": 1.0,
            "blue": 0.9019,
        }


# This instantiates the singleton for the rest of the package
CONFIG = Configuration()
