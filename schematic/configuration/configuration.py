"""Configuration singleton for the Schematic Package"""

from typing import Optional
import os
import yaml
from .dataclasses import (
    SynapseConfig,
    ManifestConfig,
    ModelConfig,
    GoogleSheetsConfig,
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
        """
        config_path = os.path.expanduser(config_path)
        config_path = os.path.abspath(config_path)
        self.config_path = config_path

        self._parent_directory = os.path.dirname(config_path)

        with open(config_path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
        self._synapse_config = SynapseConfig(
            **data.get("asset_store", {}).get("synapse", {})
        )
        self._manifest_config = ManifestConfig(**data.get("manifest", {}))
        self._model_config = ModelConfig(**data.get("model", {}))
        self._google_sheets_config = GoogleSheetsConfig(**data.get("google_sheets", {}))

    def _normalize_path(self, path: str) -> str:
        """

        Args:
            path (str): The path to normalize

        Returns:
            str: The normalized path
        """
        if not os.path.isabs(path):
            path = os.path.join(self._parent_directory, path)
        return os.path.normpath(path)

    @property
    def synapse_configuration_path(self) -> str:
        """
        Returns:
            str: The path to the synapse configuration file
        """
        return self._normalize_path(self._synapse_config.config_basename)

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
        """Sets the synapse_master_fileview_id

        Args:
            synapse_id (str): The synapse id to set
        """
        self._synapse_config.master_fileview_id = synapse_id

    @property
    def synapse_manifest_folder(self) -> str:
        """
        Returns:
            str:
        """
        return self._synapse_config.manifest_folder

    @property
    def manifest_title(self) -> str:
        """
        Returns:
            str:
        """
        return self._manifest_config.title

    @property
    def manifest_data_type(self) -> list[str]:
        """
        Returns:
            list[str]:
        """
        return self._manifest_config.data_type

    @property
    def model_location(self) -> str:
        """
        Returns:
            str:
        """
        return self._model_config.location

    @property
    def model_file_type(self) -> str:
        """
        Returns:
            str:
        """
        return self._model_config.file_type

    @property
    def service_account_credentials_synapse_id(self) -> str:
        """
        Returns:
            str:
        """
        return self._google_sheets_config.service_acct_creds_synapse_id

    @property
    def service_account_credentials_path(self) -> str:
        """
        Returns:
            str:
        """
        return self._normalize_path(
            self._google_sheets_config.service_acct_creds_basename
        )

    @property
    def google_sheets_master_template_id(self) -> str:
        """
        Returns:
            str:
        """
        return self._google_sheets_config.master_template_id

    @property
    def google_sheets_strict_validation(self) -> bool:
        """
        Returns:
            bool:
        """
        return self._google_sheets_config.strict_validation

    @property
    def google_required_background_color(self) -> dict[str, float]:
        """
        Returns:
            dict[str, float]:
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
            dict[str, float]:
        """
        return {
            "red": 1.0,
            "green": 1.0,
            "blue": 0.9019,
        }

# This instantiates the singleton for the rest of the package
CONFIG = Configuration()
