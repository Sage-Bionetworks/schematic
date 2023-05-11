"""Pydantic dataclasses"""

import re
from pydantic.dataclasses import dataclass
from pydantic import validator


@dataclass()
class FileNameConfig:
    """
    synapse_config: the name of the synapse config file
    service_acct_creds:
    """

    synapse_config: str = ".synapseConfig"
    service_acct_creds: str = "schematic_service_account_creds.json"

    @validator("synapse_config", "service_acct_creds")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string  is not empty(has at least one char)

        Args:
            value (str): A string

        Raises:
            ValueError: If the value is zero characters long

        Returns:
            (str): The input value
        """
        if len(value) == 0:
            raise ValueError(f"{value} is an empty string")
        return value


@dataclass()
class SynapseConfig:
    """
    master_fileview: Synapse id for the master file view
    service_acct_creds:
    manifest_folder: name of the folder manifests will be saved to locally
    manifest_basename: the name of downloaded manifest files
    """

    master_fileview: str = "syn23643253"
    service_acct_creds: str = "syn25171627"
    manifest_folder: str = "manifests"
    manifest_basename: str = "synapse_storage_manifest"

    @validator("master_fileview", "service_acct_creds")
    @classmethod
    def validate_synapse_id(cls, value: str) -> str:
        """Check if string is a valid synapse id

        Args:
            value (str): A string

        Raises:
            ValueError: If the value isn't a valid Synapse id

        Returns:
            (str): The input value
        """
        if not re.search("^syn[0-9]+", value):
            raise ValueError(f"{value} is not a valid Synapse id")
        return value

    @validator("manifest_folder", "manifest_basename")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string  is not empty(has at least one char)

        Args:
            value (str): A string

        Raises:
            ValueError: If the value is zero characters long

        Returns:
            (str): The input value
        """
        if len(value) == 0:
            raise ValueError(f"{value} is an empty string")
        return value


@dataclass()
class ManifestConfig:
    """
    title:
    data_type:
    """

    title: str = "example"
    data_type: list[str] = ["Biospecimen", "Patient"]

    @validator("title")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string  is not empty(has at least one char)

        Args:
            value (str): A string

        Raises:
            ValueError: If the value is zero characters long

        Returns:
            (str): The input value
        """
        if len(value) == 0:
            raise ValueError(f"{value} is an empty string")
        return value


@dataclass()
class ModelConfig:
    """
    location: location of the schema jsonld, either a path, ro url
    file_type: one of ["local"]
    """

    location: str = "tests/data/example.model.jsonld"
    file_type: str = "local"

    @validator("title", "file_type")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string  is not empty(has at least one char)

        Args:
            value (str): A string

        Raises:
            ValueError: If the value is zero characters long

        Returns:
            (str): The input value
        """
        if len(value) == 0:
            raise ValueError(f"{value} is an empty string")
        return value


@dataclass()
class GoogleConfig:
    """
    master_template_id:
    strict_validation:
    """

    master_template_id: str = "1LYS5qE4nV9jzcYw5sXwCza25slDfRA1CIg3cs-hCdpU"
    strict_validation: bool = True

    @validator("master_template_id")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string  is not empty(has at least one char)

        Args:
            value (str): A string

        Raises:
            ValueError: If the value is zero characters long

        Returns:
            (str): The input value
        """
        if len(value) == 0:
            raise ValueError(f"{value} is an empty string")
        return value
