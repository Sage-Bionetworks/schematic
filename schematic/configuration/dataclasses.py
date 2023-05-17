"""Pydantic dataclasses"""

import re
from typing import Optional
from dataclasses import field
from pydantic.dataclasses import dataclass
from pydantic import validator, ConfigDict

# This turns on validation for value assignments after creation
pydantic_config = ConfigDict(validate_assignment=True)


@dataclass(config=pydantic_config)
class SynapseConfig:
    """
    config_basename: the basename of the synapse config file
    manifest_basename: the name of downloaded manifest files
    master_fileview_id: Synapse id for the master file view
    manifest_folder: name of the folder manifests will be saved to locally
    """

    validate_assignment = True
    config_basename: str = ".synapseConfig"
    manifest_basename: str = "synapse_storage_manifest"
    master_fileview_id: str = "syn23643253"
    manifest_folder: str = "manifests"

    @validator("master_fileview_id")
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

    @validator("config_basename", "manifest_basename", "manifest_folder")
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


@dataclass(config=pydantic_config)
class ManifestConfig:
    """
    title:
    data_type:
    """

    title: str = "example"
    data_type: list[str] = field(default_factory=lambda: ["Biospecimen", "Patient"])

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


@dataclass(config=pydantic_config)
class ModelConfig:
    """
    location: location of the schema jsonld, either a path, ro url
    file_type: one of ["local"]
    """

    location: str = "tests/data/example.model.jsonld"
    file_type: str = "local"

    @validator("location", "file_type")
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


@dataclass(config=pydantic_config)
class GoogleSheetsConfig:
    """
    master_template_id:
    strict_validation:
    service_acct_creds_synapse_id:
    service_acct_creds_basename:
    """

    service_acct_creds_synapse_id: str = "syn25171627"
    service_acct_creds_basename: str = "schematic_service_account_creds.json"
    master_template_id: Optional[str] = None
    strict_validation: bool = True

    @validator("service_acct_creds_basename")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string is not empty(has at least one char)

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

    @validator("master_template_id")
    @classmethod
    def validate_optional_string_is_not_empty(cls, value: str) -> str:
        """Check if string is not empty(has at least one char)

        Args:
            value (Optional[str]): A string

        Raises:
            ValueError: If the value is zero characters long

        Returns:
            (str): The input value
        """
        if value is not None and len(value) == 0:
            raise ValueError(f"{value} is an empty string")
        return value

    @validator("service_acct_creds_synapse_id")
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
