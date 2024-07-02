"""Pydantic dataclasses"""

import re
from dataclasses import field
from pydantic.dataclasses import dataclass
from pydantic import validator, ConfigDict, Extra

# This turns on validation for value assignments after creation
pydantic_config = ConfigDict(validate_assignment=True, extra=Extra.forbid)


@dataclass(config=pydantic_config)
class SynapseConfig:
    """
    config_basename: Path to the synapse config file, either absolute or relative to this file
    manifest_basename: the name of downloaded manifest files
    master_fileview_id: Synapse ID of the file view listing all project data assets.
    """

    config: str = ".synapseConfig"
    manifest_basename: str = "synapse_storage_manifest"
    master_fileview_id: str = "syn23643253"

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

    @validator("config", "manifest_basename")
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
        if not value:
            raise ValueError(f"{value} is an empty string")
        return value


@dataclass(config=pydantic_config)
class ManifestConfig:
    """
    manifest_folder: name of the folder manifests will be saved to locally
    title: Title or title prefix given to generated manifest(s)
    data_type: Data types of manifests to be generated or data type (singular) to validate
     manifest against
    """

    manifest_folder: str = "manifests"
    title: str = "example"
    data_type: list[str] = field(default_factory=lambda: ["Biospecimen", "Patient"])

    @validator("title", "manifest_folder")
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
        if not value:
            raise ValueError(f"{value} is an empty string")
        return value


@dataclass(config=pydantic_config)
class ModelConfig:
    """
    location: location of the schema jsonld
    """

    location: str = "tests/data/example.model.jsonld"

    @validator("location")
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
        if not value:
            raise ValueError(f"{value} is an empty string")
        return value


@dataclass(config=pydantic_config)
class GoogleSheetsConfig:
    """
    master_template_id: The template id of the google sheet.
    strict_validation: When doing google sheet validation (regex match) with the validation rules.
      True is alerting the user and not allowing entry of bad values.
      False is warning but allowing the entry on to the sheet.
    service_acct_creds: Path to the Google service account credentials,
     either absolute or relative to this file
    """

    service_acct_creds: str = "schematic_service_account_creds.json"
    strict_validation: bool = True

    @validator("service_acct_creds")
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
        if not value:
            raise ValueError(f"{value} is an empty string")
        return value

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
