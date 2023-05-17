"""Testing for Configuration module"""

import os
import pytest
from pydantic import ValidationError

from schematic.configuration.dataclasses import (
    SynapseConfig,
    ManifestConfig,
    ModelConfig,
    GoogleSheetsConfig,
)
from schematic.configuration.configuration import Configuration


class TestDataclasses:
    """Testing for pydantic dataclasses"""

    def test_synapse_config(self) -> None:
        """Testing for SynapseConfig"""
        assert isinstance(SynapseConfig(), SynapseConfig)
        assert isinstance(
            SynapseConfig(
                config_basename="file_name",
                manifest_basename="file_name",
                master_fileview_id="syn1",
                manifest_folder="folder_name",
            ),
            SynapseConfig,
        )

    with pytest.raises(ValidationError):
        SynapseConfig(
            config_basename=None,
            manifest_basename="file_name",
            master_fileview_id="syn1",
            manifest_folder="folder_name",
        )

    with pytest.raises(ValidationError):
        SynapseConfig(
            config_basename="file_name",
            manifest_basename="file_name",
            master_fileview_id="syn",
            manifest_folder="folder_name",
        )

    with pytest.raises(ValidationError):
        SynapseConfig(
            config_basename="",
            manifest_basename="file_name",
            master_fileview_id="syn",
            manifest_folder="folder_name",
        )

    def test_manifest_config(self) -> None:
        """Testing for ManifestConfig"""
        assert isinstance(ManifestConfig(), ManifestConfig)
        assert isinstance(
            ManifestConfig(title="title", data_type=[]),
            ManifestConfig,
        )
        with pytest.raises(ValidationError):
            ManifestConfig(title="title", data_type="type")
        with pytest.raises(ValidationError):
            ManifestConfig(title="", data_type="type")

    def test_model_config(self) -> None:
        """Testing for ModelConfig"""
        assert isinstance(ModelConfig(), ModelConfig)
        assert isinstance(
            ModelConfig(location="url", file_type="local"),
            ModelConfig,
        )
        with pytest.raises(ValidationError):
            ModelConfig(location="", file_type="local")

    def test_google_sheets_config(self) -> None:
        """Testing for ModelConfig"""
        assert isinstance(GoogleSheetsConfig(), GoogleSheetsConfig)
        assert isinstance(
            GoogleSheetsConfig(
                service_acct_creds_basename="file_name",
                service_acct_creds_synapse_id="syn1",
                master_template_id="id",
                strict_validation=True,
            ),
            GoogleSheetsConfig,
        )
        with pytest.raises(ValidationError):
            GoogleSheetsConfig(
                service_acct_creds_basename="file_name",
                service_acct_creds_synapse_id="syn1",
                master_template_id="id",
                strict_validation="tru",
            )
        with pytest.raises(ValidationError):
            GoogleSheetsConfig(
                service_acct_creds_basename="",
                service_acct_creds_synapse_id="syn1",
                master_template_id="id",
                strict_validation=True,
            )
        with pytest.raises(ValidationError):
            GoogleSheetsConfig(
                service_acct_creds_basename="file_name",
                service_acct_creds_synapse_id="syn",
                master_template_id="id",
                strict_validation=True,
            )


class TestConfiguration:
    """Testing Configuration class"""

    def test_init(self) -> None:
        """Testing for Configuration.__init__"""
        config = Configuration()
        assert config.config_path is None
        assert config.synapse_configuration_path != ".synapseConfig"
        assert os.path.basename(config.synapse_configuration_path) == ".synapseConfig"
        assert config.synapse_manifest_basename == "synapse_storage_manifest"
        assert config.synapse_master_fileview_id == "syn23643253"
        assert config.synapse_manifest_folder == "manifests"
        assert config.manifest_title == "example"
        assert config.manifest_data_type == ["Biospecimen", "Patient"]
        assert config.model_location == "tests/data/example.model.jsonld"
        assert config.model_file_type == "local"
        assert config.service_account_credentials_synapse_id
        assert (
            config.service_account_credentials_path
            != "schematic_service_account_creds.json"
        )
        assert (
            os.path.basename(config.service_account_credentials_path)
            == "schematic_service_account_creds.json"
        )
        assert config.google_sheets_master_template_id == "1LYS5qE4nV9jzcYw5sXwCza25slDfRA1CIg3cs-hCdpU"
        assert config.google_sheets_strict_validation
        assert config.google_required_background_color == {
            "red": 0.9215,
            "green": 0.9725,
            "blue": 0.9803,
        }
        assert config.google_optional_background_color == {
            "red": 1.0,
            "green": 1.0,
            "blue": 0.9019,
        }

    def test_load_config(self) -> None:
        """Testing for Configuration.load_config"""
        config = Configuration()

        config.load_config("tests/data/test_config.yml")
        assert os.path.basename(config.config_path) == "test_config.yml"
        assert config.synapse_configuration_path != ".synapseConfig"
        assert os.path.basename(config.synapse_configuration_path) == ".synapseConfig"
        assert config.synapse_manifest_basename == "synapse_storage_manifest"
        assert config.synapse_master_fileview_id == "syn23643253"
        assert config.synapse_manifest_folder == "manifests"
        assert config.manifest_title == "example"
        assert config.manifest_data_type == ["Biospecimen", "Patient"]
        assert config.model_location == "tests/data/example.model.jsonld"
        assert config.model_file_type == "local"
        assert config.service_account_credentials_synapse_id
        assert (
            config.service_account_credentials_path
            != "schematic_service_account_creds.json"
        )
        assert (
            os.path.basename(config.service_account_credentials_path)
            == "schematic_service_account_creds.json"
        )
        assert config.google_sheets_master_template_id  == "1LYS5qE4nV9jzcYw5sXwCza25slDfRA1CIg3cs-hCdpU"
        assert config.google_sheets_strict_validation

        config.load_config("tests/data/test_config2.yml")
        assert os.path.basename(config.config_path) == "test_config2.yml"
        assert os.path.basename(config.synapse_configuration_path) == "file_name"
        assert config.synapse_manifest_basename == "file_name"
        assert config.synapse_master_fileview_id == "syn1"
        assert config.synapse_manifest_folder == "folder_name"
        assert config.manifest_title == "title"
        assert config.manifest_data_type == ["data_type"]
        assert config.model_location == "model.jsonld"
        assert config.model_file_type == "not_local"
        assert config.service_account_credentials_synapse_id
        assert os.path.basename(config.service_account_credentials_path) == "creds.json"
        assert config.google_sheets_master_template_id == ""
        assert not config.google_sheets_strict_validation

    def test_set_synapse_master_fileview_id(self) -> None:
        """Testing for Configuration synapse_master_fileview_id setter"""
        config = Configuration()
        assert config.synapse_master_fileview_id == "syn23643253"
        config.synapse_master_fileview_id = "syn1"
        assert config.synapse_master_fileview_id == "syn1"
        with pytest.raises(ValidationError):
            config.synapse_master_fileview_id = "syn"
