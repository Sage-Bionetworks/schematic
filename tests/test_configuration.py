"""Testing for Configuration module"""
import pytest
from pydantic import ValidationError
from schematic.configuration.dataclasses import (
    SynapseConfig,
    ManifestConfig,
    ModelConfig,
    GoogleSheetsConfig,
)


class TestDataclasses:
    """Testing for pydantic dataclasses"""

    def test_synapse_config(self):
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
