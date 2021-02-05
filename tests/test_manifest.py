import logging
import pytest
from click.testing import CliRunner

import numpy as np
import pandas as pd

from schematic.manifest.generator import ManifestGenerator
from schematic.manifest.commands import get_manifest
from schematic.schemas.generator import SchemaGenerator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture()
def manifest_generator(helpers):
    # Retrieve the `get_data_file` helper function
    get_data_file = helpers.get_data_file

    manifest_generator = ManifestGenerator(
        title="Patient_Manifest",
        path_to_json_ld=get_data_file("simple.model.jsonld"),
        root="Patient"
    )

    return manifest_generator


class TestManifestGenerator:

    # def test__init(self, monkeypatch, mock_creds):
    #     monkeypatch.setattr("schematic.manifest.generator.build_credentials",
    #                         lambda: mock_creds)

    #     generator = ManifestGenerator(
    #         title="mock_title",
    #         path_to_json_ld="mock_path"
    #     )

    #     assert type(generator.title) is str
    #     assert generator.sheet_service == mock_creds["sheet_service"]
    #     assert generator.root is None
    #     assert type(generator.sg) is SchemaGenerator


    def test_update_manifest(self):
        manifest_df = pd.DataFrame({
            "numCol": [1, 2],
            "entityId": ["syn01", "syn02"],
            "strCol": ["foo", "bar"]
        }, columns=["numCol", "entityId", "strCol"])
        updates_df = pd.DataFrame({
            "strCol": ["___", np.nan],
            "numCol": [np.nan, 4],
            "entityId": ["syn01", "syn02"]
        }, columns=["strCol", "numCol", "entityId"])
        expected_df = pd.DataFrame({
            "numCol": [1, float(4)],
            "entityId": ["syn01", "syn02"],
            "strCol": ["___", "bar"]
        }, columns=["numCol", "entityId", "strCol"])

        actual_df = ManifestGenerator.update_manifest(manifest_df, updates_df)
        pd.testing.assert_frame_equal(expected_df, actual_df)


    def test_get_manifest_empty(self, manifest_generator, config):
        manifest_df = manifest_generator.get_manifest(
            dataset_id="syn24226514",
            use_annotations=False,
            json_schema=config["model"]["input"]["validation_schema"]
        )
        assert "eTag" in manifest_df
        assert "Year of Birth" in manifest_df
        assert "confidence" not in manifest_df
        assert manifest_df["Year of Birth"].tolist() == ["", "", ""]
        assert manifest_df.shape[0] == 3  # Number of rows
        assert manifest_df.shape[1] == 20  # Number of columns


    def test_get_manifest_empty_use_annotations(self, manifest_generator, config):
        manifest_df = manifest_generator.get_manifest(
            dataset_id="syn24226514",
            use_annotations=True,
            json_schema=config["model"]["input"]["validation_schema"]
        )
        assert "eTag" in manifest_df
        assert "Year of Birth" in manifest_df
        assert "confidence" in manifest_df
        assert manifest_df["Year of Birth"].tolist() == ["1980", "", ""]
        assert manifest_df.shape[0] == 3  # Number of rows
        assert manifest_df.shape[1] == 24  # Number of columns
