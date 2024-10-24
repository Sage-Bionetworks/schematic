import os
import uuid

import pytest
from click.testing import CliRunner

from schematic.configuration.configuration import Configuration
from schematic.manifest.commands import manifest


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""

    return CliRunner()


class TestDownloadManifest:
    """Tests the command line interface for downloading a manifest"""

    def test_download_manifest_found(
        self,
        runner: CliRunner,
        config: Configuration,
    ) -> None:
        # GIVEN a manifest name to download as
        manifest_name = f"{uuid.uuid4()}"

        # AND a dataset id
        dataset_id = "syn23643250"

        # AND a configuration file
        config.load_config("config_example.yml")

        # WHEN the download command is run
        result = runner.invoke(
            cli=manifest,
            args=[
                "--config",
                config.config_path,
                "download",
                "--new_manifest_name",
                manifest_name,
                "--dataset_id",
                dataset_id,
            ],
        )

        # THEN the command should run successfully
        assert result.exit_code == 0

        # AND the manifest file should be created
        expected_manifest_file = os.path.join(
            config.manifest_folder, f"{manifest_name}.csv"
        )
        assert os.path.exists(expected_manifest_file)
        try:
            os.remove(expected_manifest_file)
        except Exception:
            pass

    def test_download_manifest_not_found(
        self,
        runner: CliRunner,
        config: Configuration,
    ) -> None:
        # GIVEN a manifest name to download as
        manifest_name = f"{uuid.uuid4()}"

        # AND a dataset id that does not exist
        dataset_id = "syn1234"

        # AND a configuration file
        config.load_config("config_example.yml")

        # WHEN the download command is run
        result = runner.invoke(
            cli=manifest,
            args=[
                "--config",
                config.config_path,
                "download",
                "--new_manifest_name",
                manifest_name,
                "--dataset_id",
                dataset_id,
            ],
        )

        # THEN the command should not run successfully
        assert result.exit_code == 1

        # AND the manifest file should not be created
        expected_manifest_file = os.path.join(
            config.manifest_folder, f"{manifest_name}.csv"
        )
        assert not os.path.exists(expected_manifest_file)
