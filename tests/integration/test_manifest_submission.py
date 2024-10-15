import logging
import os
from typing import Callable

import pandas as pd
import requests
from synapseclient.client import Synapse

from tests.conftest import Helpers
from tests.utils import CleanupItem

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestManifestSubmission:
    def test_submit_record_based_test_manifest_file_only(
        self,
        helpers: Helpers,
        syn: Synapse,
        syn_token: str,
        download_location: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
    ) -> None:
        """Test that a record-based manifest can be submitted with the file_only and replace option

        Args:
            helpers (Helpers): a pytest fixture
            syn (Synapse): synapse client
            syn_token (str): synapse access token
            download_location (str): path to download location
            schedule_for_cleanup (Callable[[CleanupItem], None]): Returns a closure that takes an item that should be scheduled for cleanup.

        We are validating the following:
        - The submitted manifest has correct file name: synapse_storage_manifest_<data_type>.csv
        - The submitted manifest has column entityId and Id
        - The submitted manifest has Id column that is not empty
        """

        url = "http://localhost:3001/v1/model/submit"
        data_type = "Biospecimen"
        params = {
            "schema_url": "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld",
            "data_model_labels": "class_label",
            "data_type": data_type,
            "dataset_id": "syn63561474",
            "manifest_record_type": "file_only",
            "restrict_rules": "false",
            "hide_blanks": "false",
            "asset_view": "syn63561606",
            "table_manipulation": "replace",
            "table_column_names": "class_label",
            "annotation_keys": "class_label",
            "file_annotations_upload": "false",
        }

        headers = {"Authorization": f"Bearer {syn_token}"}
        test_manifest_path = helpers.get_data_path(
            "mock_manifests/mock_example_biospecimen_manifest.csv"
        )

        # THEN we expect a successful response
        response = requests.post(
            url,
            headers=headers,
            params=params,
            files={"file_name": open(test_manifest_path, "rb")},
        )
        assert response.status_code == 200

        # Get the manifest ID from the response
        manifest_id = response.json()
        # clean up
        schedule_for_cleanup(CleanupItem(manifest_id))

        # Load then manifest from synapse
        manifest_data = syn.get(
            manifest_id,
            downloadLocation=download_location,
            ifcollision="overwrite.local",
        )
        # make sure that the file name of manifest is correct
        assert (
            manifest_data["properties"]["name"]
            == (f"synapse_storage_manifest_{data_type}.csv").lower()
        )

        # make sure that entity id and id columns were added
        manifest_file_path = os.path.join(
            download_location, manifest_data["properties"]["name"]
        )
        manifest_submitted_df = pd.read_csv(manifest_file_path)
        assert "entityId" in manifest_submitted_df.columns
        assert "Id" in manifest_submitted_df.columns

        # make sure that Id column is not empty
        assert manifest_submitted_df["Id"].notnull().all()

    def test_submit_record_based_test_manifest_table_and_file(
        self,
        helpers: Helpers,
        syn_token: str,
        syn: Synapse,
        download_location: str,
        schedule_for_cleanup: Callable[[CleanupItem], None],
    ) -> None:
        """Test that a record-based manifest can be submitted with the table and file and replace option

        Args:
            helpers (Helpers): a pytest fixture
            syn (Synapse): synapse client
            syn_token (str): synapse access token
            download_location (str): path to download location
            schedule_for_cleanup (Callable[[CleanupItem], None]): Returns a closure that takes an item that should be scheduled for cleanup.

        We are validating the following:
        - The submitted manifest has correct file name: synapse_storage_manifest_<data_type>.csv
        - The submitted manifest has column entityId and Id
        - The submitted manifest has Id column that is not empty
        - The table gets created in the parent synapse project
        """
        url = "http://localhost:3001/v1/model/submit"
        data_type = "Biospecimen"
        project_id = "syn63561415"
        dataset_id = "syn63561474"
        asset_view = "syn63561606"
        expected_table_name = f"{data_type}_synapse_storage_manifest_table".lower()

        params = {
            "schema_url": "https://raw.githubusercontent.com/Sage-Bionetworks/schematic/develop/tests/data/example.model.jsonld",
            "data_model_labels": "class_label",
            "data_type": data_type,
            "dataset_id": dataset_id,
            "manifest_record_type": "table_and_file",
            "restrict_rules": "false",
            "hide_blanks": "false",
            "asset_view": asset_view,
            "table_column_names": "class_label",
            "annotation_keys": "class_label",
            "file_annotations_upload": "false",
        }

        headers = {"Authorization": f"Bearer {syn_token}"}
        test_manifest_path = helpers.get_data_path(
            "mock_manifests/mock_example_biospecimen_manifest.csv"
        )

        # THEN we expect a successful response
        response = requests.post(
            url,
            headers=headers,
            params=params,
            files={"file_name": open(test_manifest_path, "rb")},
        )
        assert response.status_code == 200

        # Get the manifest ID from the response
        manifest_id = response.json()
        schedule_for_cleanup(CleanupItem(manifest_id))

        # Load then manifest from synapse
        manifest_data = syn.get(
            manifest_id,
            downloadLocation=download_location,
            ifcollision="overwrite.local",
        )

        # make sure that the file name of manifest is correct
        assert (
            manifest_data["properties"]["name"]
            == (f"synapse_storage_manifest_{data_type}.csv").lower()
        )

        # make sure that entity id and id columns were added
        manifest_file_path = os.path.join(
            download_location, manifest_data["properties"]["name"]
        )
        manifest_submitted_df = pd.read_csv(manifest_file_path)
        assert "entityId" in manifest_submitted_df.columns
        assert "Id" in manifest_submitted_df.columns

        # make sure that Id column is not empty
        assert manifest_submitted_df["Id"].notnull().all()

        # make sure that the table gets created
        synapse_id = syn.findEntityId(parent=project_id, name=expected_table_name)
        assert synapse_id is not None
        schedule_for_cleanup(CleanupItem(synapse_id))
