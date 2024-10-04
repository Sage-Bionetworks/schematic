import logging
import os
import shutil

import pandas as pd
import pytest
import requests

from schematic.utils.general import create_temp_folder
from tests.utils import CleanupItem

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def download_location():
    temporary_manifest_storage = "temporary_manifest_storage"
    if not os.path.exists(temporary_manifest_storage):
        os.makedirs(temporary_manifest_storage)
    full_parent_path = os.path.abspath(temporary_manifest_storage)

    download_location = create_temp_folder(full_parent_path)
    yield download_location

    # Cleanup after tests have used the temp folder
    if os.path.exists(download_location):
        shutil.rmtree(download_location)


class TestManifestSubmission:
    def test_submit_record_based_test_manifest_file_only(
        self, helpers, syn, syn_token, download_location, schedule_for_cleanup
    ) -> None:
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

        # syn = Synapse()
        # syn.login(authToken=syn_token, silent=True)

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
        self, helpers, syn_token, syn, download_location, schedule_for_cleanup
    ) -> None:
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
