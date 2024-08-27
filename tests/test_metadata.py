"""Tests for Metadata class"""

import logging
import os
from pathlib import Path
from typing import Generator, Optional
from unittest.mock import patch

import pytest

from schematic.models.metadata import MetadataModel
from tests.conftest import Helpers

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def metadata_model(helpers, data_model_labels):
    metadata_model = MetadataModel(
        inputMModelLocation=helpers.get_data_path("example.model.jsonld"),
        data_model_labels=data_model_labels,
        inputMModelLocationType="local",
    )

    return metadata_model


class TestMetadataModel:
    @pytest.mark.parametrize("as_graph", [True, False], ids=["as_graph", "as_list"])
    @pytest.mark.parametrize(
        "data_model_labels",
        ["display_label", "class_label"],
        ids=["data_model_labels-display_label", "data_model_labels-class_label"],
    )
    def test_get_component_requirements(self, helpers, as_graph, data_model_labels):
        # Instantiate MetadataModel
        meta_data_model = metadata_model(helpers, data_model_labels)

        if data_model_labels == "display_label":
            source_component = "BulkRNAseqAssay"
        else:
            source_component = "BulkRNA-seqAssay"

        output = meta_data_model.get_component_requirements(
            source_component, as_graph=as_graph
        )

        assert type(output) is list

        if as_graph:
            assert ("Biospecimen", "Patient") in output
            if data_model_labels == "display_label":
                assert ("BulkRNAseqAssay", "Biospecimen") in output
            else:
                assert ("BulkRNA-seqAssay", "Biospecimen") in output
        else:
            assert "Biospecimen" in output
            assert "Patient" in output
            if data_model_labels == "display_label":
                assert "BulkRNAseqAssay" in output
            else:
                assert "BulkRNA-seqAssay" in output

    @pytest.mark.parametrize("return_excel", [None, True, False])
    @pytest.mark.parametrize(
        "data_model_labels",
        ["display_label", "class_label"],
        ids=["data_model_labels-display_label", "data_model_labels-class_label"],
    )
    @pytest.mark.google_credentials_needed
    def test_populate_manifest(self, helpers, return_excel, data_model_labels):
        # Instantiate MetadataModel
        meta_data_model = metadata_model(helpers, data_model_labels)

        # Get path of manifest
        manifestPath = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")

        # Call populateModelManifest class
        populated_manifest_route = meta_data_model.populateModelManifest(
            title="mock_title",
            manifestPath=manifestPath,
            rootNode="MockComponent",
            return_excel=return_excel,
        )

        if not return_excel:
            # return a url
            assert type(populated_manifest_route) is str
            assert populated_manifest_route.startswith(
                "https://docs.google.com/spreadsheets/"
            )
        else:
            # return a valid file path
            assert os.path.exists(populated_manifest_route) == True

        # clean up
        output_path = os.path.join(os.getcwd(), "mock_title.xlsx")
        try:
            os.remove(output_path)
        except:
            pass

    @pytest.mark.parametrize("file_annotations_upload", [True, False])
    @pytest.mark.parametrize("restrict_rules", [True, False])
    @pytest.mark.parametrize("hide_blanks", [True, False])
    @pytest.mark.parametrize(
        "data_model_labels",
        ["display_label", "class_label"],
        ids=["data_model_labels-display_label", "data_model_labels-class_label"],
    )
    @pytest.mark.parametrize("validate_component", [None, "BulkRNA-seqAssay"])
    @pytest.mark.parametrize(
        "temporary_file_copy", ["test_BulkRNAseq.csv"], indirect=True
    )
    def test_submit_metadata_manifest(
        self,
        temporary_file_copy: Generator[str, None, None],
        helpers: Helpers,
        file_annotations_upload: bool,
        restrict_rules: bool,
        data_model_labels: str,
        hide_blanks: bool,
        validate_component: Optional[str],
    ) -> None:
        meta_data_model = metadata_model(helpers, data_model_labels)
        with patch(
            "schematic.models.metadata.MetadataModel.validateModelManifest",
            return_value=([], []),
        ):
            with patch(
                "schematic.store.synapse.SynapseStorage.associateMetadataWithFiles",
                return_value="mock manifest id",
            ):
                mock_manifest_path = temporary_file_copy

                mock_manifest_id = meta_data_model.submit_metadata_manifest(
                    manifest_path=mock_manifest_path,
                    validate_component=validate_component,
                    dataset_id="mock dataset id",
                    manifest_record_type="file_only",
                    restrict_rules=restrict_rules,
                    file_annotations_upload=file_annotations_upload,
                    hide_blanks=hide_blanks,
                )
                assert mock_manifest_id == "mock manifest id"
