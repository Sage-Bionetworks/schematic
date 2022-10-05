import os
import logging

import pytest

from schematic.models.metadata import MetadataModel

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def metadata_model(helpers):

    metadata_model = MetadataModel(
        inputMModelLocation=helpers.get_data_path("example.model.jsonld"),
        inputMModelLocationType="local",
    )

    yield metadata_model


class TestMetadataModel:
    @pytest.mark.parametrize("as_graph", [True, False], ids=["as_graph", "as_list"])
    def test_get_component_requirements(self, metadata_model, as_graph):

        source_component = "BulkRNA-seqAssay"

        output = metadata_model.get_component_requirements(
            source_component, as_graph=as_graph
        )

        assert type(output) is list

        if as_graph:
            assert ("Biospecimen", "Patient") in output
            assert ("BulkRNA-seqAssay", "Biospecimen") in output
        else:
            assert "Biospecimen" in output
            assert "Patient" in output
            assert "BulkRNA-seqAssay" in output

    @pytest.mark.parametrize("return_excel", [True, False])
    @pytest.mark.google_credentials_needed
    def test_populate_manifest(self, metadata_model, helpers, return_excel):
        #Get path of manifest 
        manifestPath = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
    
        #Call populateModelManifest class
        populated_manifest_route= metadata_model.populateModelManifest(title="mock_title", manifestPath=manifestPath, rootNode="MockComponent", return_excel=return_excel)

        if not return_excel:
            # return a url
            assert type(populated_manifest_route) is str
            assert populated_manifest_route.startswith("https://docs.google.com/spreadsheets/")
        else: 
            # return a valid file path
            assert os.path.exists(populated_manifest_route) == True

        # clean up 
        try:
            os.remove(helpers.get_data_path(f"mock_manifests/mock_title.xlsx"))
        except FileNotFoundError:
            pass
