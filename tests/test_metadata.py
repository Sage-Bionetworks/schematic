import os
import logging

import pytest

from schematic.models.metadata import MetadataModel

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def metadata_model(helpers, display_name_as_label):

    metadata_model = MetadataModel(
        inputMModelLocation=helpers.get_data_path("example.model.jsonld"), display_name_as_label=display_name_as_label,
        inputMModelLocationType="local",
    )

    return metadata_model


class TestMetadataModel:
    @pytest.mark.parametrize("as_graph", [True, False], ids=["as_graph", "as_list"])
    @pytest.mark.parametrize("display_name_as_label", [True, False], ids=["display_name_as_label-True", "display_name_as_label-False"])
    def test_get_component_requirements(self, helpers, as_graph, display_name_as_label):
        # Instantiate MetadataModel
        meta_data_model = metadata_model(helpers, display_name_as_label)

        source_component = "BulkRNA-seqAssay"

        output = meta_data_model.get_component_requirements(
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

    @pytest.mark.parametrize("return_excel", [None, True, False])
    @pytest.mark.parametrize("display_name_as_label", [True, False], ids=["display_name_as_label-True", "display_name_as_label-False"])
    @pytest.mark.google_credentials_needed
    def test_populate_manifest(self, helpers, return_excel, display_name_as_label):

        # Instantiate MetadataModel
        meta_data_model = metadata_model(helpers, display_name_as_label)

        #Get path of manifest 
        manifestPath = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
    
        #Call populateModelManifest class
        populated_manifest_route= meta_data_model.populateModelManifest(title="mock_title", manifestPath=manifestPath, rootNode="MockComponent", return_excel=return_excel)

        if not return_excel:
            # return a url
            assert type(populated_manifest_route) is str
            assert populated_manifest_route.startswith("https://docs.google.com/spreadsheets/")
        else: 
            # return a valid file path
            assert os.path.exists(populated_manifest_route) == True

        # clean up 
        output_path = os.path.join(os.getcwd(), "mock_title.xlsx")
        try:
            os.remove(output_path)
        except: 
            pass
