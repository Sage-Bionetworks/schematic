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
    # TODO: currently, there are no DependsOn Component relationships
    # in the example data model. Create a dummy relationship between Biospecimen
    # and Patient using tempfiles and assert list of values rather than data types
    
    @pytest.mark.parametrize("as_graph", [True, False], ids=["as_graph", "as_list"])
    def test_get_component_requirements(self, metadata_model, as_graph):

        source_component = "BulkRNA-seqAssay"

        output = metadata_model.get_component_requirements(
            source_component, as_graph=as_graph
        )
        
        assert type(output) is list
