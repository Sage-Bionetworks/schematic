import os
import logging
import pytest

from schematic.models.metadata import MetadataModel
from schematic.schemas.generator import SchemaGenerator

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
    @pytest.mark.parameterize("as_graph", [True, False], ids=["as_graph, as_list"])
    def test_get_component_requirements(self, metadata_model, as_graph):

        source_component = "BulkRNA-seqAssay"

        output = metadata_model.get_component_requirements(
            source_component, as_graph=as_graph
        )

        assert type(output) is list
