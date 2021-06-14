import os
import logging
import pytest

from schematic.models.metadata import MetadataModel
from schematic.schemas.generator import SchemaGenerator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def metadata_model(helpers):

        mm = MetadataModel(
            helpers.get_data_path("example.model.jsonld"),
            "local"
        )

        yield mm


class TestMetadataModel:

    def test_get_component_requirements(self, metadata_model):
            
        source_component = "BulkRNA-seqAssay"
        as_graph = True
        
        output = metadata_model.get_component_requirements(source_component, as_graph = as_graph)
        
        assert type(output) is list
