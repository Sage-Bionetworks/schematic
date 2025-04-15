import pytest

from schematic.schemas.json_schema_component_generator import GeneratorDirector
from tests.conftest import Helpers


class TestJsonSchemaComponentGenerator:
    @pytest.mark.parametrize(
        "data_model, expected_components",
        [
            (
                "example.model.jsonld",
                [
                    "Patient",
                    "Biospecimen",
                    "Bulk RNA-seq Assay",
                    "MockComponent",
                    "MockRDB",
                    "MockFilename",
                ],
            ),
        ],
        ids=[
            "example model",
        ],
    )
    def test_extract_components(
        self, helpers, data_model: str, expected_components: list[str]
    ):
        if data_model.startswith("example"):
            data_model = helpers.get_data_path(data_model)
        generator = GeneratorDirector(data_model=data_model)
        identified_components = generator._extract_components()

        assert expected_components == identified_components

        return
