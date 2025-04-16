import pytest

from pathlib import Path
import os
import filecmp

from schematic.schemas.json_schema_component_generator import GeneratorDirector
from tests.conftest import Helpers


class TestGeneratorDirector:
    @pytest.mark.parametrize("component", ["MockComponent"])
    def test_generate_jsonschema(
        self,
        helpers,
        component,
    ):
        output_directory = helpers.get_data_path("test_jsonschemas")
        expected_jsonschema = helpers.get_data_path(
            f"expected_jsonschemas/expected.{component}_validation_schema.json"
        )

        data_model = helpers.get_data_path("example.model.jsonld")
        generator = GeneratorDirector(
            data_model=data_model,
            components=[component],
            output_directory=output_directory,
        )
        json_schema = generator.generate_jsonschema()

        generated_jsonschema = helpers.get_data_path(
            f"{output_directory}/{component}_validation_schema.json"
        )

        assert json_schema is not None
        assert isinstance(json_schema, list)

        assert os.path.isfile(generated_jsonschema)
        assert filecmp.cmp(
            expected_jsonschema,
            generated_jsonschema,
            shallow=False,
        )

        return


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
