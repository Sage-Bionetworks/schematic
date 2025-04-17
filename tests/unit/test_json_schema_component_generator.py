import pytest

from pathlib import Path
import os
import filecmp
import json

from schematic.schemas.json_schema_component_generator import (
    GeneratorDirector,
    JsonSchemaComponentGenerator,
)
from schematic.schemas.data_model_parser import DataModelParser

from tests.conftest import Helpers
from tests.utils import dict_equal
from unittest.mock import MagicMock, Mock, patch


@pytest.fixture
def parsed_example_model(helpers):
    data_model = helpers.get_data_path("example.model.jsonld")

    data_model_parser = DataModelParser(data_model)

    parsed_example_model = data_model_parser.parse_model()

    yield parsed_example_model


@pytest.fixture
def data_model(helpers):
    data_model = helpers.get_data_path("example.model.jsonld")

    yield data_model


@pytest.fixture
def output_directory(helpers):
    output_directory = helpers.get_data_path("test_jsonschemas")
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    yield output_directory

    # Cleanup after test
    for file in os.listdir(output_directory):
        file_path = os.path.join(output_directory, file)
        if os.path.isfile(file_path):
            os.remove(file_path)


class TestGeneratorDirector:
    @pytest.mark.parametrize("component", ["MockComponent"])
    def test_generate_jsonschema(
        self, helpers, component, output_directory, data_model
    ):
        expected_jsonschema = helpers.get_data_path(
            f"expected_jsonschemas/expected.{component}_validation_schema.json"
        )

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


class TestJsonSchemaComponentGenerator:
    def test_init(self, helpers, parsed_example_model, output_directory, data_model):
        component = "MockComponent"
        expected_output_path = Path(
            output_directory, f"{component}_validation_schema.json"
        )

        generator = JsonSchemaComponentGenerator(
            data_model=data_model,
            component=component,
            output_directory=output_directory,
            parsed_model=parsed_example_model,
        )

        assert generator.data_model == data_model
        assert generator.component == component
        assert generator.output_path == expected_output_path

    def test_init_missing_component(
        self, helpers, parsed_example_model, output_directory, data_model
    ):
        component = None
        with pytest.raises(ValueError):
            generator = JsonSchemaComponentGenerator(
                data_model=data_model,
                component=component,
                output_directory=output_directory,
                parsed_model=parsed_example_model,
            )

    def test_init_invalid_component(
        self, helpers, parsed_example_model, output_directory, data_model
    ):
        component = "InvalidComponent"

        generator = JsonSchemaComponentGenerator(
            data_model=data_model,
            component=component,
            output_directory=output_directory,
            parsed_model=parsed_example_model,
        )

        generator.get_data_model_json_schema()
        with pytest.raises(ValueError):
            generator.get_component_json_schema()

    def test_get_intermediate_json_schemas(
        self, helpers, parsed_example_model, output_directory, data_model
    ):
        component = "MockComponent"
        generated_json_schema_path = Path(
            output_directory, f"example.{component}.schema.json"
        )
        expected_intermediate_json_schema_content_path = helpers.get_data_path(
            f"expected_jsonschemas/incomplete.expected.{component}_validation.schema.json"
        )

        with patch(
            "schematic.schemas.data_model_json_schema.get_json_schema_log_file_path"
        ) as mock_get_json_schema_log_file_path:
            mock_get_json_schema_log_file_path.return_value = Path(
                output_directory, f"example.{component}.schema.json"
            )
            generator = JsonSchemaComponentGenerator(
                data_model=data_model,
                component=component,
                output_directory=output_directory,
                parsed_model=parsed_example_model,
            )

            generator.get_data_model_json_schema()
            generator.get_component_json_schema()

            assert os.path.isfile(generated_json_schema_path)

            with open(generated_json_schema_path, "r") as generated_json_schema_file:
                generated_json_schema = json.load(generated_json_schema_file)
            with open(
                expected_intermediate_json_schema_content_path, "r"
            ) as expected_intermediate_json_schema_file:
                expected_intermediate_json_schema = json.load(
                    expected_intermediate_json_schema_file
                )

            assert dict_equal(expected_intermediate_json_schema, generated_json_schema)

    def test_add_description_to_json_schema(
        self, helpers, parsed_example_model, output_directory, data_model
    ):
        component = "MockComponent"
        generator = JsonSchemaComponentGenerator(
            data_model=data_model,
            component=component,
            output_directory=output_directory,
            parsed_model=parsed_example_model,
        )

        generator.get_data_model_json_schema()

        generator.get_component_json_schema()
        generator.add_description_to_json_schema()

        assert generator.component_json_schema.get(
            "description"
        ) == parsed_example_model.get("MockComponent").get("Relationships").get(
            "Description"
        )
