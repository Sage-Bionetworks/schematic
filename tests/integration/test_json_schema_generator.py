"""Tests for JSON Schema generation"""

from typing import Generator
import os
import json
import uuid

import synapseclient
import pytest

from schematic.models.metadata import MetadataModel
from schematic.schemas.json_schema_generator import (
    JSONSchemaGenerator,
)

@pytest.fixture(name="dm_json_schema")
def fixture_dm_json_schema() -> Generator[JSONSchemaGenerator, None, None]:
    """Yields a DataModelJSONSchema2 with the example data model"""
    metadata_model = MetadataModel(
        inputMModelLocation="tests/data/example.model.jsonld",
        inputMModelLocationType="local",
        data_model_labels="class_label",
    )
    data_model_js = JSONSchemaGenerator(
        jsonld_path=metadata_model.inputMModelLocation,
        graph=metadata_model.graph_data_model,
    )
    yield data_model_js

@pytest.mark.parametrize(
    "datatype",
    [
        ("Biospecimen"),
        ("BulkRNA-seqAssay"),
        ("MockComponent"),
        ("MockFilename"),
        ("MockRDB"),
        ("Patient"),
    ],
)
def test_upload_schemas_to_synapse(
    dm_json_schema: JSONSchemaGenerator, datatype: str
) -> None:
    """Tests for JSONSchemaGenerator.get_json_validation_schema"""
    try:
        test_folder = "tests/data/test_jsonschemas"
        test_file = f"test.{datatype}.schema.json"
        test_path = os.path.join(test_folder, test_file)
        title = f"{datatype}_validation"
        os.makedirs(test_folder, exist_ok=True)
        dm_json_schema.get_json_validation_schema(datatype, title, test_path)

        # Create a unique id fot the schema that is only characters
        schema_id = ''.join(i for i in str(uuid.uuid4()) if i.isalpha())
        test_schema_name = f"test.schematic.{schema_id}"

        syn = synapseclient.login()
        js = syn.service("json_schema")
        org = js.JsonSchemaOrganization("dpetest")
        with open(test_path, "r") as f:
            temp = json.load(f)
        org.create_json_schema(temp, test_schema_name, "0.0.1")

    finally:
        os.remove(test_path)
        js.delete_json_schema(f"dpetest-{test_schema_name}")
