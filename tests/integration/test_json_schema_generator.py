"""Tests for JSON Schema generation"""

from typing import Generator, Any
import os
import json
import uuid
from time import sleep

from synapseclient import Folder
from synapseclient.client import Synapse
import pytest

from schematic.models.metadata import MetadataModel
from schematic.schemas.json_schema_generator import (
    JSONSchemaGenerator,
)


@pytest.fixture(name="js_generator")
def fixture_js_generator() -> Generator[JSONSchemaGenerator, None, None]:
    """Yields a DataModelJSONSchema2 with the example data model"""
    metadata_model = MetadataModel(
        inputMModelLocation="tests/data/example.model.jsonld",
        inputMModelLocationType="local",
        data_model_labels="class_label",
    )
    js_generator = JSONSchemaGenerator(
        jsonld_path=metadata_model.inputMModelLocation,
        graph=metadata_model.graph_data_model,
    )
    yield js_generator


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
    syn: Synapse, js_generator: JSONSchemaGenerator, datatype: str
) -> None:
    """Tests for JSONSchemaGenerator.get_json_validation_schema"""
    try:
        test_folder = "tests/data/test_jsonschemas"
        test_file = f"test.{datatype}.schema.json"
        test_path = os.path.join(test_folder, test_file)
        title = f"{datatype}_validation"
        os.makedirs(test_folder, exist_ok=True)
        js_generator.create_json_schema(datatype, title, test_path)

        # Create a unique id fot the schema that is only characters
        schema_id = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
        test_schema_name = f"test.schematic.{schema_id}"

        js = syn.service("json_schema")
        org = js.JsonSchemaOrganization("dpetest")
        with open(test_path, "r") as f:
            temp = json.load(f)
        org.create_json_schema(temp, test_schema_name, "0.0.1")

    finally:
        os.remove(test_path)
        js.delete_json_schema(f"dpetest-{test_schema_name}")


@pytest.mark.parametrize(
    "datatype, annotations, is_valid, messages",
    [
        (
            "Patient",
            {
                "Diagnosis": "Healthy",
                "Component": "test",
                "Sex": "Male",
                "PatientID": "test",
            },
            True,
            [],
        ),
        (
            "Patient",
            {
                "Diagnosis": "Cancer",
                "Component": "test",
                "Sex": "Male",
                "PatientID": "test",
            },
            False,
            [
                "#: required key [CancerType] not found",
                "#: required key [FamilyHistory] not found",
            ],
        ),
    ],
    ids=[
        "Patient:valid",
        "Patient:CancerType and FamilyHistory required when Diagnosis==Cancer",
    ],
)
def test_upload_and_validate_schemas_in_synapse(
    syn: Synapse,
    js_generator: JSONSchemaGenerator,
    datatype: str,
    annotations: dict[str, Any],
    is_valid: bool,
    messages: list[str],
) -> None:
    """Tests for JSONSchemaGenerator.get_json_validation_schema"""
    try:
        test_folder = "tests/data/test_jsonschemas"
        test_file = f"test.{datatype}.schema.json"
        test_path = os.path.join(test_folder, test_file)
        title = f"{datatype}_validation"
        os.makedirs(test_folder, exist_ok=True)
        js_generator.get_json_validation_schema(datatype, title, test_path)

        # Create a unique id fot the schema that is only characters
        schema_id = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
        test_schema_name = f"test.schematic.{schema_id}"

        js = syn.service("json_schema")
        org = js.JsonSchemaOrganization("dpetest")
        with open(test_path, "r") as f:
            temp = json.load(f)
        org.create_json_schema(temp, test_schema_name, "0.0.1")

        folder_name = f"test_json_schema_validation_{str(uuid.uuid4())}"
        folder = Folder(name=folder_name, parent="syn23643250")
        folder = syn.store(obj=folder)
        folder_id = folder.id
        test_schema_uri = f"dpetest-{test_schema_name}-0.0.1"
        js.bind_json_schema(test_schema_uri, folder_id)

        existing_annotations = syn.get_annotations(folder_id)
        existing_annotations.update(annotations)
        syn.set_annotations(annotations=existing_annotations)
        sleep(4)

        results = js.validate(folder.id)
        print(results)
        assert results["isValid"] == is_valid
        if not results["isValid"]:
            assert "allValidationMessages" in results
            assert sorted(results["allValidationMessages"]) == messages

    finally:
        os.remove(test_path)
        js.unbind_json_schema(folder.id)
        syn.delete(folder.id)
        js.delete_json_schema(f"dpetest-{test_schema_name}")
