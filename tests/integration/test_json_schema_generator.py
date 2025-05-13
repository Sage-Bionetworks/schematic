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
        with open(test_path, encoding="utf-8") as schema_file:
            schema = json.load(schema_file)
        org.create_json_schema(schema, test_schema_name, "0.0.1")

    finally:
        os.remove(test_path)
        js.delete_json_schema(f"dpetest-{test_schema_name}")


@pytest.mark.parametrize(
    "datatype, instance_path, is_valid, messages",
    [
        (
            "Patient",
            "tests/data/json_instances/valid_patient1.json",
            True,
            [],
        ),
        (
            "Patient",
            "tests/data/json_instances/patient_missing_conditional_dependencies.json",
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
    instance_path: str,
    is_valid: bool,
    messages: list[str],
) -> None:
    """Tests for JSONSchemaGenerator.get_json_validation_schema"""
    try:
        test_folder = "tests/data/test_jsonschemas"
        test_file = f"test.{datatype}.schema.json"
        test_path = os.path.join(test_folder, test_file)
        title = f"{datatype}_validation"

        # Create JSON Schema locally
        os.makedirs(test_folder, exist_ok=True)
        js_generator.create_json_schema(datatype, title, test_path)

        # Create a unique id fot the schema that is only characters
        schema_id = "".join(i for i in str(uuid.uuid4()) if i.isalpha())

        # Create the JSON Schema in Synapse
        js = syn.service("json_schema")
        org = js.JsonSchemaOrganization("dpetest")
        with open(test_path, encoding="utf-8") as schema_file:
            schema = json.load(schema_file)
        test_schema_name = f"test.schematic.{schema_id}"
        org.create_json_schema(schema, test_schema_name, "0.0.1")
        test_schema_uri = f"dpetest-{test_schema_name}-0.0.1"

        # Create a folder to bind the JSON Schema to in Synapse
        folder_name = f"test_json_schema_validation_{str(uuid.uuid4())}"
        folder = Folder(name=folder_name, parent="syn23643250")
        folder = syn.store(obj=folder)
        folder_id = folder.id

        # Bind the Synapse folder with the schema
        js.bind_json_schema(test_schema_uri, folder_id)

        # Annotate the folder with a test instance
        existing_annotations = syn.get_annotations(folder_id)
        with open(instance_path, encoding="utf-8") as instance_file:
            instance = json.load(instance_file)
        existing_annotations.update(instance)
        syn.set_annotations(annotations=existing_annotations)
        sleep(4)

        # Attempt to validate the annotations against the bound schema
        results = js.validate(folder.id)
        assert results["isValid"] == is_valid
        if not results["isValid"]:
            assert "allValidationMessages" in results
            assert sorted(results["allValidationMessages"]) == messages

    finally:
        os.remove(test_path)
        js.unbind_json_schema(folder.id)
        syn.delete(folder.id)
        js.delete_json_schema(f"dpetest-{test_schema_name}")
