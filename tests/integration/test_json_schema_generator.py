"""Tests for JSON Schema generation"""

from typing import Generator
import os
import json
import uuid
from time import sleep
import configparser

from synapseclient import Folder
from synapseclient.client import Synapse
import pytest

from schematic.models.metadata import MetadataModel
from schematic.schemas.json_schema_generator import (
    JSONSchemaGenerator,
)
from schematic.configuration.configuration import CONFIG


@pytest.fixture(name="synapse", scope="module")
def fixture_synapse() -> Generator[Synapse, None, None]:
    """
    This yields a Synapse instance that's been logged in.
    This has a module scope.
    The module scope is needed so that entity cleanup happens in the correct order.
    This allows the schema entities created below to be created once at the beginning
      of the module tests, and torn down at the end.
    """
    synapse_config_path = CONFIG.synapse_configuration_path
    config_parser = configparser.ConfigParser()
    config_parser.read(synapse_config_path)
    if "SYNAPSE_ACCESS_TOKEN" in os.environ:
        token = os.environ["SYNAPSE_ACCESS_TOKEN"]
    else:
        token = config_parser["authentication"]["authtoken"]
    syn = Synapse()
    syn.login(authToken=token, silent=True)
    return syn


@pytest.fixture(name="biospecimen_json_schema", scope="module")
def fixture_biospecimen_json_schema(synapse: Synapse):
    """This yields a Synapse JSON Schema uri"""
    path = "tests/data/expected_jsonschemas/expected.Biospecimen.schema.json"
    schema_name = upload_schema_to_synapse(path, synapse)
    js = synapse.service("json_schema")
    uri = f"dpetest-{schema_name}-0.0.1"
    yield uri
    js.delete_json_schema(f"dpetest-{schema_name}")


@pytest.fixture(name="bulk_rna_json_schema", scope="module")
def fixture_bulk_rna_json_schema(synapse: Synapse):
    """This yields a Synapse JSON Schema uri"""
    path = "tests/data/expected_jsonschemas/expected.BulkRNA-seqAssay.schema.json"
    schema_name = upload_schema_to_synapse(path, synapse)
    js = synapse.service("json_schema")
    uri = f"dpetest-{schema_name}-0.0.1"
    yield uri
    js.delete_json_schema(f"dpetest-{schema_name}")


@pytest.fixture(name="mock_component_json_schema", scope="module")
def fixture_mock_component_json_schema(synapse: Synapse):
    """This yields a Synapse JSON Schema uri"""
    path = "tests/data/expected_jsonschemas/expected.MockComponent.schema.json"
    schema_name = upload_schema_to_synapse(path, synapse)
    js = synapse.service("json_schema")
    uri = f"dpetest-{schema_name}-0.0.1"
    yield uri
    js.delete_json_schema(f"dpetest-{schema_name}")


@pytest.fixture(name="mock_filename_json_schema", scope="module")
def fixture_mock_filename_json_schema(synapse: Synapse):
    """This yields a Synapse JSON Schema uri"""
    path = "tests/data/expected_jsonschemas/expected.MockFilename.schema.json"
    schema_name = upload_schema_to_synapse(path, synapse)
    js = synapse.service("json_schema")
    uri = f"dpetest-{schema_name}-0.0.1"
    yield uri
    js.delete_json_schema(f"dpetest-{schema_name}")


@pytest.fixture(name="mock_rdb_json_schema", scope="module")
def fixture_mock_rdb_json_schema(synapse: Synapse):
    """This yields a Synapse JSON Schema uri"""
    path = "tests/data/expected_jsonschemas/expected.MockRDB.schema.json"
    schema_name = upload_schema_to_synapse(path, synapse)
    js = synapse.service("json_schema")
    uri = f"dpetest-{schema_name}-0.0.1"
    yield uri
    js.delete_json_schema(f"dpetest-{schema_name}")


@pytest.fixture(name="patient_json_schema", scope="module")
def fixture_patient_json_schema(synapse: Synapse):
    """This yields a Synapse JSON Schema uri"""
    path = "tests/data/expected_jsonschemas/expected.Patient.schema.json"
    schema_name = upload_schema_to_synapse(path, synapse)
    js = synapse.service("json_schema")
    uri = f"dpetest-{schema_name}-0.0.1"
    yield uri
    js.delete_json_schema(f"dpetest-{schema_name}")


@pytest.fixture(name="synapse_folder", scope="function")
def fixture_synapse_folder(syn: Synapse):
    """
    This yields a synapse id for a folder created for a test function.
    """
    folder_name = f"test_json_schema_validation_{str(uuid.uuid4())}"
    folder = Folder(name=folder_name, parent="syn23643250")
    folder = syn.store(obj=folder)
    yield folder.id
    js = syn.service("json_schema")
    js.unbind_json_schema(folder.id)
    syn.delete(folder.id)


@pytest.fixture(name="js_generator")
def fixture_js_generator() -> Generator[JSONSchemaGenerator, None, None]:
    """Yields a JSONSchemaGenerator with the example data model"""
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


def upload_schema_to_synapse(path: str, syn: Synapse) -> str:
    """Uploads a JSON Schema file to Synapse

    Arguments:
        path: The path to the JSON Schema file
        syn: A Synapse instance that's been logged in

    Returns:
        The name of the Schema in synapse
    """
    # Create a unique id for the schema that is only characters
    schema_id = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
    schema_name = f"test.schematic.{schema_id}"
    with open(path, encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    js = syn.service("json_schema")
    org = js.JsonSchemaOrganization("dpetest")
    org.create_json_schema(schema, schema_name, "0.0.1")
    return schema_name


@pytest.mark.parametrize(
    "schema_fixture",
    [
        ("biospecimen_json_schema"),
        ("bulk_rna_json_schema"),
        ("mock_component_json_schema"),
        ("mock_filename_json_schema"),
        ("mock_rdb_json_schema"),
        ("patient_json_schema"),
    ],
)
def test_upload_schemas_fixtures(schema_fixture: str, request) -> None:
    """
    This tests all the JSON Schema fixtures above.
    This tests that each test schema can be uploaded successfully.
    """
    request.getfixturevalue(schema_fixture)


@pytest.mark.parametrize(
    "instance_path, is_valid, messages, schema_fixture",
    [
        (
            "tests/data/json_instances/valid_patient1.json",
            True,
            [],
            "patient_json_schema",
        ),
        (
            "tests/data/json_instances/valid_patient2.json",
            True,
            [],
            "patient_json_schema",
        ),
        (
            "tests/data/json_instances/patient_missing_conditional_dependencies.json",
            False,
            [
                "#: required key [CancerType] not found",
                "#: required key [FamilyHistory] not found",
            ],
            "patient_json_schema",
        ),
    ],
    ids=[
        "Patient:valid #1",
        "Patient:valid #2",
        "Patient:CancerType and FamilyHistory required when Diagnosis==Cancer",
    ],
)
def test_upload_and_validate_schemas_in_synapse(
    instance_path: str,
    is_valid: bool,
    messages: list[str],
    schema_fixture: str,
    syn: Synapse,
    synapse_folder: str,
    request,
) -> None:
    """
    This:
    1. Creates a Synapse folder
    2. Binds the JSON Schema to the folder
    3. Annotates the folder
    4. Validates the annotations
    """
    js = syn.service("json_schema")

    test_schema_uri = request.getfixturevalue(schema_fixture)

    # Bind the Synapse folder with the schema
    js.bind_json_schema(test_schema_uri, synapse_folder)

    # Annotate the folder with a test instance
    existing_annotations = syn.get_annotations(synapse_folder)
    with open(instance_path, encoding="utf-8") as instance_file:
        instance = json.load(instance_file)
    existing_annotations.update(instance)
    syn.set_annotations(annotations=existing_annotations)
    sleep(4)

    # Attempt to validate the annotations against the bound schema
    results = js.validate(synapse_folder)
    assert results["isValid"] == is_valid
    if not results["isValid"]:
        assert "allValidationMessages" in results
        assert sorted(results["allValidationMessages"]) == messages
