"""Tests for JSON Schema generation"""

from typing import Generator, Any
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


@pytest.fixture(name="schema_org", scope="module")
def fixture_schema_org() -> Generator[str, None, None]:
    """
    This yields the Synapse org the test Schemas will be created at
    """
    yield "dpetest"


@pytest.fixture(name="schema_version", scope="module")
def fixture_schema_version() -> Generator[str, None, None]:
    """
    This the version to give all created test schemas
    """
    yield "0.0.1"


@pytest.fixture(name="js_generator", scope="module")
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
def fixture_biospecimen_json_schema(
    synapse: Synapse,
    schema_org: str,
    schema_version: str,
    js_generator: JSONSchemaGenerator,
):
    """This yields a Synapse JSON Schema uri"""
    schema = js_generator.create_json_schema(
        "Biospecimen", schema_name="Biospecimen_validation", write_schema=False
    )
    schema_name = upload_schema_to_synapse(schema, synapse, schema_org, schema_version)
    js = synapse.service("json_schema")
    uri = f"{schema_org}-{schema_name}-{schema_version}"
    yield uri
    js.delete_json_schema(f"{schema_org}-{schema_name}")


@pytest.fixture(name="bulk_rna_json_schema", scope="module")
def fixture_bulk_rna_json_schema(
    synapse: Synapse,
    schema_org: str,
    schema_version: str,
    js_generator: JSONSchemaGenerator,
):
    """This yields a Synapse JSON Schema uri"""
    schema = js_generator.create_json_schema(
        "BulkRNA-seqAssay",
        schema_name="BulkRNA-seqAssay_validation",
        write_schema=False,
    )
    schema_name = upload_schema_to_synapse(schema, synapse, schema_org, schema_version)
    js = synapse.service("json_schema")
    uri = f"{schema_org}-{schema_name}-{schema_version}"
    yield uri
    js.delete_json_schema(f"{schema_org}-{schema_name}")


@pytest.fixture(name="mock_component_json_schema", scope="module")
def fixture_mock_component_json_schema(
    synapse: Synapse,
    schema_org: str,
    schema_version: str,
    js_generator: JSONSchemaGenerator,
):
    """This yields a Synapse JSON Schema uri"""
    schema = js_generator.create_json_schema(
        "MockComponent", schema_name="MockComponent_validation", write_schema=False
    )
    schema_name = upload_schema_to_synapse(schema, synapse, schema_org, schema_version)
    js = synapse.service("json_schema")
    uri = f"{schema_org}-{schema_name}-{schema_version}"
    yield uri
    js.delete_json_schema(f"{schema_org}-{schema_name}")


@pytest.fixture(name="mock_filename_json_schema", scope="module")
def fixture_mock_filename_json_schema(
    synapse: Synapse,
    schema_org: str,
    schema_version: str,
    js_generator: JSONSchemaGenerator,
):
    """This yields a Synapse JSON Schema uri"""
    schema = js_generator.create_json_schema(
        "MockFilename", schema_name="MockFilename_validation", write_schema=False
    )
    schema_name = upload_schema_to_synapse(schema, synapse, schema_org, schema_version)
    js = synapse.service("json_schema")
    uri = f"{schema_org}-{schema_name}-{schema_version}"
    yield uri
    js.delete_json_schema(f"{schema_org}-{schema_name}")


@pytest.fixture(name="mock_rdb_json_schema", scope="module")
def fixture_mock_rdb_json_schema(
    synapse: Synapse,
    schema_org: str,
    schema_version: str,
    js_generator: JSONSchemaGenerator,
):
    """This yields a Synapse JSON Schema uri"""
    schema = js_generator.create_json_schema(
        "MockRDB", schema_name="MockRDB_validation", write_schema=False
    )
    schema_name = upload_schema_to_synapse(schema, synapse, schema_org, schema_version)
    js = synapse.service("json_schema")
    uri = f"{schema_org}-{schema_name}-{schema_version}"
    yield uri
    js.delete_json_schema(f"{schema_org}-{schema_name}")


@pytest.fixture(name="patient_json_schema", scope="module")
def fixture_patient_json_schema(
    synapse: Synapse,
    schema_org: str,
    schema_version: str,
    js_generator: JSONSchemaGenerator,
):
    """This yields a Synapse JSON Schema uri"""
    schema = js_generator.create_json_schema(
        "Patient", schema_name="Patient_validation", write_schema=False
    )
    schema_name = upload_schema_to_synapse(schema, synapse, schema_org, schema_version)
    js = synapse.service("json_schema")
    uri = f"{schema_org}-{schema_name}-{schema_version}"
    yield uri
    js.delete_json_schema(f"{schema_org}-{schema_name}")


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


def upload_schema_to_synapse(
    schema: dict[str, Any], syn: Synapse, schema_org: str, version: str
) -> str:
    """Uploads a JSON Schema file to Synapse

    Arguments:
        schema: The schema to upload
        syn: A Synapse instance that's been logged in
        schema_org: The name of the org to store the schema at
        version: the version to give to the schema

    Returns:
        The name of the Schema in synapse
    """
    # Create a unique id for the schema that is only characters
    schema_id = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
    schema_name = f"test.schematic.{schema_id}"
    js = syn.service("json_schema")
    org = js.JsonSchemaOrganization(schema_org)
    org.create_json_schema(schema, schema_name, version)
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

    # GIVEN a schema in Synapse
    test_schema_uri = request.getfixturevalue(schema_fixture)

    # WHEN binding the schema to a folder
    js.bind_json_schema(test_schema_uri, synapse_folder)

    # WHEN annotating the folder with a JSON instance of the datatype
    existing_annotations = syn.get_annotations(synapse_folder)
    with open(instance_path, encoding="utf-8") as instance_file:
        instance = json.load(instance_file)
    existing_annotations.update(instance)
    syn.set_annotations(annotations=existing_annotations)
    sleep(4)

    # WHEN validating the annotation against the schema
    results = js.validate(synapse_folder)
    # THEN the annotations were valid
    assert results["isValid"] == is_valid
    # OR THEN the annotations were invalid
    if not results["isValid"]:
        # THEN the results contain "allValidationMessages"
        assert "allValidationMessages" in results
         # THEN the "allValidationMessages" match the expected messages
        assert sorted(results["allValidationMessages"]) == messages
