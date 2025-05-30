"""
This tests the function create_json_schema.
This includes:
- creating the schema
- uploading to Synapse
- binding the schema to a Synapse Folder
 -validation annotations on the folder
"""

from typing import Any
import json
import uuid
from time import sleep

from synapseclient import Folder
from synapseclient.client import Synapse
import pytest

from schematic.schemas.create_json_schema import (
    create_json_schema,
)

from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_parser import DataModelParser

SCHEMA_TEST_ORG = "dpetest"
SCHEMA_TEST_VERSION = "0.0.1"


@pytest.fixture(name="dmge", scope="module")
def fixture_dmge() -> DataModelGraphExplorer:
    """
    This returns a DataModelGraphExplorer instance, using the example data model.
    This has a module scope.
    This allows for it to an input to other module-scoped fixtures.
    """
    data_model_parser = DataModelParser(
        path_to_data_model="tests/data/example.model.csv"
    )
    parsed_data_model = data_model_parser.parse_model()
    data_model_graph = DataModelGraph(
        parsed_data_model, data_model_labels="class_label"
    )
    graph_data_model = data_model_graph.graph
    return DataModelGraphExplorer(graph_data_model)


@pytest.fixture(name="biospecimen_json_schema", scope="module")
def fixture_biospecimen_json_schema(
    synapse_module_scope: Synapse,
    dmge: DataModelGraphExplorer,
    request,
) -> str:
    """This creates a JSON Schema, uploads it to Synapse, then returns the uri"""
    js = synapse_module_scope.service("json_schema")

    schema = create_json_schema(
        dmge=dmge,
        datatype="Biospecimen",
        schema_name="Biospecimen_validation",
        write_schema=False,
        use_property_display_names=False,
    )
    schema_name = upload_schema_to_synapse(schema, synapse_module_scope)

    def delete_schema():
        js.delete_json_schema(f"{SCHEMA_TEST_ORG}-{schema_name}")

    request.addfinalizer(delete_schema)

    uri = f"{SCHEMA_TEST_ORG}-{schema_name}-{SCHEMA_TEST_VERSION}"
    return uri


@pytest.fixture(name="bulk_rna_json_schema", scope="module")
def fixture_bulk_rna_json_schema(
    synapse_module_scope: Synapse,
    dmge: DataModelGraphExplorer,
    request,
) -> str:
    """This creates a JSON Schema, uploads it to Synapse, then returns the uri"""
    js = synapse_module_scope.service("json_schema")

    schema = create_json_schema(
        dmge=dmge,
        datatype="BulkRNA-seqAssay",
        schema_name="BulkRNA-seqAssay_validation",
        write_schema=False,
        use_property_display_names=False,
    )
    schema_name = upload_schema_to_synapse(schema, synapse_module_scope)

    def delete_schema():
        js.delete_json_schema(f"{SCHEMA_TEST_ORG}-{schema_name}")

    request.addfinalizer(delete_schema)

    uri = f"{SCHEMA_TEST_ORG}-{schema_name}-{SCHEMA_TEST_VERSION}"
    return uri


@pytest.fixture(name="mock_component_json_schema", scope="module")
def fixture_mock_component_json_schema(
    synapse_module_scope: Synapse,
    dmge: DataModelGraphExplorer,
    request,
) -> str:
    """This creates a JSON Schema, uploads it to Synapse, then returns the uri"""
    js = synapse_module_scope.service("json_schema")

    schema = create_json_schema(
        dmge=dmge,
        datatype="MockComponent",
        schema_name="MockComponent_validation",
        write_schema=False,
        use_property_display_names=False,
    )
    schema_name = upload_schema_to_synapse(schema, synapse_module_scope)

    def delete_schema():
        js.delete_json_schema(f"{SCHEMA_TEST_ORG}-{schema_name}")

    request.addfinalizer(delete_schema)

    uri = f"{SCHEMA_TEST_ORG}-{schema_name}-{SCHEMA_TEST_VERSION}"
    return uri


@pytest.fixture(name="mock_filename_json_schema", scope="module")
def fixture_mock_filename_json_schema(
    synapse_module_scope: Synapse,
    dmge: DataModelGraphExplorer,
    request,
) -> str:
    """This creates a JSON Schema, uploads it to Synapse, then returns the uri"""
    js = synapse_module_scope.service("json_schema")

    schema = create_json_schema(
        dmge=dmge,
        datatype="MockFilename",
        schema_name="MockFilename_validation",
        write_schema=False,
        use_property_display_names=False,
    )
    schema_name = upload_schema_to_synapse(schema, synapse_module_scope)

    def delete_schema():
        js.delete_json_schema(f"{SCHEMA_TEST_ORG}-{schema_name}")

    request.addfinalizer(delete_schema)

    uri = f"{SCHEMA_TEST_ORG}-{schema_name}-{SCHEMA_TEST_VERSION}"
    return uri


@pytest.fixture(name="mock_rdb_json_schema", scope="module")
def fixture_mock_rdb_json_schema(
    synapse_module_scope: Synapse,
    dmge: DataModelGraphExplorer,
    request,
) -> str:
    """This creates a JSON Schema, uploads it to Synapse, then returns the uri"""
    js = synapse_module_scope.service("json_schema")

    schema = create_json_schema(
        dmge=dmge,
        datatype="MockRDB",
        schema_name="MockRDB_validation",
        write_schema=False,
        use_property_display_names=False,
    )
    schema_name = upload_schema_to_synapse(schema, synapse_module_scope)

    def delete_schema():
        js.delete_json_schema(f"{SCHEMA_TEST_ORG}-{schema_name}")

    request.addfinalizer(delete_schema)

    uri = f"{SCHEMA_TEST_ORG}-{schema_name}-{SCHEMA_TEST_VERSION}"
    return uri


@pytest.fixture(name="patient_json_schema", scope="module")
def fixture_patient_json_schema(
    synapse_module_scope: Synapse,
    dmge: DataModelGraphExplorer,
    request,
) -> str:
    """This creates a JSON Schema, uploads it to Synapse, then returns the uri"""
    js = synapse_module_scope.service("json_schema")

    schema = create_json_schema(
        dmge=dmge,
        datatype="Patient",
        schema_name="Patient_validation",
        write_schema=False,
        use_property_display_names=False,
    )
    schema_name = upload_schema_to_synapse(
        schema,
        synapse_module_scope,
    )

    def delete_schema():
        js.delete_json_schema(f"{SCHEMA_TEST_ORG}-{schema_name}")

    request.addfinalizer(delete_schema)

    uri = f"{SCHEMA_TEST_ORG}-{schema_name}-{SCHEMA_TEST_VERSION}"
    return uri


@pytest.fixture(name="synapse_folder", scope="function")
def fixture_synapse_folder(syn: Synapse, request) -> str:
    """This returns a Synapse id for a created Synapse folder"""
    js = syn.service("json_schema")
    folder_name = f"test_json_schema_validation_{str(uuid.uuid4())}"
    folder = Folder(name=folder_name, parent="syn23643250")
    folder = syn.store(obj=folder)

    def delete_folder():
        js.unbind_json_schema(folder.id)
        syn.delete(folder.id)

    request.addfinalizer(delete_folder)

    return folder.id


def upload_schema_to_synapse(schema: dict[str, Any], syn: Synapse) -> str:
    """Uploads a JSON Schema file to Synapse

    Arguments:
        schema: The schema to upload
        syn: A Synapse instance that's been logged in

    Returns:
        The name of the Schema in synapse
    """
    # Create a unique id for the schema that is only characters
    schema_id = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
    schema_name = f"test.schematic.{schema_id}"
    js = syn.service("json_schema")
    org = js.JsonSchemaOrganization(SCHEMA_TEST_ORG)
    org.create_json_schema(schema, schema_name, SCHEMA_TEST_VERSION)
    return schema_name


@pytest.mark.single_process_execution
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


@pytest.mark.single_process_execution
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
        "Patient:invalid; CancerType and FamilyHistory required when Diagnosis==Cancer",
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

    # AND annotating the folder with a JSON instance of the datatype
    existing_annotations = syn.get_annotations(synapse_folder)
    with open(instance_path, encoding="utf-8") as instance_file:
        instance = json.load(instance_file)
    existing_annotations.update(instance)
    syn.set_annotations(annotations=existing_annotations)
    sleep(4)

    # AND validating the annotation against the schema
    results = js.validate(synapse_folder)
    # THEN the annotations were valid
    assert results["isValid"] == is_valid
    # OR THEN the annotations were invalid
    if not results["isValid"]:
        # AND the results contain "allValidationMessages"
        assert "allValidationMessages" in results
        # AND the "allValidationMessages" match the expected messages
        assert sorted(results["allValidationMessages"]) == messages
