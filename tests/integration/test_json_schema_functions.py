"""Integration tests for JSON Schema functions"""

import uuid

import pytest
from synapseclient.client import Synapse
from synapseclient.models import EntityView, ColumnType, Project, Folder
from synapseclient import Wiki


from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.create_json_schema import create_json_schema
from schematic.json_schema_functions import (
    create_json_schema_entity_view_and_wiki,
    create_and_bind_json_schema,
    upload_json_schema,
    create_json_schema_entity_view,
    create_or_update_wiki_with_entity_view,
)
from tests.utils import CleanupItem

SCHEMA_TEST_ORG = "dpetest"
SCHEMA_TEST_VERSION = "0.0.1"
TEST_COMPONENT_URI = "dpetest-test.schematic.MockComponent-0.0.3"

MOCK_COMPONENT_SYNAPSE_COLUMNS = {
    "CheckAges": ColumnType.STRING,
    "CheckDate": ColumnType.STRING,
    "CheckFloat": ColumnType.DOUBLE,
    "CheckInt": ColumnType.INTEGER,
    "CheckList": ColumnType.STRING_LIST,
    "CheckListEnum": ColumnType.STRING_LIST,
    "CheckListEnumStrict": ColumnType.STRING_LIST,
    "CheckListLike": ColumnType.STRING_LIST,
    "CheckListLikeEnum": ColumnType.STRING_LIST,
    "CheckListStrict": ColumnType.STRING_LIST,
    "CheckMatchExactly": ColumnType.STRING,
    "CheckMatchExactlyvalues": ColumnType.STRING,
    "CheckMatchNone": ColumnType.STRING,
    "CheckMatchNonevalues": ColumnType.STRING,
    "CheckMatchatLeast": ColumnType.STRING,
    "CheckMatchatLeastvalues": ColumnType.STRING,
    "CheckNA": ColumnType.INTEGER,
    "CheckNum": ColumnType.DOUBLE,
    "CheckRange": ColumnType.DOUBLE,
    "CheckRecommended": ColumnType.STRING,
    "CheckRegexFormat": ColumnType.STRING,
    "CheckRegexInteger": ColumnType.STRING,
    "CheckRegexList": ColumnType.STRING_LIST,
    "CheckRegexListLike": ColumnType.STRING_LIST,
    "CheckRegexListStrict": ColumnType.STRING_LIST,
    "CheckRegexSingle": ColumnType.STRING,
    "CheckString": ColumnType.STRING,
    "CheckURL": ColumnType.STRING,
    "CheckUnique": ColumnType.STRING,
    "Component": ColumnType.STRING,
}

ENTITY_VIEW_COLUMNS = [
    "benefactorId",
    "createdBy",
    "createdOn",
    "currentVersion",
    "dataFileBucket",
    "dataFileConcreteType",
    "dataFileHandleId",
    "dataFileKey",
    "dataFileMD5Hex",
    "dataFileName",
    "dataFileSizeBytes",
    "description",
    "etag",
    "id",
    "modifiedBy",
    "modifiedOn",
    "name",
    "parentId",
    "path",
    "projectId",
    "type",
]


@pytest.fixture(name="synapse_project", scope="function")
def fixture_synapse_project(syn: Synapse, schedule_for_cleanup) -> tuple[str, str]:
    """This returns Synapse ids for a created Synapse project and a folder created in the project"""
    project = Project(name=f"test_json_schemas_{str(uuid.uuid4())}")
    project = project.store(synapse_client=syn)

    folder_name = f"{str(uuid.uuid4())}"
    folder = Folder(name=folder_name, parent_id=project.id)
    folder.store(synapse_client=syn)

    schedule_for_cleanup(CleanupItem(synapse_id=project.id))

    return project.id, folder.id


@pytest.fixture(name="schema_name", scope="function")
def fixture_schema_name() -> str:
    """This returns a randomized Synapse valid schema name"""
    schema_id = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
    return f"test.schematic.{schema_id}"


def test_create_json_schema_entity_view_and_wiki(
    syn: Synapse, synapse_project: str, schema_name: str
) -> None:
    """
    Test for create_json_schema_entity_view_and_wiki
    This tests that:
    - the JSON Schema is uploaded
    - the fileview columns are the appropriate type
    - the wiki is created
    """
    js = syn.service("json_schema")
    # GIVEN a Synapse Project with a Folder
    _, folder_id = synapse_project
    # WHEN creating a JSON Schema fileview and wiki
    try:
        _, fileview_id = create_json_schema_entity_view_and_wiki(
            syn=syn,
            data_model_path="tests/data/example.model.csv",
            datatype="MockComponent",
            synapse_org_name=SCHEMA_TEST_ORG,
            synapse_entity_id=folder_id,
            schema_name=schema_name,
        )
        # THEN the schema should be getable from the folder
        json_schema = js.get_json_schema(folder_id)
    finally:
        js.unbind_json_schema(folder_id)
        js.delete_json_schema(f"{SCHEMA_TEST_ORG}-{schema_name}")
    # AND the schema URI should have the given schema name
    uri = f"{SCHEMA_TEST_ORG}-{schema_name}-{SCHEMA_TEST_VERSION}"
    assert uri == json_schema["jsonSchemaVersionInfo"]["$id"]
    # AND the fileview was created
    view = EntityView(id=fileview_id).get(synapse_client=syn)
    # AND the fileview's column types should match the JSON Schema types as well as possible
    column_types = {k: v.column_type for (k, v) in view.columns.items()}
    for item in ENTITY_VIEW_COLUMNS:
        column_types.pop(item)
    assert column_types == MOCK_COMPONENT_SYNAPSE_COLUMNS
    folder = syn.get(folder_id)
    # AND the wiki exists
    wiki = syn.getWiki(folder)
    # AND the wiki owner is the folder
    assert wiki.ownerId == folder_id
    # AND the wiki markdown includes the fileview
    assert wiki.markdown == (
        "${synapsetable?query=select %2A from "
        f"{fileview_id}"
        "&showquery=false&tableonly=false}"
    )


def test_create_and_bind_json_schema(
    syn: Synapse, synapse_project: str, schema_name: str
) -> None:
    """
    Test for create_and_bind_json_schema
    Tests that
    - the JSON Schema can be gotten form the folder its bound to
    - the bound JSON Schema has the correct id
    """
    js = syn.service("json_schema")
    # GIVEN a Synapse Project with a Folder
    _, folder_id = synapse_project
    try:
        # WHEN the schema is uploaded and bound to the Synapse folder
        json_schema_uri = create_and_bind_json_schema(
            syn=syn,
            data_model_path="tests/data/example.model.csv",
            datatype="MockComponent",
            synapse_org_name=SCHEMA_TEST_ORG,
            synapse_entity_id=folder_id,
            schema_name=schema_name,
            schema_version=SCHEMA_TEST_VERSION,
        )
        # THEN the schema should be getable from the folder
        json_schema = js.get_json_schema(folder_id)
    finally:
        js.unbind_json_schema(folder_id)
        js.delete_json_schema(f"{SCHEMA_TEST_ORG}-{schema_name}")
    # AND the schema URI should have the given schema name
    uri = f"{SCHEMA_TEST_ORG}-{schema_name}-{SCHEMA_TEST_VERSION}"
    assert uri == json_schema["jsonSchemaVersionInfo"]["$id"]
    assert uri == json_schema_uri


def test_upload_json_schema(
    syn: Synapse, dmge: DataModelGraphExplorer, schema_name: str
) -> None:
    """
    Test for upload_json_schema
    Tests that
    - the JSON schema is uploaded
    - the URI for the uploaded JSON Schema is correct
    """
    js = syn.service("json_schema")
    # GIVEN a JSON Schema
    json_schema = create_json_schema(
        dmge=dmge, datatype="MockComponent", schema_name="", write_schema=False
    )
    try:
        # WHEN the schema is uploaded
        json_schema_uri = upload_json_schema(
            syn=syn,
            json_schema=json_schema,
            synapse_org_name=SCHEMA_TEST_ORG,
            schema_name=schema_name,
            schema_version=SCHEMA_TEST_VERSION,
        )
        # THEN it should be downloadable
        js.get_json_schema_body(json_schema_uri)
    finally:
        js.delete_json_schema(f"{SCHEMA_TEST_ORG}-{schema_name}")
    # AND the schema URI should have the given schema name
    uri = f"{SCHEMA_TEST_ORG}-{schema_name}-{SCHEMA_TEST_VERSION}"
    assert uri == json_schema_uri


def test_create_json_schema_entity_view(syn: Synapse, synapse_project: str) -> None:
    """
    Test for create_json_schema_entity_view
    Tests that the crated fileview has the appropriate column types based on the JSON Schema
    """
    js = syn.service("json_schema")
    # GIVEN a Synapse Project with a Folder
    _, folder_id = synapse_project
    # WHEN the folder has a JSON Schema bound to it
    js.bind_json_schema(TEST_COMPONENT_URI, folder_id)
    view_id = None
    # WHEN creating a fileview from it
    view_id = create_json_schema_entity_view(syn=syn, synapse_entity_id=folder_id)
    view = EntityView(id=view_id).get(synapse_client=syn)
    # THEN the fileview's column types should match the JSON Schema types as well as possible
    column_types = {k: v.column_type for (k, v) in view.columns.items()}
    for item in ENTITY_VIEW_COLUMNS:
        column_types.pop(item)
    assert column_types == MOCK_COMPONENT_SYNAPSE_COLUMNS


def test_create_or_update_wiki_with_entity_view_no_existing_wiki(
    syn: Synapse, synapse_project: str
) -> None:
    """
    Test for create_or_update_wiki_with_entity_view when no wiki exists for the folder
    Tests that:
    - the markdown includes a query of the fileview
    - the title was added
    """
    _, folder_id = synapse_project
    wiki = create_or_update_wiki_with_entity_view(syn, "syn1", folder_id, "test_title")
    assert wiki.markdown == (
        "${synapsetable?query=select %2A from syn1&showquery=false&tableonly=false}"
    )
    assert wiki.title == "test_title"


def test_create_or_update_wiki_with_entity_view_with_existing_wiki(
    syn: Synapse, synapse_project: str
) -> None:
    """
    Test for create_or_update_wiki_with_entity_view when a wiki exists for the folder
    Tests that:
    - the a query of the fileview was added to the markdown
    - the title was changed
    """
    _, folder_id = synapse_project
    old_wiki = Wiki(title="old_title", owner=folder_id, markdown="old_content")
    syn.store(old_wiki)
    new_wiki = create_or_update_wiki_with_entity_view(
        syn, "syn1", folder_id, "new_title"
    )
    assert new_wiki.markdown == (
        "old_content\n${synapsetable?query=select %2A from syn1&showquery=false&tableonly=false}"
    )
    assert new_wiki.title == "new_title"
