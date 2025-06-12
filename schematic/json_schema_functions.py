"""JSON Schema functions
This module includes functions for:
- uploading JSON Schemas to Synapse
- binding JSON Schemas to Synapse entities
- creating file views and wikis based on JSON Schemas

Several functions are deprecated. These are the ones that have to do with creating a fileview and
 wiki in Synapse. This functionality is only needed temporarily for Curator purposes while the
 Synapse platform creates this functionality internally. These will be sunsetted in the near future.

"""

from typing import Any, Optional
import warnings

from deprecated import deprecated
from synapseclient import Synapse  # type: ignore
from synapseclient.models import Column, ColumnType, ViewTypeMask, EntityView  # type: ignore
from synapseclient import Wiki  # type: ignore
from synapseclient.core.exceptions import SynapseHTTPError  # type: ignore

from schematic.schemas.create_json_schema import create_json_schema
from schematic.schemas.data_model_graph import create_dmge

TYPE_DICT = {
    "string": ColumnType.STRING,
    "number": ColumnType.DOUBLE,
    "integer": ColumnType.INTEGER,
    "boolean": ColumnType.BOOLEAN,
}

LIST_TYPE_DICT = {
    "string": ColumnType.STRING_LIST,
    "integer": ColumnType.INTEGER_LIST,
    "boolean": ColumnType.BOOLEAN_LIST,
}


@deprecated(reason="Entity view functionality is only need temporarily")
def create_json_schema_entity_view_and_wiki(  # pylint: disable=too-many-arguments
    syn: Synapse,
    data_model_path: str,
    datatype: str,
    synapse_org_name: str,
    synapse_entity_id: str,
    synapse_parent_id: str,
    schema_name: Optional[str] = None,
    entity_view_name: Optional[str] = None,
    wiki_title: Optional[str] = None,
    schema_version: str = "0.0.1",
) -> tuple[str, str]:
    """
    1. Creates a JSON Schema from the data model for the input datatype
    2. Uploads the JSON Schema to Synapse and binds it to the input entity id
    3. Creates a entity view with columns based on the JSON Schema
    4. Creates a wiki for the entity view

    This functionality is needed only temporarily. See note at top of module.

    Arguments:
        syn: A Synapse object thats been logged in
        data_model_path: A path to the data model used to create the JSON Schema
        datatype: The datatype in the data model to create the JSON Schema for
        synapse_org_name: The Synapse org to upload the JSON Schema to
        synapse_entity_id: The ID of the entity in Synapse to bind the JSON Schema to
        synapse_parent_id: The ID of the entity in Synapse to put the entity_view at
        schema_name: The name the created JSON Schema will have
        entity_view_name: The name the created entity view will have
        wiki_title: The title the created/updated wiki will have
        schema_version: The version the created JSON Schema will have

    Returns:
        The URI of the uploaded JSON Schema,
        the Synapse id of the created entity view
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    json_schema_uri = create_and_bind_json_schema(
        syn=syn,
        data_model_path=data_model_path,
        datatype=datatype,
        synapse_org_name=synapse_org_name,
        synapse_entity_id=synapse_entity_id,
        schema_name=schema_name,
        schema_version=schema_version,
    )

    entity_view_id = create_json_schema_entity_view(
        syn=syn,
        synapse_entity_id=synapse_entity_id,
        synapse_parent_id=synapse_parent_id,
        entity_view_name=entity_view_name,
    )
    create_or_update_wiki_with_entity_view(
        syn=syn,
        entity_view_id=entity_view_id,
        owner_id=synapse_entity_id,
        title=wiki_title,
    )
    return (json_schema_uri, entity_view_id)


def create_and_bind_json_schema(  # pylint: disable=too-many-arguments
    syn: Synapse,
    data_model_path: str,
    datatype: str,
    synapse_org_name: str,
    synapse_entity_id: str,
    schema_name: Optional[str] = None,
    schema_version: str = "0.0.1",
) -> str:
    """
    This function:
    - creates a json schema
    - uploads it to Synapse
    - binds it to a Synapse entity

    Arguments:
        syn: A Synapse object thats been logged in
        data_model_path: A path to the data model used to create the JSON Schema
        datatype: The datatype in the data model to create the JSON Schema for
        synapse_org_name: The Synapse org to upload the JSON Schema to
        synapse_entity_id: The ID of the entity in Synapse to bind the JSON Schema to
        schema_name: The name the created JSON Schema will have
        schema_version: The version the created JSON Schema will have

    Returns:
        The URI of the uploaded JSON Schema
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    js_service = syn.service("json_schema")
    dmge = create_dmge(data_model_path)

    if not schema_name:
        schema_name = f"{datatype}.schema"

    json_schema = create_json_schema(
        dmge=dmge,
        datatype=datatype,
        schema_name=schema_name,
        write_schema=False,
        use_property_display_names=False,
    )
    json_schema_uri = upload_json_schema(
        syn=syn,
        json_schema=json_schema,
        synapse_org_name=synapse_org_name,
        schema_name=schema_name,
        schema_version=schema_version,
    )

    js_service.bind_json_schema(json_schema_uri, synapse_entity_id)
    return json_schema_uri


def upload_json_schema(
    syn: Synapse,
    json_schema: dict[str, Any],
    synapse_org_name: str,
    schema_name: str,
    schema_version: str = "0.0.1",
) -> str:
    """
    1. Uploads a JSON Schema to Synapse
    2. Binds the JSON Schema to a Synapse entity

    Args:
        syn: A Synapse object thats been logged in
        json_schema: A JSON Schema in dict form
        synapse_org_name: The Synapse org to upload the JSON Schema to
        schema_name: The name the created JSON Schema will have
        schema_version: The version the created JSON Schema will have

    Returns:
        The URI for the JSON Schema
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    js_service = syn.service("json_schema")
    org = js_service.JsonSchemaOrganization(synapse_org_name)
    org.create_json_schema(json_schema, schema_name, schema_version)
    uri = f"{synapse_org_name}-{schema_name}-{schema_version}"
    return uri


@deprecated(reason="Entity view functionality is only need temporarily")
def create_json_schema_entity_view(
    syn: Synapse,
    synapse_entity_id: str,
    synapse_parent_id: str,
    entity_view_name: str = "JSON Schema view",
) -> str:
    """
    Creates A Synapse entity view based on a JSON Schema that is bound to a Synapse entity
    This functionality is needed only temporarily. See note at top of module.

    Args:
        syn: A Synapse object thats been logged in
        synapse_entity_id: The ID of the entity in Synapse to bind the JSON Schema to
        synapse_parent_id: The ID of the entity in Synapse to put the entity_view at
        entity_view_name: The name the crated entity view will have

    Returns:
        The Synapse id of the crated entity view
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    syn.get_available_services()
    js_service = syn.service("json_schema")
    json_schema = js_service.get_json_schema(synapse_entity_id)
    org = js_service.JsonSchemaOrganization(
        json_schema["jsonSchemaVersionInfo"]["organizationName"]
    )
    schema_version = js_service.JsonSchemaVersion(
        org,
        json_schema["jsonSchemaVersionInfo"]["schemaName"],
        json_schema["jsonSchemaVersionInfo"]["semanticVersion"],
    )
    columns = _create_columns_from_json_schema(schema_version.body)
    view = EntityView(
        name=entity_view_name,
        parent_id=synapse_parent_id,
        scope_ids=[synapse_entity_id],
        view_type_mask=ViewTypeMask.FILE,
        columns=columns,
    ).store(synapse_client=syn)
    view.reorder_column(name="createdBy", index=0)
    view.reorder_column(name="name", index=0)
    view.reorder_column(name="id", index=0)
    view.store(synapse_client=syn)
    return view.id


@deprecated(reason="Entity view functionality is only need temporarily")
def create_or_update_wiki_with_entity_view(
    syn: Synapse,
    entity_view_id: str,
    owner_id: str,
    title: Optional[str] = None,
) -> Wiki:
    """
    Creates or updates a Wiki for an entity if the wiki exists or not.
    An EntityView query is added to the wiki markdown

    This functionality is needed only temporarily. See note at top of module.

    Args:
        syn: A Synapse object thats been logged in
        entity_view_id: The Synapse id of the EntityView for the query
        owner_id: The ID of the entity in Synapse that the wiki will be created/updated
        title: The (new) title of the wiki to be created/updated

    Returns:
        The created Wiki object
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    entity = syn.get(owner_id)

    try:
        wiki = syn.getWiki(entity)
    except SynapseHTTPError:
        wiki = None
    if wiki:
        return update_wiki_with_entity_view(syn, entity_view_id, owner_id, title)
    return create_entity_view_wiki(syn, entity_view_id, owner_id, title)


@deprecated(reason="Entity view functionality is only need temporarily")
def create_entity_view_wiki(
    syn: Synapse,
    entity_view_id: str,
    owner_id: str,
    title: Optional[str] = None,
) -> Wiki:
    """
    Creates a wiki with a query of an entity view
    This functionality is needed only temporarily. See note at top of module.

    Args:
        syn: A Synapse object thats been logged in
        entity_view_id: The Synapse id of the entity view to make the wiki for
        owner_id: The ID of the entity in Synapse to put as owner of the wiki
        title: The title of the wiki to be created

    Returns:
        The created wiki object
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    content = (
        "${synapsetable?query=select %2A from "
        f"{entity_view_id}"
        "&showquery=false&tableonly=false}"
    )
    if title is None:
        title = "Entity View"
    wiki = Wiki(title=title, owner=owner_id, markdown=content)
    wiki = syn.store(wiki)
    return wiki


@deprecated(reason="Entity view functionality is only need temporarily")
def update_wiki_with_entity_view(
    syn: Synapse, entity_view_id: str, owner_id: str, title: Optional[str] = None
) -> Wiki:
    """
    Updates a wiki to include a query of an entity view
    This functionality is needed only temporarily. See note at top of module.

    Args:
        syn: A Synapse object thats been logged in
        entity_view_id: The Synapse id of the entity view to make the query for
        owner_id: The ID of the entity in Synapse to put as owner of the wiki
        title: The title of the wiki to be updated

    Returns:
        The created wiki object
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    entity = syn.get(owner_id)
    wiki = syn.getWiki(entity)

    new_content = (
        "\n"
        "${synapsetable?query=select %2A from "
        f"{entity_view_id}"
        "&showquery=false&tableonly=false}"
    )
    wiki.markdown = wiki.markdown + new_content
    if title:
        wiki.title = title

    syn.store(wiki)
    return wiki


def _create_columns_from_json_schema(json_schema: dict[str, Any]) -> list[Column]:
    """Creates a list of Synapse Columns based on the JSON Schema type

    Arguments:
        json_schema: The JSON Schema in dict form

    Raises:
        ValueError: If the JSON Schema has no properties
        ValueError: If the JSON Schema properties is not a dict

    Returns:
        A list of Synapse columns based on the JSON Schema
    """
    if "properties" not in json_schema:
        raise ValueError("JSON Schema does not have a properties field")
    properties = json_schema["properties"]
    if not isinstance(properties, dict):
        raise ValueError("JSON Schema properties is not a dictionary")
    columns = []
    for name, prop_schema in properties.items():
        column_type = _get_column_type_from_js_property(prop_schema)
        maximum_size = None
        if column_type == "STRING":
            maximum_size = 100
        if column_type in LIST_TYPE_DICT.values():
            maximum_size = 5

        column = Column(
            name=name,
            column_type=column_type,
            maximum_size=maximum_size,
            default_value=None,
        )
        columns.append(column)
    return columns


def _get_column_type_from_js_property(js_property: dict[str, Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema property.
    The JSON Schema should be valid but that should not be assumed.
    If the type can not be determined ColumnType.STRING will be returned.

    Args:
        js_property: A JSON Schema property in dict form.

    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    # Enums are always strings in Synapse tables
    if "enum" in js_property:
        return ColumnType.STRING
    if "type" in js_property:
        if js_property["type"] == "array":
            return _get_list_column_type_from_js_property(js_property)
        return TYPE_DICT.get(js_property["type"], ColumnType.STRING)
    # A oneOf list usually indicates that the type could be one or more different things
    if "oneOf" in js_property and isinstance(js_property["oneOf"], list):
        return _get_column_type_from_js_one_of_list(js_property["oneOf"])
    return ColumnType.STRING


def _get_column_type_from_js_one_of_list(js_one_of_list: list[Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema oneOf list.
    Items in the oneOf list should be dicts, but that should not be assumed.

    Args:
        js_one_of_list: A list of items to check for type

    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    # items in a oneOf list should be dicts
    items = [item for item in js_one_of_list if isinstance(item, dict)]
    # Enums are always strings in Synapse tables
    if [item for item in items if "enum" in item]:
        return ColumnType.STRING
    # For Synapse ColumnType we can ignore null types in JSON Schemas
    type_items = [item for item in items if "type" in item if item["type"] != "null"]
    if len(type_items) == 1:
        type_item = type_items[0]
        if type_item["type"] == "array":
            return _get_list_column_type_from_js_property(type_item)
        return TYPE_DICT.get(type_item["type"], ColumnType.STRING)
    return ColumnType.STRING


def _get_list_column_type_from_js_property(js_property: dict[str, Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema array property

    Args:
        js_property: A JSON Schema property in dict form.

    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    if "items" in js_property and isinstance(js_property["items"], dict):
        # Enums are always strings in Synapse tables
        if "enum" in js_property["items"]:
            return ColumnType.STRING_LIST
        if "type" in js_property["items"]:
            return LIST_TYPE_DICT.get(
                js_property["items"]["type"], ColumnType.STRING_LIST
            )

    return ColumnType.STRING_LIST
