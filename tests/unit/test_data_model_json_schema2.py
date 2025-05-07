from typing import Generator, Any

import pytest

from schematic.models.metadata import MetadataModel
from schematic.schemas.data_model_json_schema2 import (
    PropertyType,
    JSONSchema,
    DataModelJSONSchema2,
    _set_conditional_dependencies,
    _set_property2,
    _create_enum_array_property,
    _create_array_property,
    _create_enum_property,
    _create_simple_property,
    _get_type_from_validation_rules,
    _get_type_rules_from_rule_list,
)

from schematic.utils.types import JsonType

# pylint: disable=protected-access
# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments


@pytest.fixture(name="dm_json_schema")
def fixture_dm_json_schema() -> Generator[DataModelJSONSchema2, None, None]:
    """Yields a DataModelJSONSchema2 with the example data model"""
    metadata_model = MetadataModel(
        inputMModelLocation="tests/data/example.model.jsonld",
        inputMModelLocationType="local",
        data_model_labels="class_label",
    )
    data_model_js = DataModelJSONSchema2(
        jsonld_path=metadata_model.inputMModelLocation,
        graph=metadata_model.graph_data_model,
    )
    yield data_model_js


def test_json_schema():
    js = JSONSchema("id", "title")


def test_get_json_validation_schema(dm_json_schema: DataModelJSONSchema2) -> None:
    schema = dm_json_schema.get_json_validation_schema("Patient", "")
    # print(schema)
    assert False



@pytest.mark.parametrize(
    "reverse_dependencies, range_domain_map",
    [
        # If the input node has no reverse dependencies, nothing gets added
        ({"property_name": []}, {}),
        # If the input node has reverse dependencies,
        #  but none of them are in the range domain map, nothing gets added
        ({"property_name": ["enum1"]}, {}),
        # If the input node has any reverse dependencies,
        #  and atleast one of them are in the range domain map,
        #  but the range domain map is empty for that node, nothing gets added
        ({"property_name": ["enum1"]}, {"enum1": []}),
    ],
)
def test_set_conditional_dependencies_nothing_added(
    reverse_dependencies: dict[str, list[str]],
    range_domain_map: dict[str, list[str]],
) -> None:
    """
    Tests for _set_conditional_dependencies
      were the schema doesn't change
    """
    json_schema = {"allOf": []}
    _set_conditional_dependencies(
        json_schema=json_schema,
        conditional_property="property_name",
        reverse_dependencies=reverse_dependencies,
        range_domain_map=range_domain_map,
    )
    assert json_schema == {"allOf": []}



@pytest.mark.parametrize(
    "reverse_dependencies, range_domain_map, expected_schema",
    [
        (
            {"property_name": ["enum1"]},
            {"enum1": ["rev_dep_property1"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {
                            "properties": {"rev_dep_property1": {"enum": ["enum1"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
                        },
                    }
                ]
            ),
        ),
        (
            {"property_name": ["enum1"]},
            {"enum1": ["rev_dep_property1", "rev_dep_property2"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {
                            "properties": {"rev_dep_property1": {"enum": ["enum1"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
                        },
                    },
                    {
                        "if": {
                            "properties": {"rev_dep_property2": {"enum": ["enum1"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
                        },
                    },
                ]
            ),
        ),
        (
            {"property_name": ["enum1", "enum2"]},
            {"enum1": ["rev_dep_property1"], "enum2": ["rev_dep_property2"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {
                            "properties": {"rev_dep_property1": {"enum": ["enum1"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
                        },
                    },
                    {
                        "if": {
                            "properties": {"rev_dep_property2": {"enum": ["enum2"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
                        },
                    },
                ]
            ),
        ),
    ],
)
def test_set_conditional_dependencies(
    reverse_dependencies: dict[str, list[str]],
    range_domain_map: dict[str, list[str]],
    expected_schema: dict[str, Any],
) -> None:
    """Tests for _set_conditional_dependencies"""
    json_schema = JSONSchema()
    _set_conditional_dependencies(
        json_schema=json_schema,
        conditional_property="property_name",
        reverse_dependencies=reverse_dependencies,
        range_domain_map=range_domain_map,
    )
    assert json_schema == expected_schema


@pytest.mark.parametrize(
    "schema, enum_list, property_type, is_required, expected_schema",
    [
        # enum_list is not empty, and property_type.is_array is True, is_required is True
        # The property should be an array with an enum
        # required list should have "property_name"
        (
            JSONSchema(),
            ["enum1"],
            PropertyType(is_array=True),
            True,
            JSONSchema(
                properties={
                    "property_name": {
                        "oneOf": [
                            {
                                "type": "array",
                                "items": {"enum": ["enum1"]},
                            },
                        ]
                    }
                },
                required=["property_name"],
            ),
        ),
        # enum_list is not empty, and property_type.is_array is True, is_required is False
        # The property should be an array with an enum
        # required list should be empty
        (
            JSONSchema(),
            ["enum1"],
            PropertyType(is_array=True),
            False,
            JSONSchema(
                properties={
                    "property_name": {
                        "oneOf": [
                            {
                                "type": "array",
                                "items": {"enum": ["enum1"]},
                            },
                            {
                                "type": "null",
                            },
                        ]
                    }
                },
                required=[],
            ),
        ),
        # enum_list is not empty, and property_type.is_array is False
        # The property should be an enum
        (
            JSONSchema(),
            ["enum1"],
            PropertyType(),
            False,
            JSONSchema(
                properties={"property_name": {"enum": ["enum1", None]}},
                required=[],
            ),
        ),
        # enum_list is empty and and property_type.is_array is True
        # The property should be an array
        (
            JSONSchema(),
            [],
            PropertyType(is_array=True),
            False,
            JSONSchema(
                properties={
                    "property_name": {"oneOf": [{"type": "array"}, {"type": "null"}]}
                },
                required=[],
            ),
        ),
        # enum_list is empty and property_type.is_array is False
        # The property should be neither an array or enum
        (
            JSONSchema(),
            [],
            PropertyType(),
            False,
            JSONSchema(
                properties={"property_name": {}},
                required=[],
            ),
        ),
    ],
)
def test_set_property2(
    schema: JSONSchema,
    enum_list: list[str],
    expected_schema: dict[str, Any],
    property_type: PropertyType,
    is_required: bool,
) -> None:
    """Tests for set_property2"""
    _set_property2(
        json_schema=schema,
        name="property_name",
        enum_list=enum_list,
        property_type=property_type,
        is_required=is_required,
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "enum_list, is_required, expected_schema",
    [
        (
            ["enum1"],
            True,
            {"name": {"oneOf": [{"type": "array", "items": {"enum": ["enum1"]}}]}},
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            ["enum1"],
            False,
            {
                "name": {
                    "oneOf": [
                        {"type": "array", "items": {"enum": ["enum1"]}},
                        {"type": "null"},
                    ]
                }
            },
        ),
    ],
)
def test_create_enum_array_property(
    enum_list: list[str],
    is_required: bool,
    expected_schema: JsonType,
) -> None:
    """Test for _create_enum_array_property"""
    schema = _create_enum_array_property(
        name="name",
        enum_list=enum_list,
        is_required=is_required,
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "item_type, is_required, expected_schema",
    [
        (None, True, {"name": {"oneOf": [{"type": "array"}]}}),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (None, False, {"name": {"oneOf": [{"type": "array"}, {"type": "null"}]}}),
        # If item_type is given, it is set in the schema
        (
            "string",
            True,
            {"name": {"oneOf": [{"type": "array", "items": {"type": "string"}}]}},
        ),
    ],
)
def test_create_array_property(
    item_type: None | str,
    is_required: bool,
    expected_schema: JsonType,
) -> None:
    """Test for _create_array_property"""
    schema = _create_array_property(
        name="name", item_type=item_type, is_required=is_required
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "enum_list, is_required, expected_schema",
    [
        ([], True, {"name": {"enum": []}}),
        (["enum1"], True, {"name": {"enum": ["enum1"]}}),
        (["enum1", "enum2"], True, {"name": {"enum": ["enum1", "enum2"]}}),
        # If is is_required is False, None is added to the enum_list
        ([], False, {"name": {"enum": [None]}}),
        (["enum1"], False, {"name": {"enum": ["enum1", None]}}),
    ],
)
def test_create_enum_property(
    enum_list: list[str],
    is_required: bool,
    expected_schema: JsonType,
) -> None:
    """Test for _create_enum_property"""
    schema = _create_enum_property(
        enum_list=enum_list, name="name", is_required=is_required
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "property_type, is_required, expected_schema",
    [
        (None, False, {"name": {}}),
        # If property_type is given, it is added to the schema
        ("string", True, {"name": {"type": "string"}}),
        # If property_type is given, and is_required is False,
        # type is set to given property_type and "null"
        ("string", False, {"name": {"type": ["string", "null"]}}),
        # If is_required is True '"not": {"type":"null"}' is added to schema if
        # property_type is not given
        (None, True, {"name": {"not": {"type": "null"}}}),
    ],
)
def test_create_simple_property(
    property_type: str | None,
    is_required: bool,
    expected_schema: JsonType,
) -> None:
    """Test for _create_simple_property"""
    schema = _create_simple_property(
        "name",
        property_type,
        is_required,
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "validation_rules, expected_type",
    [
        # If there are no type validation rules the property_type is None
        ([], PropertyType()),
        (["xxx"], PropertyType()),
        # if there are more than one validation rules found property_type is None
        (["str", "bool"], PropertyType()),
        # If there is one type validation rule the property_type is set to the
        #  JSON Schema equivalent of the validation rule
        (["str"], PropertyType("string")),
        (["bool"], PropertyType("boolean")),
        # If there are any list type validation rules the property_type is set to "array"
        (["list like"], PropertyType(is_array=True)),
        (["list strict"], PropertyType(is_array=True)),
        (["list::regex"], PropertyType(is_array=True)),
        # If there are any list type validation rules and one type validation rule
        #  the property_type is set to "array", and the item_type is set to the
        #  JSON Schema equivalent of the validation rule
        (["list::regex", "str"], PropertyType("string", is_array=True)),
    ],
)
def test_get_type_from_validation_rules(
    validation_rules: list[str], expected_type: PropertyType
) -> None:
    """Test for _get_type_from_validation_rules"""
    result = _get_type_from_validation_rules(validation_rules)
    assert result == expected_type


@pytest.mark.parametrize(
    "input_rules, expected_rules",
    [
        ([], []),
        (["list strict"], []),
        (["str"], ["str"]),
        (["str error"], ["str"]),
        (["str error", "int warning"], ["str", "int"]),
        (["str error", "int warning", "list strict"], ["str", "int"]),
    ],
)
def test_get_type_rules_from_rule_list(
    input_rules: list[str],
    expected_rules: list[str],
) -> None:
    """Test for _get_type_rules_from_rule_list"""
    result = _get_type_rules_from_rule_list(input_rules)
    assert result == expected_rules
