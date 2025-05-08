from typing import Generator, Any

import pytest

from schematic.models.metadata import MetadataModel
from schematic.schemas.data_model_json_schema2 import (
    PropertyData,
    JSONSchema,
    DataModelJSONSchema2,
    _set_conditional_dependencies,
    _set_property,
    _create_enum_array_property,
    _create_array_property,
    _create_enum_property,
    _create_simple_property,
    _get_property_data_from_validation_rules,
    _get_ranges_from_range_rule,
    _get_in_range_rule_from_rule_list,
    _get_type_rule_from_rule_list,
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
    JSONSchema("id", "title")


def test_get_json_validation_schema(dm_json_schema: DataModelJSONSchema2) -> None:
    dm_json_schema.get_json_validation_schema("Patient", "")
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
    "schema, enum_list, validation_rules, is_required, expected_schema",
    [
        # enum_list is not empty, validation rules contain a list rule, is_required is True
        # The property should be an array with an enum
        # required list should have "property_name"
        (
            JSONSchema(),
            ["enum1"],
            ["list"],
            True,
            JSONSchema(
                properties={
                    "property_name": {
                        "description":"TBD",
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
        # enum_list is not empty, validation rules contain a list rule, is_required is False
        # The property should be an array with an enum
        # required list should be empty
        (
            JSONSchema(),
            ["enum1"],
            ["list"],
            False,
            JSONSchema(
                properties={
                    "property_name": {
                        "description":"TBD",
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
        # enum_list is not empty, validation rules do not contain a list rule
        # The property should be an enum
        (
            JSONSchema(),
            ["enum1"],
            [],
            False,
            JSONSchema(
                properties={"property_name": {"enum": ["enum1", None], "description":"TBD"}},
                required=[],
            ),
        ),
        # enum_list is empty, validation rules do contain a list rule
        # The property should be an array
        (
            JSONSchema(),
            [],
            ["list"],
            False,
            JSONSchema(
                properties={
                    "property_name": {"oneOf": [{"type": "array"}, {"type": "null"}], "description":"TBD"}
                },
                required=[],
            ),
        ),
        # enum_list is empty, validation rules do not contain a list rule
        # The property should be neither an array or enum
        (
            JSONSchema(),
            [],
            [],
            False,
            JSONSchema(
                properties={"property_name": {"description":"TBD"}},
                required=[],
            ),
        ),
    ],
)
def test_set_property(
    schema: JSONSchema,
    enum_list: list[str],
    expected_schema: dict[str, Any],
    validation_rules: list[str],
    is_required: bool,
) -> None:
    """Tests for set_property"""
    _set_property(
        json_schema=schema,
        name="property_name",
        enum_list=enum_list,
        validation_rules=validation_rules,
        is_required=is_required,
        description="TBD"
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "enum_list, is_required, expected_schema",
    [
        (
            ["enum1"],
            True,
            {"name": {"description":"TBD", "oneOf": [{"type": "array", "items": {"enum": ["enum1"]}}]}},
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            ["enum1"],
            False,
            {
                "name": {
                    "description":"TBD",
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
        description="TBD"
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "property_data, is_required, expected_schema",
    [
        (PropertyData(is_array=True), True, {"name": {"description":"TBD", "oneOf": [{"type": "array"}]}}),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            PropertyData(is_array=True),
            False,
            {"name": {"oneOf": [{"type": "array"}, {"type": "null"}], "description":"TBD"}}
        ),
        # If item_type is given, it is set in the schema
        (
            PropertyData("string", is_array=True),
            True,
            {"name": {"oneOf": [{"type": "array", "items": {"type": "string"}}], "description":"TBD"}},
        ),
        # If property_data has range_min or range_max, they are set in the schema
        (
            PropertyData("number", is_array=True, range_min=0, range_max=1),
            True,
            {"name":{
                "description":"TBD",
                "oneOf": [{"type": "array", "items": {"type": "number", "minimum":0, "maximum":1}}],
            }}
        ),
    ],
)
def test_create_array_property(
    property_data: PropertyData,
    is_required: bool,
    expected_schema: JsonType,
) -> None:
    """Test for _create_array_property"""
    schema = _create_array_property(
        name="name", property_data=property_data, is_required=is_required, description="TBD"
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "enum_list, is_required, expected_schema",
    [
        ([], True, {"name": {"enum": [], "description":"TBD"}}),
        (["enum1"], True, {"name": {"enum": ["enum1"], "description":"TBD"}}),
        (["enum1", "enum2"], True, {"name": {"enum": ["enum1", "enum2"], "description":"TBD"}}),
        # If is is_required is False, None is added to the enum_list
        ([], False, {"name": {"enum": [None], "description":"TBD"}}),
        (["enum1"], False, {"name": {"enum": ["enum1", None], "description":"TBD"}}),
    ],
)
def test_create_enum_property(
    enum_list: list[str],
    is_required: bool,
    expected_schema: JsonType,
) -> None:
    """Test for _create_enum_property"""
    schema = _create_enum_property(
        enum_list=enum_list, name="name", is_required=is_required, description="TBD"
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "property_data, is_required, expected_schema",
    [
        (PropertyData(), False, {"name": {"description":"TBD"}}),
        # If property_type is given, it is added to the schema
        (PropertyData("string"), True, {"name": {"type": "string", "description":"TBD"}}),
        # If property_type is given, and is_required is False,
        # type is set to given property_type and "null"
        (PropertyData("string"), False, {"name": {"type": ["string", "null"], "description":"TBD"}}),
        # If is_required is True '"not": {"type":"null"}' is added to schema if
        # property_type is not given
        (PropertyData(), True, {"name": {"not": {"type": "null"}, "description":"TBD"}}),
        (
            PropertyData(property_type="number", range_min=0, range_max=1),
            True,
            {"name": {"type": "number", "minimum": 0, "maximum":1, "description":"TBD"}}
        )
    ],
)
def test_create_simple_property(
    property_data: PropertyData,
    is_required: bool,
    expected_schema: JsonType,
) -> None:
    """Test for _create_simple_property"""
    schema = _create_simple_property(
        "name",
        property_data,
        is_required,
        description="TBD"
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "validation_rules, expected_type",
    [
        # If there are no type validation rules the property_type is None
        ([], PropertyData()),
        (["xxx"], PropertyData()),
        # If there is one type validation rule the property_type is set to the
        #  JSON Schema equivalent of the validation rule
        (["str"], PropertyData("string")),
        (["bool"], PropertyData("boolean")),
        # If there are any list type validation rules the property_type is set to "array"
        (["list like"], PropertyData(is_array=True)),
        (["list strict"], PropertyData(is_array=True)),
        (["list::regex"], PropertyData(is_array=True)),
        # If there are any list type validation rules and one type validation rule
        #  the property_type is set to "array", and the item_type is set to the
        #  JSON Schema equivalent of the validation rule
        (["list::regex", "str"], PropertyData("string", is_array=True)),
        # If there are any inRange rules the min and max will be set
        (["inRange 0 1", "int"], PropertyData("integer", range_min=0, range_max=1)),
        # If there are any inRange rules but no type rule, or a non-numeric type rule
        #  the type will be set to number
        (["inRange 0 1"], PropertyData("number", range_min=0, range_max=1)),
        (["inRange 0 1", "str"], PropertyData("number", range_min=0, range_max=1)),
    ],
)
def test_get_property_data_from_validation_rules(
    validation_rules: list[str], expected_type: PropertyData
) -> None:
    """Test for _get_property_data_from_validation_rules"""
    result = _get_property_data_from_validation_rules(validation_rules)
    assert result == expected_type


@pytest.mark.parametrize(
    "input_rule, expected_tuple",
    [
        ("", (None, None)),
        ("inRange", (None, None)),
        ("inRange x x", (None, None)),
        ("inRange 0", (0, None)),
        ("inRange 0 x", (0, None)),
        ("inRange 0 1", (0, 1)),
        ("inRange 0 1 x", (0, 1)),
    ],
)
def test_get_ranges_from_range_rule(
    input_rule: str,
    expected_tuple: tuple[float|None, float|None],
) -> None:
    """Test for _get_ranges_from_range_rule"""
    result = _get_ranges_from_range_rule(input_rule)
    assert result == expected_tuple

@pytest.mark.parametrize(
    "input_rules, expected_rule",
    [
        ([], None),
        (["list strict"], None),
        (["inRange 0 1"], "inRange 0 1"),
        (["str error", "inRange 0 1"], "inRange 0 1")
    ],
)
def test_get_in_range_rule_from_rule_list(
    input_rules: list[str],
    expected_rule: str|None,
) -> None:
    """Test for _get_in_range_rule_from_rule_list"""
    result = _get_in_range_rule_from_rule_list(input_rules)
    assert result == expected_rule


@pytest.mark.parametrize(
    "input_rules, expected_rule",
    [
        ([], None),
        (["list strict"], None),
        (["str"], "str"),
        (["str error"], "str")
    ],
)
def test_get_type_rule_from_rule_list(
    input_rules: list[str],
    expected_rule: str|None,
) -> None:
    """Test for _get_type_rule_from_rule_list"""
    result = _get_type_rule_from_rule_list(input_rules)
    assert result == expected_rule
