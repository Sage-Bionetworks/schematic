"Unit tests for JSON Schema functions"

from typing import Any
import json

import pytest
from synapseclient.models import ColumnType

from schematic.json_schema_functions import (
    _create_columns_from_js_schema,
    _get_column_type_from_js_property,
    _get_column_type_from_js_one_of_list,
    _get_list_column_type_from_js_property,
)

@pytest.mark.parametrize(
    "js_path, expected_column_types",
    [
        (
            "tests/data/expected_jsonschemas/expected.Biospecimen.schema.json",
            {
                "Component": ColumnType.STRING,
                "PatientID": ColumnType.STRING,
                "SampleID": ColumnType.STRING,
                "TissueStatus": ColumnType.STRING,
            }
        ),
        (
            "tests/data/expected_jsonschemas/expected.JSONSchemaComponent.schema.json",
            {
                "Component": ColumnType.STRING,
                "Enum": ColumnType.STRING,
                "EnumNotRequired": ColumnType.STRING,
                "ListEnumNotRequired": ColumnType.STRING_LIST,
                "ListInRange": ColumnType.STRING_LIST,
                "ListNotRequired": ColumnType.STRING_LIST,
                "ListString": ColumnType.STRING_LIST,
                "NoRules": ColumnType.STRING,
                "NoRulesNotRequired": ColumnType.STRING,
                "StringNotRequired": ColumnType.STRING
            }
        ),
        (
            "tests/data/expected_jsonschemas/expected.MockComponent.schema.json",
            {
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
        )
    ],
    ids=[
        "Biospecimen",
        "JSONSchemaComponent",
        "MockComponent"
    ]
)
def test_create_columns_from_js_schema(
    js_path: str, expected_column_types: dict[str, ColumnType]
) -> None:
    """
    Tests for _create_columns_from_js_schema
    This tests that the column_types in output Synapse Columns match the expected type based on
      the types in the JSON Schema
    """
    with open(js_path, encoding="utf-8") as js_file:
        js_schema = json.load(js_file)
    columns = _create_columns_from_js_schema(js_schema)
    column_types = {column.name:column.column_type for column in columns}
    assert column_types == expected_column_types


@pytest.mark.parametrize(
    "js_property, expected_type",
    [
        ({}, ColumnType.STRING),
        ({"enum": []}, ColumnType.STRING),
        ({"type": "array"}, ColumnType.STRING_LIST),
        ({"type": "string"}, ColumnType.STRING),
        ({"type": "boolean"}, ColumnType.BOOLEAN),
        ({"type": "integer"}, ColumnType.INTEGER),
        ({"type": "number"}, ColumnType.DOUBLE),
        ({"oneOf": [{"type": "null"}, {"type": "array"}]}, ColumnType.STRING_LIST)
    ],
    ids=[
        "Empty",
        "Enum",
        "Array type",
        "String type",
        "Boolean type",
        "Integer type",
        "Number type",
        "Array in oneOf list"
    ]
)
def test_get_column_type_from_js_property(
    js_property: dict[str, Any], expected_type:ColumnType
) -> None:
    """
    Tests for _get_column_type_from_js_property
    This tests that the output Synapse ColumnType match the expected type based on
      the types in the JSON Schema
    """
    assert _get_column_type_from_js_property(js_property) == expected_type

@pytest.mark.parametrize(
    "js_one_of_list, expected_type",
    [
        ([], ColumnType.STRING),
        (["x"], ColumnType.STRING),
        ([{}], ColumnType.STRING),
        ([{"enum": []}], ColumnType.STRING),
        ([{"type": "null"}], ColumnType.STRING),
        ([{"type": "boolean"}, {"type": "integer"}], ColumnType.STRING),
        ([{"type": "array"}], ColumnType.STRING_LIST),
        ([{"type": "string"}], ColumnType.STRING),
        ([{"type": "boolean"}], ColumnType.BOOLEAN),
        ([{"type": "integer"}], ColumnType.INTEGER),
        ([{"type": "number"}], ColumnType.DOUBLE),
    ],
    ids=[
        "Empty List",
        "No objects in list",
        "Empty object",
        "Enum",
        "Null type",
        "More than one type",
        "Array type",
        "String type",
        "Boolean type",
        "Integer type",
        "Number type"
    ]
)
def test_get_column_type_from_js_one_of_list(
    js_one_of_list: list[Any], expected_type:ColumnType
) -> None:
    """
    Tests for _get_column_type_from_js_one_of_list
    This tests that the output Synapse ColumnType match the expected type based on
      the types in the JSON Schema
    """
    assert _get_column_type_from_js_one_of_list(js_one_of_list) == expected_type

@pytest.mark.parametrize(
    "js_property, expected_type",
    [
        ({}, ColumnType.STRING_LIST),
        ({"items": {}}, ColumnType.STRING_LIST),
        ({"items": {"enum": []}}, ColumnType.STRING_LIST),
        ({"items": {"type": "not_a_type"}}, ColumnType.STRING_LIST),
        ({"items": {"type": "string"}}, ColumnType.STRING_LIST),
        ({"items": {"type": "boolean"}}, ColumnType.BOOLEAN_LIST),
        ({"items": {"type": "integer"}}, ColumnType.INTEGER_LIST),
        # Synapse does not currently have a double list
        ({"items": {"type": "number"}}, ColumnType.STRING_LIST),
    ],
    ids=[
        "Empty dict",
        "Empty items",
        "List enum",
        "Not a type",
        "List string",
        "List boolean",
        "List integer",
        "List number"
    ]
)
def test_get_list_column_type_from_js_property(
    js_property: dict[str, Any], expected_type:ColumnType
) -> None:
    """
    Tests for _get_list_column_type_from_js_property
    This tests that the output Synapse ColumnType match the expected type based on
      the types in the JSON Schema
    """
    assert _get_list_column_type_from_js_property(js_property) == expected_type
