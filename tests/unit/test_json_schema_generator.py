"""Tests for JSON Schema generation"""

from typing import Generator, Any, Union
import os
import json
from shutil import rmtree

import pytest
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from schematic.models.metadata import MetadataModel
from schematic.schemas.json_schema_generator import (
    PropertyData,
    JSONSchema,
    NodeProcessor,
    JSONSchemaGenerator,
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
from tests.utils import json_files_equal

# pylint: disable=protected-access
# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments


@pytest.fixture(name="js_generator")
def fixture_js_generator() -> Generator[JSONSchemaGenerator, None, None]:
    """Yields a DataModelJSONSchema2 with the example data model"""
    metadata_model = MetadataModel(
        inputMModelLocation="tests/data/example.model.jsonld",
        inputMModelLocationType="local",
        data_model_labels="class_label",
    )
    data_model_js = JSONSchemaGenerator(
        jsonld_path=metadata_model.inputMModelLocation,
        graph=metadata_model.graph_data_model,
    )
    yield data_model_js


@pytest.fixture(name="test_directory", scope="session")
def fixture_test_directory():
    """Yields a directory for creating test jSON Schemas in"""
    test_folder = "tests/data/test_jsonschemas"
    os.makedirs(test_folder, exist_ok=True)
    yield test_folder
    rmtree(test_folder)


class TestNodeProcessor:
    """Tests for NodeProcessor class"""

    def test_init(self) -> None:
        """Test NodeProcessor.__init__"""
        np = NodeProcessor(["node1", "node2"])
        assert np.current_node == "node1"
        assert np.nodes_to_process == ["node2"]
        assert np.root_dependencies == ["node1", "node2"]

    def test_move_to_next_node(self) -> None:
        """Test NodeProcessor.move_to_next_node"""
        np = NodeProcessor(["node1", "node2"])
        np.move_to_next_node()
        assert np.current_node == "node2"
        assert np.nodes_to_process == []

    def test_are_nodes_remaining(self) -> None:
        """Test NodeProcessor.are_nodes_remaining"""
        np = NodeProcessor(["node1"])
        assert np.are_nodes_remaining()
        np.move_to_next_node()
        assert not np.are_nodes_remaining()

    def test_is_current_node_processed(self) -> None:
        """Test NodeProcessor.is_current_node_processed"""
        np = NodeProcessor(["node1"])
        assert not np.is_current_node_processed()
        np.processed_nodes += ["node1"]
        assert np.is_current_node_processed()

    def test_update_range_domain_map(self) -> None:
        """Test NodeProcessor.update_range_domain_map"""
        np = NodeProcessor(["node1"])
        assert not np.range_domain_map
        np.update_range_domain_map("node1", ["nodeA", "nodeB"])
        assert np.range_domain_map == {"nodeA": ["node1"], "nodeB": ["node1"]}

    def test_update_reverse_dependencies(self) -> None:
        """Test NodeProcessor.update_reverse_dependencies"""
        np = NodeProcessor(["node1"])
        assert not np.reverse_dependencies
        np.update_reverse_dependencies("node1", ["nodeA", "nodeB"])
        assert np.reverse_dependencies == {"nodeA": ["node1"], "nodeB": ["node1"]}

    def test_update_nodes_to_process(self) -> None:
        """Test NodeProcessor.update_nodes_to_process"""
        np = NodeProcessor(["node1"])
        assert np.nodes_to_process == []
        np.update_nodes_to_process(["node2"])
        assert np.nodes_to_process == ["node2"]

    def test_update_processed_nodes_with_current_node(self) -> None:
        """Test NodeProcessor.update_processed_nodes_with_current_node"""
        np = NodeProcessor(["node1"])
        assert not np.processed_nodes
        np.update_processed_nodes_with_current_node()
        assert np.processed_nodes == ["node1"]


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
def test_create_json_schema(
    js_generator: JSONSchemaGenerator, datatype: str, test_directory: str
) -> None:
    """Tests for JSONSchemaGenerator.create_json_schema"""
    test_file = f"test.{datatype}.schema.json"
    test_path = os.path.join(test_directory, test_file)
    expected_path = f"tests/data/expected_jsonschemas/expected.{datatype}.schema.json"
    title = f"{datatype}_validation"
    js_generator.create_json_schema(datatype, title, test_path)
    json_files_equal(test_path, expected_path)


@pytest.mark.parametrize(
    "instance_path, datatype",
    [
        ("tests/data/json_instances/valid_biospecimen1.json", "Biospecimen"),
        ("tests/data/json_instances/valid_bulk_rna1.json", "BulkRNA-seqAssay"),
        ("tests/data/json_instances/valid_bulk_rna2.json", "BulkRNA-seqAssay"),
        ("tests/data/json_instances/valid_patient1.json", "Patient"),
        ("tests/data/json_instances/valid_patient2.json", "Patient"),
    ],
    ids=[
        "Biospecimen",
        "BulkRNASeqAssay, FileFormat is BAM",
        "BulkRNASeqAssay, FileFormat is CRAM",
        "Patient, Diagnosis is Healthy",
        "Patient, Diagnosis is Cancer",
    ],
)
def test_validate_valid_instances(
    instance_path: str,
    datatype: str,
) -> None:
    """Validates instances using expected JSON Schemas"""
    schema_path = f"tests/data/expected_jsonschemas/expected.{datatype}.schema.json"
    with open(schema_path, encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    with open(instance_path, encoding="utf-8") as instance_file:
        instance = json.load(instance_file)
    validator = Draft7Validator(schema)
    validator.validate(instance)


@pytest.mark.parametrize(
    "instance_path, datatype",
    [
        (
            "tests/data/json_instances/bulk_rna_missing_conditional_dependencies.json",
            "BulkRNA-seqAssay",
        ),
        (
            "tests/data/json_instances/patient_missing_conditional_dependencies.json",
            "Patient",
        ),
    ],
    ids=[
        "BulkRNA, FileFormat is CRAM, missing conditional dependencies",
        "Patient, Diagnosis is Cancer, missing conditional dependencies"
    ],
)
def test_validate_invalid_instances(
    instance_path: str,
    datatype: str,
) -> None:
    """Raises a ValidationError validating invalid instances using expected JSON Schemas"""
    schema_path = f"tests/data/expected_jsonschemas/expected.{datatype}.schema.json"
    with open(schema_path, encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    with open(instance_path, encoding="utf-8") as instance_file:
        instance = json.load(instance_file)
    validator = Draft7Validator(schema)
    with pytest.raises(ValidationError):
        validator.validate(instance)


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
        property_display_name="property_name",
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
    ids=["one rev dep, one enum", "two rev deps, one enum", "two rev deps, two enums"],
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
        property_display_name="property_name",
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
                        "description": "TBD",
                        "oneOf": [
                            {
                                "type": "array",
                                "title": "array",
                                "items": {"enum": ["enum1"]},
                            },
                        ],
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
                        "description": "TBD",
                        "oneOf": [
                            {
                                "type": "array",
                                "title": "array",
                                "items": {"enum": ["enum1"]},
                            },
                            {"type": "null", "title": "null"},
                        ],
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
                properties={
                    "property_name": {
                        "description": "TBD",
                        "oneOf": [
                            {"enum": ["enum1"], "title": "enum"},
                            {"type": "null", "title": "null"},
                        ],
                    }
                },
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
                    "property_name": {
                        "oneOf": [
                            {"type": "array", "title": "array"},
                            {"type": "null", "title": "null"},
                        ],
                        "description": "TBD",
                    }
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
                properties={"property_name": {"description": "TBD"}},
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
        description="TBD",
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "enum_list, is_required, expected_schema, valid_values, invalid_values",
    [
        (
            ["enum1"],
            True,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {
                            "type": "array",
                            "title": "array",
                            "items": {"enum": ["enum1"]},
                        }
                    ],
                }
            },
            [[], ["enum1"]],
            [[None], ["x"], None],
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            ["enum1"],
            False,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {
                            "type": "array",
                            "title": "array",
                            "items": {"enum": ["enum1"]},
                        },
                        {"type": "null", "title": "null"},
                    ],
                }
            },
            [[], ["enum1"], None],
            [[None], ["x"]],
        ),
    ],
    ids=["Required", "Not required"],
)
def test_create_enum_array_property(
    enum_list: list[str],
    is_required: bool,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
) -> None:
    """Test for _create_enum_array_property"""
    schema = _create_enum_array_property(
        name="name", enum_list=enum_list, is_required=is_required, description="TBD"
    )
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": schema, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "property_data, is_required, expected_schema, valid_values, invalid_values",
    [
        (
            PropertyData(is_array=True),
            True,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [{"type": "array", "title": "array"}],
                }
            },
            [[], [None], ["x"]],
            ["x", None],
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            PropertyData(is_array=True),
            False,
            {
                "name": {
                    "oneOf": [
                        {"type": "array", "title": "array"},
                        {"type": "null", "title": "null"},
                    ],
                    "description": "TBD",
                }
            },
            [None, [], [None], ["x"]],
            ["x"],
        ),
        # If item_type is given, it is set in the schema
        (
            PropertyData("string", is_array=True),
            True,
            {
                "name": {
                    "oneOf": [
                        {"type": "array", "title": "array", "items": {"type": "string"}}
                    ],
                    "description": "TBD",
                }
            },
            [[], ["x"]],
            [None, [None], [1]],
        ),
        # If property_data has range_min or range_max, they are set in the schema
        (
            PropertyData("number", is_array=True, range_min=0, range_max=1),
            True,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {
                            "type": "array",
                            "title": "array",
                            "items": {"type": "number", "minimum": 0, "maximum": 1},
                        }
                    ],
                }
            },
            [[], [1]],
            [None, [None], [2], ["x"]],
        ),
    ],
    ids=[
        "Required, no item type",
        "Not required, no item type",
        "Required, string item type",
        "Required, number item type",
    ],
)
def test_create_array_property(
    property_data: PropertyData,
    is_required: bool,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
) -> None:
    """Test for _create_array_property"""
    schema = _create_array_property(
        name="name",
        property_data=property_data,
        is_required=is_required,
        description="TBD",
    )
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": schema, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "enum_list, is_required, expected_schema, valid_values, invalid_values",
    [
        # Empty enum list
        (
            [],
            True,
            {"name": {"description": "TBD", "oneOf": [{"enum": [], "title": "enum"}]}},
            [],
            [1, "x", None],
        ),
        # If is_required is True, no type is added
        (
            ["enum1"],
            True,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [{"enum": ["enum1"], "title": "enum"}],
                }
            },
            ["enum1"],
            [1, "x", None],
        ),
        # If is_required is False, "null" is added as a type
        (
            ["enum1"],
            False,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {"enum": ["enum1"], "title": "enum"},
                        {"type": "null", "title": "null"},
                    ],
                }
            },
            ["enum1", None],
            [1, "x"],
        ),
    ],
    ids=["empty enum list", "is required==True", "is required==False"],
)
def test_create_enum_property(
    enum_list: list[str],
    is_required: bool,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
) -> None:
    """Test for _create_enum_property"""
    schema = _create_enum_property(
        enum_list=enum_list, name="name", is_required=is_required, description="TBD"
    )
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": schema, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "property_data, is_required, expected_schema, valid_values, invalid_values",
    [
        (PropertyData(), False, {"name": {"description": "TBD"}}, [None, 1, ""], []),
        # If property_type is given, it is added to the schema
        (
            PropertyData("string"),
            True,
            {"name": {"type": "string", "description": "TBD"}},
            [""],
            [1, None],
        ),
        # If property_type is given, and is_required is False,
        # type is set to given property_type and "null"
        (
            PropertyData("string"),
            False,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {"type": "string", "title": "string"},
                        {"type": "null", "title": "null"},
                    ],
                }
            },
            [None, "x"],
            [1],
        ),
        # If is_required is True '"not": {"type":"null"}' is added to schema if
        # property_type is not given
        (
            PropertyData(),
            True,
            {"name": {"not": {"type": "null"}, "description": "TBD"}},
            ["x", 1],
            [None],
        ),
        (
            PropertyData(property_type="number", range_min=0, range_max=1),
            True,
            {
                "name": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "description": "TBD",
                }
            },
            [1],
            [None, 2],
        ),
    ],
    ids=[
        "Not required, no type",
        "Required, string type",
        "Not required, string type",
        "Required, no type",
        "Required, number type",
    ],
)
def test_create_simple_property(
    property_data: PropertyData,
    is_required: bool,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
) -> None:
    """Test for _create_simple_property"""
    schema = _create_simple_property(
        "name", property_data, is_required, description="TBD"
    )
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": schema, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


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
    expected_tuple: tuple[Union[str, None], Union[str, None]],
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
        (["str error", "inRange 0 1"], "inRange 0 1"),
    ],
)
def test_get_in_range_rule_from_rule_list(
    input_rules: list[str],
    expected_rule: Union[str, None],
) -> None:
    """Test for _get_in_range_rule_from_rule_list"""
    result = _get_in_range_rule_from_rule_list(input_rules)
    assert result == expected_rule


@pytest.mark.parametrize(
    "input_rules",
    [(["inRange", "inRange"]), (["inRange 0", "inRange 0"])],
)
def test_get_in_range_rule_from_rule_list_exceptions(
    input_rules: list[str],
) -> None:
    """Test for _get_in_range_rule_from_rule_list with exceptions"""
    with pytest.raises(
        ValueError, match="Found more than one inRange rule in validation rules"
    ):
        _get_in_range_rule_from_rule_list(input_rules)


@pytest.mark.parametrize(
    "input_rules, expected_rule",
    [([], None), (["list strict"], None), (["str"], "str"), (["str error"], "str")],
)
def test_get_type_rule_from_rule_list(
    input_rules: list[str],
    expected_rule: Union[str, None],
) -> None:
    """Test for _get_type_rule_from_rule_list"""
    result = _get_type_rule_from_rule_list(input_rules)
    assert result == expected_rule


@pytest.mark.parametrize(
    "input_rules",
    [(["str", "int"]), (["str", "str", "str"]), (["str", "str error", "str warning"])],
)
def test_get_type_rule_from_rule_list_exceptions(
    input_rules: list[str],
) -> None:
    """Test for _get_type_rule_from_rule_list with exceptions"""
    with pytest.raises(
        ValueError, match="Found more than one type rule in validation rules"
    ):
        _get_type_rule_from_rule_list(input_rules)
